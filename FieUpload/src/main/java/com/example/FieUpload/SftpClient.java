package com.example.FieUpload;


import com.example.FieUpload.util.CommonConstants;
import com.example.FieUpload.util.CommonConstants;
import com.example.FieUpload.util.PropertyReader;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SftpClient {

    private static final Logger logger = LoggerFactory.getLogger(SftpClient.class);

    private static final String SFTP_HOST = PropertyReader.getProperty(CommonConstants.Host);
    private static final int SFTP_PORT = 22;
    private static final String SFTP_USER = PropertyReader.getProperty(CommonConstants.username);
    private static final String SFTP_PASS = PropertyReader.getProperty(CommonConstants.pwd);
    private static final String SFTP_REMOTE_DIR = PropertyReader.getProperty(CommonConstants.remotedir);
    private static final String SFTP_LOCAL_DIR = PropertyReader.getProperty(CommonConstants.localdir);
    private static final String SFTP_REMOTE_ARCHIVE_DIR = PropertyReader.getProperty(CommonConstants.remoteArchiveDir);
    private static final String SFTP_LOCAL_ARCHIVE_DIR = PropertyReader.getProperty(CommonConstants.localArchiveDir);

    public static void main(String[] args) {
        SftpClient client = new SftpClient();
        client.processFiles();
    }

    public void uploadFile(String fileName) {
        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTP_USER, SFTP_HOST, SFTP_PORT);
            session.setPassword(SFTP_PASS);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            String localFilePath = SFTP_LOCAL_DIR + fileName;
            String remoteFilePath = SFTP_REMOTE_DIR + fileName;

            try (InputStream inputStream = new FileInputStream(localFilePath)) {
                channelSftp.put(inputStream, remoteFilePath);
                logger.info("File uploaded successfully - {}", fileName);
            }
        } catch (Exception ex) {
            logger.error("Error uploading file: {}", fileName, ex);
        } finally {
            if (channelSftp != null) {
                channelSftp.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }

    public void downloadFile(String fileName) {
        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTP_USER, SFTP_HOST, SFTP_PORT);
            session.setPassword(SFTP_PASS);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            String remoteFilePath = SFTP_REMOTE_DIR + fileName;
            String localFilePath = SFTP_LOCAL_DIR + fileName;

            try (OutputStream outputStream = new FileOutputStream(localFilePath)) {
                channelSftp.get(remoteFilePath, outputStream);
                logger.info("File downloaded successfully - {}", fileName);
            }
        } catch (Exception ex) {
            logger.error("Error downloading file: {}", fileName, ex);
        } finally {
            if (channelSftp != null) {
                channelSftp.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }

    public void processFiles() {
        Session session = null;
        ChannelSftp channelSftp = null;

        ExecutorService executor = Executors.newFixedThreadPool(10);

        try {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTP_USER, SFTP_HOST, SFTP_PORT);
            session.setPassword(SFTP_PASS);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            // List all files in the remote directory
            Vector<ChannelSftp.LsEntry> fileList = channelSftp.ls(SFTP_REMOTE_DIR);

            for (ChannelSftp.LsEntry entry : fileList) {
                if (!entry.getAttrs().isDir()) {
                    String fileName = entry.getFilename();
                    executor.submit(() -> processFile(fileName));
                }
            }
        } catch (Exception ex) {
            logger.error("Error processing files", ex);
        } finally {
            if (channelSftp != null) {
                channelSftp.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    private void processFile(String fileName) {
        Session session = null;
        ChannelSftp channelSftp = null;

        try {
            JSch jsch = new JSch();
            session = jsch.getSession(SFTP_USER, SFTP_HOST, SFTP_PORT);
            session.setPassword(SFTP_PASS);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect();
            channelSftp = (ChannelSftp) session.openChannel("sftp");
            channelSftp.connect();

            String remoteFilePath = SFTP_REMOTE_DIR + fileName;
            String localFilePath = SFTP_LOCAL_DIR + fileName;

            // Download the file
            try (OutputStream outputStream = new FileOutputStream(localFilePath)) {
                channelSftp.get(remoteFilePath, outputStream);
                logger.info("File downloaded successfully - {}", fileName);
            }

            // Move the file to the remote archive directory
            String remoteArchivePath = SFTP_REMOTE_ARCHIVE_DIR + fileName;
            channelSftp.rename(remoteFilePath, remoteArchivePath);
            logger.info("File moved to archive - {}", fileName);

        } catch (Exception ex) {
            logger.error("Error processing file: {}", fileName, ex);
        } finally {
            if (channelSftp != null) {
                channelSftp.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }
}
