#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-macro-identifier"
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#pragma GCC diagnostic ignored "-Wdocumentation"

#include <iostream>
#include <libssh/sftp.h>
#include <fcntl.h>

#pragma GCC diagnostic pop

#include <memory>
#include "SSHException.h"

namespace DB {
    // Ssh wrapper on raw c libssh
    class SFTPWrapper;

    class SSHWrapper {
    private:
        friend class SFTPWrapper;

        ssh_session ssh_session = nullptr;
        String user;
        String host;
        int port;

        void setOption(ssh_options_e type, const void *value) {
            int rc = ssh_options_set(ssh_session, type, value);
            if (rc != SSH_OK) {
                throw SSHException(ssh_get_error_code(ssh_session));
            }
        }

        void connect() {
            int rc = ssh_connect(ssh_session);
            if (rc != SSH_OK) {
                throw SSHException(ssh_get_error_code(ssh_session));
            }
        }

        void initAndConnect() {
            ssh_session = ssh_new();
            if (ssh_session == nullptr) {
                throw SSHException(SSH_ERROR);
            }
            try {
                setOption(SSH_OPTIONS_USER, user.c_str());
                setOption(SSH_OPTIONS_HOST, host.c_str());
                setOption(SSH_OPTIONS_PORT, &port);
                connect();
            }
            catch (...) {
                ssh_free(ssh_session);
                throw;
            }
        }

        void userauthPassword(String password) {
            int rc = ssh_userauth_password(ssh_session, user.c_str(), password.c_str());
            if (rc != SSH_AUTH_SUCCESS) {
                throw SSHException(ssh_get_error_code(ssh_session));
            }
        }

        void userauthSshAgent() {
            int rc = ssh_userauth_agent(ssh_session, user.c_str());
            if (rc != SSH_AUTH_SUCCESS) {
                throw SSHException(ssh_get_error_code(ssh_session));
            }
        }

        ::ssh_session getCSession() {
            return ssh_session;
        }

    public:
        SSHWrapper(String user_, String host_, int port_ = 22) : user(user_), host(host_), port(port_) {
            initAndConnect();
            try {
                userauthSshAgent();
            } catch (...) {
                ssh_disconnect(ssh_session);
                ssh_free(ssh_session);
                throw;
            }
        }

        SSHWrapper(String user_, String password_, String host_, int port_ = 22) : user(user_), host(host_),
                                                                                   port(port_) {
            initAndConnect();
            try {
                userauthPassword(password_);
            } catch (...) {
                ssh_disconnect(ssh_session);
                ssh_free(ssh_session);
                throw;
            }
        }

        SSHWrapper(const SSHWrapper &) = delete;

        SSHWrapper &operator=(const SSHWrapper &) = delete;

        SSHWrapper(SSHWrapper &&) = delete;

        SSHWrapper &operator=(SSHWrapper &&) = delete;

        ~SSHWrapper() {
            ssh_disconnect(ssh_session);
            ssh_free(ssh_session);
        }
    };

    class SftpAttributes {
    private:
        friend class SFTPWrapper;

        friend class FileStream;

        sftp_attributes attributes = nullptr;

        explicit SftpAttributes(sftp_attributes attributes_) : attributes(attributes_) {}

    public:
        SftpAttributes() = default;

        SftpAttributes(const SftpAttributes &) = delete;

        SftpAttributes &operator=(const SftpAttributes &) = delete;

        SftpAttributes(SftpAttributes &&other) noexcept: SftpAttributes() {
            *this = std::move(other);
        }

        SftpAttributes &operator=(SftpAttributes &&other) noexcept {
            std::swap(attributes, other.attributes);
            return *this;
        }

        ~SftpAttributes() {
            if (attributes != nullptr)
                sftp_attributes_free(attributes);
        }

        operator bool() const {
            return attributes != nullptr;
        }


        bool eof() {
            return attributes == nullptr;
        }

        String getName() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->name;
        }

        String getLongName() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->longname;
        }

        uint64_t getSize() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->size;
        }

        uint32_t getUID() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->uid;
        }

        uint32_t getGID() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->gid;
        }

        String getOwner() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->owner;
        }

        String getGroup() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->group;
        }

        uint32_t getPermissions() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->permissions;
        }

        uint64_t getLastAccessedTime() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->atime64 != 0 ? attributes->atime64 : attributes->atime;
        }

        uint64_t getLastModifiedTime() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->mtime64 != 0 ? attributes->mtime64 : attributes->mtime;
        }

        uint64_t getCreatedTime() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->createtime;
        }

        bool isDirectory() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->type == SSH_FILEXFER_TYPE_DIRECTORY;
        }

        bool isSymlink() {
            if (attributes == nullptr) {
                throw std::runtime_error("attributes not initialized");
            }
            return attributes->type == SSH_FILEXFER_TYPE_SYMLINK;
        }
    };

    class SFTPWrapper : public std::enable_shared_from_this<SFTPWrapper> {
    private:
        sftp_session sftp_session = nullptr;
        std::shared_ptr<SSHWrapper> ssh_session;

    public:
        explicit SFTPWrapper(std::shared_ptr<SSHWrapper> ssh_session_) : ssh_session(std::move(ssh_session_)) {
            sftp_session = sftp_new(ssh_session->getCSession());
            if (sftp_session == nullptr) {
                if (SSH_ERROR == ssh_get_error_code(ssh_session->getCSession())) {
                    throw SSHException(SSH_ERROR);
                }
            }
            int rc = sftp_init(sftp_session);
            if (rc != SSH_OK) {
                auto code = sftp_get_error(sftp_session);
                sftp_free(sftp_session);
                throw SSHException(code);
            }
        }

        SFTPWrapper(const SFTPWrapper &) = delete;

        SFTPWrapper &operator=(const SFTPWrapper &) = delete;

        SFTPWrapper(SFTPWrapper &&) = delete;

        SFTPWrapper &operator=(SFTPWrapper &&) = delete;

        ~SFTPWrapper() {
            sftp_free(sftp_session);
        }

        SftpAttributes getPathInfo(String path) {
            return SftpAttributes{sftp_stat(sftp_session, path.c_str())};
        }

        class FileStream {
        private:
            friend class SFTPWrapper;

            sftp_file file = nullptr;
            std::shared_ptr<SFTPWrapper> sftp_wrapper;

            FileStream(const std::shared_ptr<SFTPWrapper> &sftp_wrapper_, sftp_file file_)
                    : file(file_), sftp_wrapper(sftp_wrapper_) {}

        public:
            FileStream(const FileStream &) = delete;

            FileStream &operator=(const FileStream &) = delete;

            FileStream(FileStream &&) noexcept = default;

            FileStream &operator=(FileStream &&) noexcept = default;

            ~FileStream() {
                if (file) {
                    sftp_close(file);
                }
            }

            SftpAttributes getAttributes() {
                return SftpAttributes{sftp_fstat(file)};
            }

            void seek(off_t seek) {
                if (!file) {
                    throw std::runtime_error("file not opened");
                }

                auto rc = sftp_seek64(file, seek);
                if (rc < 0) {
                    throw SFTPException(sftp_wrapper->sftp_session);
                }
            }

            off_t read(char *buffer, off_t bufferSize) {
                if (!file) {
                    throw std::runtime_error("file not opened");
                }

                ssize_t readCount = sftp_read(file, buffer, bufferSize);

                if (readCount < 0) {
                    throw SFTPException(sftp_wrapper->sftp_session);
                }

                return readCount;
            }
        };

        FileStream openFile(String fileName, int accessSpecifiers, mode_t permissions = {}) {
            if (accessSpecifiers & O_RDONLY) {
                accessSpecifiers &= ~O_TRUNC;
            }
            auto *file = sftp_open(sftp_session, fileName.c_str(), accessSpecifiers, permissions);
            if (file == nullptr) {
                throw SFTPException(sftp_session);
            }
            return FileStream(shared_from_this(), file);
        }

        class DirectoryIterator {
        private:
            friend class SFTPWrapper;

            sftp_dir dir = nullptr;
            std::shared_ptr<SFTPWrapper> sftp_wrapper = nullptr;

            DirectoryIterator(const std::shared_ptr<SFTPWrapper> &sftp_wrapper_, const String &path) : sftp_wrapper(
                    sftp_wrapper_) {
                dir = sftp_opendir(sftp_wrapper->sftp_session, path.c_str());
                if (dir == nullptr) {
                    throw SFTPException(sftp_wrapper->sftp_session);
                }
            }

        public:
            DirectoryIterator() = default;

            ~DirectoryIterator() {
                if (dir != nullptr) {
                    sftp_closedir(dir);
                }
            }

            DirectoryIterator(const DirectoryIterator &) = delete;

            DirectoryIterator &operator=(const DirectoryIterator &) = delete;

            DirectoryIterator(DirectoryIterator &&other) noexcept: DirectoryIterator() {
                *this = std::move(other);
            }

            DirectoryIterator &operator=(DirectoryIterator &&other) noexcept {
                std::swap(dir, other.dir);
                std::swap(sftp_wrapper, other.sftp_wrapper);
                return *this;
            }

            SftpAttributes next() {
                if (dir == nullptr) {
                    throw std::runtime_error("directory not opened");
                }
                SftpAttributes result;
                do {
                    result = SftpAttributes{sftp_readdir(sftp_wrapper->sftp_session, dir)};
                } while (!sftp_dir_eof(dir) && (result.getName() == "." || result.getName() == ".."));

                if (sftp_dir_eof(dir)) {
                    return SftpAttributes();
                }
                return result;
            }

            bool eof() {
                if (dir == nullptr) {
                    throw std::runtime_error("directory not opened");
                }
                return sftp_dir_eof(dir);
            }
        };

        DirectoryIterator openDir(const String &path) {
            return DirectoryIterator(shared_from_this(), path);
        }
    };
}
