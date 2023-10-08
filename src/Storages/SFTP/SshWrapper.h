#pragma once

#include <libssh/sftp.h>
#include <libssh/libsshpp.hpp>
#include <memory>
#include "SshException.h"

namespace DB {
    // Ssh wrapper on raw c libssh
    class SftpWrapper;

    class SshWrapper {
    private:
        friend class SftpWrapper;

        ssh_session ssh_session;
        String user;
        String host;
        int port;

        void setOption(ssh_options_e type, const void *value) {
            int rc = ssh_options_set(ssh_session, type, value);
            if (rc != SSH_OK) {
                throw SshException(ssh_get_error_code(ssh_session));
            }
        }

        void connect() {
            int rc = ssh_connect(ssh_session);
            if (rc != SSH_OK) {
                throw SshException(ssh_get_error_code(ssh_session));
            }
        }

        void initAndConnect() {
            ssh_session = ssh_new();
            if (ssh_session == nullptr) {
                throw SshException(SSH_ERROR);
            }
            try {
                setOption(SSH_OPTIONS_USER, user.c_str());
                setOption(SSH_OPTIONS_HOST, host.c_str());
                setOption(SSH_OPTIONS_LOG_VERBOSITY, "SSH_LOG_PROTOCOL");
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
                throw SshException(ssh_get_error_code(ssh_session));
            }
        }

        void userauthSshAgent() {
            int rc = ssh_userauth_agent(ssh_session, user.c_str());
            if (rc != SSH_AUTH_SUCCESS) {
                throw SshException(ssh_get_error_code(ssh_session));
            }
        }

        ::ssh_session getCSession() {
            return ssh_session;
        }

    public:
        SshWrapper(String user_, String host_, int port_ = 22) : user(user_), host(host_), port(port_) {
            initAndConnect();
            try {
                userauthSshAgent();
            } catch (...) {
                ssh_disconnect(ssh_session);
                ssh_free(ssh_session);
                throw;
            }
        }

        SshWrapper(String user_, String password_, String host_, int port_ = 22) : user(user_), host(host_),
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

        SshWrapper(const SshWrapper &) = delete;

        SshWrapper &operator=(const SshWrapper &) = delete;

        SshWrapper(SshWrapper &&) = delete;

        SshWrapper &operator=(SshWrapper &&) = delete;

        ~SshWrapper() {
            ssh_disconnect(ssh_session);
            ssh_free(ssh_session);
        }
    };

    class SftpAttributes {
    private:
        friend class SftpWrapper;

        friend class FileStream;

        sftp_attributes attributes;

        explicit SftpAttributes(sftp_attributes attributes_) : attributes(attributes_) {}

    public:
        SftpAttributes() = default;

        SftpAttributes(const SftpAttributes &) = delete;

        SftpAttributes &operator=(const SftpAttributes &) = delete;

        explicit SftpAttributes(SftpAttributes && other) : SftpAttributes() {
            *this = std::move(other);
        }

        SftpAttributes &operator=(SftpAttributes && other) noexcept {
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

    class SftpWrapper : public std::enable_shared_from_this<SftpWrapper> {
    private:
        sftp_session sftp_session;
        std::shared_ptr<SshWrapper> ssh_session;

    public:
        explicit SftpWrapper(std::shared_ptr<SshWrapper> ssh_session_) : ssh_session(std::move(ssh_session_)) {
            sftp_session = sftp_new(ssh_session->getCSession());
            if (sftp_session == nullptr) {
                if (SSH_ERROR == ssh_get_error_code(ssh_session->getCSession())) {
                    throw SshException(SSH_ERROR);
                }
            }
            int rc = sftp_init(sftp_session);
            if (rc != SSH_OK) {
                auto code = sftp_get_error(sftp_session);
                sftp_free(sftp_session);
                throw SshException(code);
            }
        }

        SftpWrapper(const SftpWrapper &) = delete;

        SftpWrapper &operator=(const SftpWrapper &) = delete;

        SftpWrapper(SftpWrapper &&) = delete;

        SftpWrapper &operator=(SftpWrapper &&) = delete;

        ~SftpWrapper() {
            sftp_free(sftp_session);
        }

        SftpAttributes getPathInfo(String path) {
            return SftpAttributes{sftp_stat(sftp_session, path.c_str())};
        }

        class FileStream {
        private:
            friend class SftpWrapper;

            sftp_file file;
            std::shared_ptr<SftpWrapper> sftp_wrapper;

            FileStream(const std::shared_ptr<SftpWrapper> &sftp_wrapper_, sftp_file file_)
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

            template<size_t bufferSize = 1024>
            void Write(std::istream &in) {
                char buffer[bufferSize];
                do {
                    in.read(buffer, bufferSize);
                    if (in.bad()) {
                        throw std::runtime_error("error reading input stream");
                    }
                    auto read = in.gcount();
                    if (read > 0) {
                        auto written = sftp_write(file, buffer, read);
                        if (written != read) {
                            throw std::runtime_error("error writing file");
                        }
                    }
                } while (in);
            }

            template<size_t bufferSize = 1024>
            std::future<std::shared_ptr<FileStream>> WriteAsync(std::istream &in) {
                if (!file) {
                    throw std::runtime_error("file not opened");
                }

                auto ptr = std::make_shared<FileStream>(std::move(*this));
                return std::async([](std::shared_ptr<FileStream> const &ptr_, std::istream &in_) {
                                      ptr_->Write<bufferSize>(in_);
                                      return ptr_;
                                  },
                                  ptr,
                                  std::reference_wrapper(in));
            }

            template<size_t bufferSize = 1024>
            void Read(std::ostream &out) {
                if (!file) {
                    throw std::runtime_error("file not opened");
                }

                char buffer[bufferSize];

                ssize_t readCount;
                do {
                    readCount = sftp_read(file, buffer, bufferSize);

                    if (readCount < 0) {
                        throw std::runtime_error("error reading file");
                    }

                    out.write(buffer, readCount);
                    if (!out) {
                        throw std::runtime_error("error writing the contents read via ssh to output stream");
                    }
                } while (readCount > 0);
            }

            void seek(off_t seek) {
                if (!file) {
                    throw std::runtime_error("file not opened");
                }

                auto rc = sftp_seek64(file, seek);
                if (rc < 0) {
                    throw SftpException(sftp_wrapper->sftp_session);
                }
            }

            off_t read(char *buffer, off_t bufferSize) {
                if (!file) {
                    throw std::runtime_error("file not opened");
                }

                ssize_t readCount = sftp_read(file, buffer, bufferSize);

                if (readCount < 0) {
                    throw SftpException(sftp_wrapper->sftp_session);
                }

                return readCount;
            }

            template<size_t bufferSize = 1024>
            std::future<std::shared_ptr<FileStream>> ReadAsync(std::ostream &out) {
                if (!file) {
                    throw std::runtime_error("file not opened");
                }

                auto ptr = std::make_shared<FileStream>(std::move(*this));
                return std::async([](std::shared_ptr<FileStream> const &ptr_, std::ostream &out_) {
                                      ptr_->Read<bufferSize>(out_);
                                      return ptr_;
                                  },
                                  ptr,
                                  std::reference_wrapper(out));
            }
        };

        FileStream openFile(String fileName, int accessSpecifiers, mode_t permissions = {}) {
            if (accessSpecifiers & O_RDONLY) {
                accessSpecifiers ^= O_TRUNC;
            }
            auto *file = sftp_open(sftp_session, fileName.c_str(), accessSpecifiers, permissions);
            if (file == nullptr) {
                throw SftpException(sftp_session);
            }
            return FileStream(shared_from_this(), file);
        }

        class DirectoryIterator {
        private:
            friend class SftpWrapper;

            sftp_dir dir;
            std::shared_ptr<SftpWrapper> sftp_wrapper;

            DirectoryIterator(const std::shared_ptr<SftpWrapper> &sftp_wrapper_, const String &path) : sftp_wrapper(
                    sftp_wrapper_) {
                dir = sftp_opendir(sftp_wrapper->sftp_session, path.c_str());
                if (dir == nullptr) {
                    throw SftpException(sftp_wrapper->sftp_session);
                }
            }

        public:
            DirectoryIterator() = default;

            ~DirectoryIterator() {
                if (dir != nullptr)
                    sftp_closedir(dir);
            }

            DirectoryIterator(const DirectoryIterator &) = delete;

            DirectoryIterator &operator=(const DirectoryIterator &) = delete;

            DirectoryIterator(DirectoryIterator &&other) noexcept : DirectoryIterator() {
                *this = std::move(other);
            }

            DirectoryIterator &operator=(DirectoryIterator &&other) noexcept {
                if (dir != other.dir)
                    std::swap(dir, other.dir);
                return *this;
            }

            SftpAttributes next() {
                if (dir == nullptr) {
                    throw std::runtime_error("directory not opened");
                }
                if (sftp_dir_eof(dir)) {
                    return SftpAttributes();
                }
                return SftpAttributes{sftp_readdir(sftp_wrapper->sftp_session, dir)};
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
