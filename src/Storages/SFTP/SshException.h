#pragma once

#include <Common/Exception.h>
#include <libssh/sftp.h>
#include <libssh/libssh.h>
#include <libssh/auth.h>
#include <string>

namespace DB {
    class SshException : public Exception {
    public:
        SshException(ssh_session csession) {
            code = ssh_get_error_code(csession);
        }

        SshException(int code_) {
            code = code_;
        }

        SshException(const SshException &e) {
            code = e.code;
        }

        int getCode() {
            return code;
        }

    private:
        int code;
    };

    class SftpException : public Exception {
    public:
        SftpException(sftp_session csession) {
            code = sftp_get_error(csession);
        }

        SftpException(int code_) {
            code = code_;
        }

        SftpException(const SftpException &e) {
            code = e.code;
        }

        int getCode() {
            return code;
        }

    private:
        int code;
    };
}
