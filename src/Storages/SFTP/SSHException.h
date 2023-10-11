#pragma once

#include <Common/Exception.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-macro-identifier"
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#pragma GCC diagnostic ignored "-Wdocumentation"

#include <libssh/sftp.h>
#include <libssh/libssh.h>
#include <libssh/auth.h>

#pragma GCC diagnostic pop

#include <string>

namespace DB {
    class SSHException : public Exception {
    public:
        SSHException(ssh_session csession) : Exception("SSH Error", ssh_get_error_code(csession)) {
            code = ssh_get_error_code(csession);
        }

        SSHException(int code_) : Exception("SSH Error", code_) {
            code = code_;
        }

        SSHException(const SSHException &e) : Exception("SSH Error", e.code) {
            code = e.code;
        }

        int getCode() {
            return code;
        }

    private:
        int code;
    };

    class SFTPException : public Exception {
    public:
        SFTPException(sftp_session csession) : Exception("SFTP Error", sftp_get_error(csession)) {
            code = sftp_get_error(csession);
        }

        SFTPException(int code_) : Exception("SFTP Error", code_) {
            code = code_;
        }

        SFTPException(const SFTPException &e) : Exception("SFTP Error", e.code) {
            code = e.code;
        }

        int getCode() {
            return code;
        }

    private:
        int code;
    };
}
