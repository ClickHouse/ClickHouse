#include <Server/SSH/SSHBind.h>

#if USE_SSH

#include <stdexcept>
#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/clibssh.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SSH_EXCEPTION;
}

}

namespace ssh
{

SSHBind::SSHBind() : bind(ssh_bind_new(), &deleter)
{
    if (!bind)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to create ssh_bind");
    }
}

SSHBind::~SSHBind() = default;

SSHBind::SSHBind(SSHBind && other) noexcept = default;

SSHBind & SSHBind::operator=(SSHBind && other) noexcept = default;

void SSHBind::setHostKey(const std::string & key_path)
{
    if (ssh_bind_options_set(bind.get(), SSH_BIND_OPTIONS_HOSTKEY, key_path.c_str()) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting host key in sshbind due to {}", getError());
}

String SSHBind::getError()
{
    return String(ssh_get_error(bind.get()));
}

void SSHBind::disableDefaultConfig()
{
    bool enable = false;
    if (ssh_bind_options_set(bind.get(), SSH_BIND_OPTIONS_PROCESS_CONFIG, &enable) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed disabling default config in sshbind due to {}", getError());
}

void SSHBind::setFd(int fd)
{
    ssh_bind_set_fd(bind.get(), fd);
}

void SSHBind::listen()
{
    if (ssh_bind_listen(bind.get()) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed listening in sshbind due to {}", getError());
}

void SSHBind::acceptFd(SSHSession & session, int fd)
{
    if (ssh_bind_accept_fd(bind.get(), session.getCSessionPtr(), fd) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed accepting fd in sshbind due to {}", getError());
}

void SSHBind::deleter(BindPtr bind)
{
    ssh_bind_free(bind);
}

}

#endif
