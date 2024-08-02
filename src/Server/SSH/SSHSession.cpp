#include <Server/SSH/SSHSession.h>

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

SSHSession::SSHSession() : session(ssh_new(), &deleter)
{
    if (!session)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to create ssh_session");
    }
}

SSHSession::~SSHSession() = default;

SSHSession::SSHSession(SSHSession && other) noexcept : session(std::move(other.session))
{
}

SSHSession & SSHSession::operator=(SSHSession && other) noexcept
{
    if (this != &other)
    {
        session = std::move(other.session);
    }
    return *this;
}

SSHSession::SessionPtr SSHSession::getCSessionPtr() const
{
    return session.get();
}

void SSHSession::connect()
{
    int rc = ssh_connect(session.get());
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed connecting in ssh session due due to {}", getError());
    }
}

void SSHSession::disableDefaultConfig()
{
    bool enable = false;
    int rc = ssh_options_set(session.get(), SSH_OPTIONS_PROCESS_CONFIG, &enable);
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed disabling default config for ssh session due due to {}", getError());
    }
}

void SSHSession::disableSocketOwning()
{
    bool owns_socket = false;
    int rc = ssh_options_set(session.get(), SSH_OPTIONS_OWNS_SOCKET, &owns_socket);
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed disabling socket owning for ssh session due to {}", getError());
    }
}

void SSHSession::setPeerHost(const String & host)
{
    int rc = ssh_options_set(session.get(), SSH_OPTIONS_HOST, host.c_str());
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting peer host option for ssh session due due to {}", getError());
    }
}

void SSHSession::setFd(int fd)
{
    int rc = ssh_options_set(session.get(), SSH_OPTIONS_FD, &fd);
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting fd option for ssh session due due to {}", getError());
    }
}

void SSHSession::setTimeout(int timeout, int timeout_usec)
{
    int rc = ssh_options_set(session.get(), SSH_OPTIONS_TIMEOUT, &timeout);
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting for ssh session due timeout option due to {}", getError());
    }
    rc |= ssh_options_set(session.get(), SSH_OPTIONS_TIMEOUT_USEC, &timeout_usec);
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting for ssh session due timeout_usec option due to {}", getError());
    }
}

void SSHSession::handleKeyExchange()
{
    int rc = ssh_handle_key_exchange(session.get());
    if (rc != SSH_OK)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed key exchange for ssh session due to {}", getError());
    }
}

void SSHSession::disconnect()
{
    ssh_disconnect(session.get());
}

String SSHSession::getError()
{
    return String(ssh_get_error(session.get()));
}

bool SSHSession::hasFinished()
{
    return ssh_get_status(session.get()) & (SSH_CLOSED | SSH_CLOSED_ERROR);
}

void SSHSession::deleter(SessionPtr session)
{
    ssh_free(session);
}

}

#endif
