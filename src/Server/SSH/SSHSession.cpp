#include <Server/SSH/SSHSession.h>

#if USE_SSH && defined(OS_LINUX)

#include <fmt/format.h>
#include <mutex>
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

SSHSession::SSHSession() : session(ssh_new()), disconnected(false)
{
    if (!session)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to create ssh_session");
}

SSHSession::~SSHSession()
{
    if (session)
        ssh_free(session);
}

SSHSession::SSHSession(SSHSession && rhs) noexcept
{
    *this = std::move(rhs);
}

SSHSession & SSHSession::operator=(SSHSession && rhs) noexcept
{
    this->session = rhs.session;
    this->disconnected = rhs.disconnected;
    rhs.session = nullptr;
    rhs.disconnected = true;
    return *this;
}

SSHSession::SessionPtr SSHSession::getInternalPtr() const
{
    return session;
}

void SSHSession::connect()
{
    int rc = ssh_connect(session);
    if (rc != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed connecting in ssh session due due to {}", getError());

}

void SSHSession::disableDefaultConfig()
{
    bool enable = false;
    int rc = ssh_options_set(session, SSH_OPTIONS_PROCESS_CONFIG, &enable);
    if (rc != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed disabling default config for ssh session due due to {}", getError());
}

void SSHSession::setPeerHost(const String & host)
{
    int rc = ssh_options_set(session, SSH_OPTIONS_HOST, host.c_str());
    if (rc != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting peer host option for ssh session due due to {}", getError());
}

void SSHSession::setFd(int fd)
{
    int rc = ssh_options_set(session, SSH_OPTIONS_FD, &fd);
    if (rc != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting fd option for ssh session due due to {}", getError());
}

void SSHSession::setTimeout(int timeout, int timeout_usec)
{
    int rc = ssh_options_set(session, SSH_OPTIONS_TIMEOUT, &timeout);
    if (rc != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting for ssh session due timeout option due to {}", getError());

    rc |= ssh_options_set(session, SSH_OPTIONS_TIMEOUT_USEC, &timeout_usec);
    if (rc != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed setting for ssh session due timeout_usec option due to {}", getError());
}

void SSHSession::handleKeyExchange()
{
    int rc = ssh_handle_key_exchange(session);
    if (rc != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed key exchange for ssh session due to {}", getError());
}

void SSHSession::disconnect()
{
    std::lock_guard<std::mutex> lock(disconnect_mutex);
    if (!disconnected)
    {
        // Ensure disconnect is called only once per session
        disconnected = true;
        // Use global mutex to prevent concurrent ssh_disconnect() calls across ALL sessions
        // because libssh has shared global state that is not thread-safe
        static std::mutex global_disconnect_mutex;
        std::lock_guard<std::mutex> global_lock(global_disconnect_mutex);
        ssh_disconnect(session);
    }
}

String SSHSession::getError()
{
    return String(ssh_get_error(session));
}

bool SSHSession::hasFinished()
{
    return ssh_get_status(session) & (SSH_CLOSED | SSH_CLOSED_ERROR);
}

}

#endif
