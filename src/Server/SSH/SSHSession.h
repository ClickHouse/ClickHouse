#pragma once

#include "config.h"

#if USE_SSH && defined(OS_LINUX)

#include <memory>
#include <base/types.h>

struct ssh_session_struct;

namespace ssh
{

// Wrapper around libssh's ssh_session
class SSHSession
{
public:
    SSHSession();
    ~SSHSession();
    SSHSession(SSHSession &&) noexcept;
    SSHSession & operator=(SSHSession &&) noexcept;

    SSHSession(const SSHSession &) = delete;
    SSHSession & operator=(const SSHSession &) = delete;

    using SessionPtr = ssh_session_struct *;
    /// Get raw pointer from libssh to be able to pass it to other objects
    SessionPtr getInternalPtr() const;

    /// Disable reading default libssh configuration
    void disableDefaultConfig();
    /// Disable session from closing socket. Can be used when a socket is passed.
    void disableSocketOwning();

    /// Connect / disconnect
    void connect();
    void disconnect();

    /// Configure session
    void setPeerHost(const String & host);
    // Pass ready socket to session
    void setFd(int fd);
    void setTimeout(int timeout, int timeout_usec);

    void handleKeyExchange();

    /// Error handling
    String getError();

    // Check that session was closed
    bool hasFinished();

private:
    SessionPtr session = nullptr;
};

}

#endif
