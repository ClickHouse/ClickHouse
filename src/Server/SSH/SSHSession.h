#pragma once

#include "config.h"

#if USE_SSH

#include <memory>
#include <base/types.h>

struct ssh_session_struct;

namespace ssh
{

// Wrapper around libssh's ssh_session
class SSHSession
{
public:
    using SessionPtr = ssh_session_struct *;

    SSHSession();
    ~SSHSession();

    SSHSession(const SSHSession &) = delete;
    SSHSession & operator=(const SSHSession &) = delete;

    SSHSession(SSHSession &&) noexcept;
    SSHSession & operator=(SSHSession &&) noexcept;

    // Get c pointer from libssh to be able to pass it to other objects
    SessionPtr getCSessionPtr() const;

    // Disable reading default libssh configuration
    void disableDefaultConfig();
    void connect();
    void setPeerHost(const String & host);
    // Pass ready socket to session
    void setFd(int fd);
    void setTimeout(int timeout, int timeout_usec);
    // Disable session from closing socket. Can be used when a socket is passed
    void disableSocketOwning();
    void handleKeyExchange();
    void disconnect();
    String getError();
    // Check that session was closed
    bool hasFinished();

private:
    static void deleter(SessionPtr session);

    std::unique_ptr<ssh_session_struct, decltype(&deleter)> session;
};

}

#endif
