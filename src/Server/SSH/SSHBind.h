#pragma once

#include "config.h"

#if USE_SSH

#include <cstdint>
#include <memory>
#include <string>
#include <base/types.h>
#include <Server/SSH/SSHSession.h>

struct ssh_bind_struct;

namespace ssh
{

// Wrapper around libssh's ssh_bind
class SSHBind
{
public:
    using BindPtr = ssh_bind_struct *;

    SSHBind();
    ~SSHBind();

    SSHBind(const SSHBind &) = delete;
    SSHBind & operator=(const SSHBind &) = delete;

    SSHBind(SSHBind &&) noexcept;
    SSHBind & operator=(SSHBind &&) noexcept;

    // Disables libssh's default config
    void disableDefaultConfig();
    // Sets host key for a server. It can be set only one time for each key type.
    // If you provide different keys of one type, the first one will be overwritten.
    void setHostKey(const std::string & key_path);
    // Passes external socket to ssh_bind
    void setFd(int fd);
    // Listens on a socket. If it was passed via setFd just read hostkeys
    void listen();
    // Assign accepted socket to ssh_session
    void acceptFd(SSHSession & session, int fd);
    String getError();

private:
    static void deleter(BindPtr bind);

    std::unique_ptr<ssh_bind_struct, decltype(&deleter)> bind;
};

}

#endif
