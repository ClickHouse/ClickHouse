#pragma once

#include "config.h"

#if USE_SSH && defined(OS_LINUX)

#include <memory>
#include <Server/SSH/SSHSession.h>

struct ssh_channel_struct;

namespace ssh
{

// Wrapper around libssh's ssh_channel
class SSHChannel
{
public:
    using ChannelPtr = ssh_channel_struct *;

    explicit SSHChannel(SSHSession::SessionPtr session);
    ~SSHChannel();

    SSHChannel(const SSHChannel &) = delete;
    SSHChannel & operator=(const SSHChannel &) = delete;

    SSHChannel(SSHChannel &&) noexcept;
    SSHChannel & operator=(SSHChannel &&) noexcept;

    // Exposes ssh_channel c pointer, which could be used to be passed into other objects
    ChannelPtr getCChannelPtr() const;

    int read(void * dest, uint32_t count, int is_stderr);
    int readTimeout(void * dest, uint32_t count, int is_stderr, int timeout);
    int write(const void * data, uint32_t len);
    // Send eof signal to the other side of channel. It does not close the socket.
    int sendEof();
    // Send exit status to the remote process.
    int sendExitStatus(int exit_status);
    // Sends eof if it has not been sent and then closes channel.
    int close();
    bool isOpen();

private:
    static void deleter(ChannelPtr ch);

    std::unique_ptr<ssh_channel_struct, decltype(&deleter)> channel;
};

}

#endif
