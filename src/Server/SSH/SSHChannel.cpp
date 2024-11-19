#include <Server/SSH/SSHChannel.h>

#if USE_SSH

#include <stdexcept>
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

SSHChannel::SSHChannel(SSHSession::SessionPtr session) : channel(ssh_channel_new(session), &deleter)
{
    if (!channel)
    {
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to create ssh_channel");
    }
}

SSHChannel::~SSHChannel() = default;

SSHChannel::SSHChannel(SSHChannel && other) noexcept : channel(std::move(other.channel))
{
}

SSHChannel & SSHChannel::operator=(SSHChannel && other) noexcept
{
    if (this != &other)
    {
        channel = std::move(other.channel);
    }
    return *this;
}

ssh_channel SSHChannel::getCChannelPtr() const
{
    return channel.get();
}

int SSHChannel::read(void * dest, uint32_t count, int isStderr)
{
    return ssh_channel_read(channel.get(), dest, count, isStderr);
}

int SSHChannel::readTimeout(void * dest, uint32_t count, int isStderr, int timeout)
{
    return ssh_channel_read_timeout(channel.get(), dest, count, isStderr, timeout);
}

int SSHChannel::write(const void * data, uint32_t len)
{
    return ssh_channel_write(channel.get(), data, len);
}

int SSHChannel::sendEof()
{
    return ssh_channel_send_eof(channel.get());
}

int SSHChannel::close()
{
    return ssh_channel_close(channel.get());
}

bool SSHChannel::isOpen()
{
    return ssh_channel_is_open(channel.get()) != 0;
}

void SSHChannel::deleter(ssh_channel ch)
{
    ssh_channel_free(ch);
}

}

#endif
