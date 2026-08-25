#include <Server/SSH/SSHEvent.h>

#if USE_SSH && defined(OS_LINUX)

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

SSHEvent::SSHEvent() : event(ssh_event_new(), &deleter)
{
    if (!event)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Failed to create ssh_event");
}

SSHEvent::~SSHEvent() = default;

SSHEvent::SSHEvent(SSHEvent && other) noexcept : event(std::move(other.event))
{
}

SSHEvent & SSHEvent::operator=(SSHEvent && other) noexcept
{
    if (this != &other)
        event = std::move(other.event);

    return *this;
}

void SSHEvent::addSession(SSHSession & session)
{
    if (ssh_event_add_session(event.get(), session.getInternalPtr()) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Error adding session to ssh event");
}

void SSHEvent::removeSession(SSHSession & session)
{
    if (ssh_event_remove_session(event.get(), session.getInternalPtr()) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Error removing session from ssh event");
}

int SSHEvent::poll(int timeout)
{
    while (true)
    {
        int rc = ssh_event_dopoll(event.get(), timeout);

        if (rc == SSH_AGAIN)
            continue;
        if (rc == SSH_ERROR)
            throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Error on polling on ssh event. {}", errnoToString());
        if (rc != SSH_OK)
            throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "The failure happened with no reason provided by the libssh");

        return rc;
    }
}

int SSHEvent::poll()
{
    return poll(-1);
}

void SSHEvent::addFd(int fd, int events, EventCallback cb, void * userdata)
{
    if (ssh_event_add_fd(event.get(), fd, events, cb, userdata) != SSH_OK)
        throw DB::Exception(DB::ErrorCodes::SSH_EXCEPTION, "Error on adding custom file descriptor to ssh event");
}

void SSHEvent::removeFd(socket_t fd)
{
    ssh_event_remove_fd(event.get(), fd);
}

void SSHEvent::deleter(EventPtr e)
{
    ssh_event_free(e);
}

}

#endif
