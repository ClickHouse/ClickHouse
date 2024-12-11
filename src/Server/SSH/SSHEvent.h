#pragma once

#include "config.h"

#if USE_SSH

#include <memory>
#include <Server/SSH/SSHSession.h>

struct ssh_event_struct;

namespace ssh
{

// Wrapper around ssh_event from libssh
class SSHEvent
{
public:
    using EventPtr = ssh_event_struct *;
    using EventCallback = int (*)(int fd, int revents, void * userdata);

    SSHEvent();
    ~SSHEvent();

    SSHEvent(const SSHEvent &) = delete;
    SSHEvent & operator=(const SSHEvent &) = delete;

    SSHEvent(SSHEvent &&) noexcept;
    SSHEvent & operator=(SSHEvent &&) noexcept;

    // Adds session's socket to event. Now callbacks will be executed for this session on poll
    void addSession(SSHSession & session);
    void removeSession(SSHSession & session);
    // Add fd to ssh_event and assign callbacks on fd event
    void addFd(int fd, int events, EventCallback cb, void * userdata);
    void removeFd(int fd);
    int poll(int timeout);
    int poll();

private:
    static void deleter(EventPtr e);

    std::unique_ptr<ssh_event_struct, decltype(&deleter)> event;
};

}

#endif
