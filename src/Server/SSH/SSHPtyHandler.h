#pragma once

#include "config.h"

#if USE_SSH

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Server/IServer.h>
#include <Server/SSH/SSHSession.h>

namespace DB
{

class SSHPtyHandler : public Poco::Net::TCPServerConnection
{
public:
    explicit SSHPtyHandler
    (
        IServer & server_,
        ::ssh::SSHSession && session_,
        const Poco::Net::StreamSocket & socket,
        unsigned int max_auth_attempts_,
        unsigned int auth_timeout_seconds_,
        unsigned int finish_timeout_seconds_,
        unsigned int event_poll_interval_milliseconds_
    );

    void run() override;

private:
    IServer & server;
    Poco::Logger * log;
    ::ssh::SSHSession session;
    unsigned int max_auth_attempts;
    unsigned int auth_timeout_seconds;
    unsigned int finish_timeout_seconds;
    unsigned int event_poll_interval_milliseconds;
};

}

#endif
