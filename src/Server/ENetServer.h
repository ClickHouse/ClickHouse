#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#pragma GCC diagnostic ignored "-Wpacked"

#include "config.h"

#include <base/types.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>

#if USE_ENET

#include <enet.h>

namespace DB
{
class Context;
class IServer;

class ENetServer : public Poco::Runnable
{
public:
    explicit ENetServer(IServer & iserver_);

    void run() override;

    void stop();

    void start();

    bool isOpen() const { return !_stopped; }

    size_t currentThreads() const { return 1; }
    // placeholder value

    size_t currentConnections() const { return 1; }
    // placeholder value

    UInt16 portNumber() const { return port_number; }

private:
    Poco::Thread * thread;
    std::atomic<bool> _stopped;
    UInt16 port_number;
    ENetAddress address;
    ENetHost * server;
    IServer & ch_server;
};

}
#endif
#pragma GCC diagnostic pop
