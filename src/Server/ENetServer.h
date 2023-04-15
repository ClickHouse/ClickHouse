#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#pragma GCC diagnostic ignored "-Wpacked"

#include <enet.h>

#include <base/types.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>


namespace DB
{
class Context;

class ENetServer : public Poco::Runnable
{
public:
    explicit ENetServer();

    void run() override;

    void stop();

    void start();

    bool isOpen() const { return !_stopped; }

    UInt16 portNumber() const { return port_number; }

private:
    Poco::Thread * thread;
    std::atomic<bool> _stopped;
    UInt16 port_number;
    ENetAddress address;
    ENetHost * server;
};

}
#pragma GCC diagnostic pop
