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

class ENetServer
{
public:
    explicit ENetServer();

    void run();

    void stop();

    void start();

    bool isOpen() const { return is_open; }

    UInt16 portNumber() const { return port_number; }

private:
    Poco::Thread * thread;
    std::atomic<bool> is_open;
    UInt16 port_number;
};

}
#pragma GCC diagnostic pop
