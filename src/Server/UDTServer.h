#pragma once

#include "config.h"

#include <base/types.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/Runnable.h>
#include <Poco/ThreadPool.h>

#include <Common/logger_useful.h>

#if USE_UDT

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"

#include <udt.h>

namespace DB
{
class Context;
class IServer;

class UDTServer : public Poco::Runnable
{
public:
    explicit UDTServer(IServer & iserver_, std::string listen_host, UInt16 port);

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
    IServer & ch_server;
    Poco::ThreadPool * pool;
    Poco::Logger * logger;
    std::atomic<bool> _stopped;
    std::string host;
    UInt16 port_number;
    UDTSOCKET serv;
    UDTSOCKET recver;
    sockaddr_in server_addr;
};

}
#pragma GCC diagnostic pop
#endif
