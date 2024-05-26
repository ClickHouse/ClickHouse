#pragma once

#include <memory>
#include <base/types.h>

namespace DB
{

class IProtocolServer
{
public:
    IProtocolServer(const std::string & listen_host_, const std::string & port_name_, const std::string & description_)
        : listen_host(listen_host_), port_name(port_name_), description(description_)
    {
    }

    virtual ~IProtocolServer() = default;

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    virtual void start() = 0;

    /// Stops the server. No new connections will be accepted.
    virtual void stop() = 0;

    virtual bool isStopping() const = 0;

    /// Returns the port this server is listening to.
    virtual UInt16 portNumber() const = 0;

    /// Returns the number of current threads.
    virtual size_t currentThreads() const = 0;

    /// Returns the number of currently handled connections.
    virtual size_t currentConnections() const = 0;

    const std::string & getPortName() const { return port_name; }
    const std::string & getListenHost() const { return listen_host; }
    const std::string & getDescription() const { return description; }

private:
    std::string listen_host;
    std::string port_name;
    std::string description;
};

using IProtocolServerPtr = std::unique_ptr<IProtocolServer>;

}
