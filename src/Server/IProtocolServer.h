#pragma once

#include <base/types.h>

namespace DB
{

class IProtocolServer
{
public:
    IProtocolServer(
        UInt16 port_number_,
        const std::string & port_name_,
        const std::string & listen_host_,
        const std::string & description_)
        : port_number(port_number_)
        , port_name(port_name_)
        , listen_host(listen_host_)
        , description(description_)
    {}

    virtual ~IProtocolServer() = default;

    // Starts the server. A new thread will be created that waits for and accepts incoming connections.
    virtual void start() = 0;

    // Stops the server. No new connections will be accepted.
    virtual void stop() { is_open = false; }

    // Returns if the server is accepting connections.
    bool isOpen() const { return is_open; }

    // Returns if the server is stopping.
    bool isStopping() const { return !is_open; }

    // Returns the port this server is listening to.
    UInt16 portNumber() const { return port_number; }

    // Returns the name of the port this server is listening to.
    const std::string & getPortName() const { return port_name; }

    // Returns the number of current threads.
    virtual size_t currentThreads() const = 0;

    // Returns the number of currently handled connections.
    virtual size_t currentConnections() const = 0;

    // Returns server's listen host.
    const std::string & getListenHost() const { return listen_host; }

    // Returns server's description.
    const std::string & getDescription() const { return description; }

private:
    UInt16 port_number;
    std::string port_name;
    std::string listen_host;
    std::string description;

    std::atomic<bool> is_open = true;
};

using IProtocolServerPtr = std::unique_ptr<IProtocolServer>;

}
