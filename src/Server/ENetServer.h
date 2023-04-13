#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wreserved-identifier"
#pragma GCC diagnostic ignored "-Wpacked"

#include <enet.h>

#include <base/types.h>


namespace DB
{
class Context;

class ENetServer
{
public:
    explicit ENetServer(bool _is_open);

    /// Close the socket and ask existing connections to stop serving queries
    void stop()
    {}

    bool isOpen() const { return is_open; }

    UInt16 portNumber() const { return port_number; }

private:
    //Poco::Net::ServerSocket socket;
    std::atomic<bool> is_open;
    UInt16 port_number;
};

}
#pragma GCC diagnostic pop
