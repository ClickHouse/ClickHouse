#pragma once

#include "config.h"

#if USE_GRPC

namespace DB
{

class IGRPCServer
{
public:
    virtual ~IGRPCServer() = default;

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    virtual void start() = 0;

    /// Stops the server. No new connections will be accepted.
    virtual void stop() = 0;

    /// Returns the port this server is listening to.
    virtual UInt16 portNumber() const = 0;

    /// Returns the number of currently handled connections.
    virtual size_t currentConnections() const = 0;

    /// Returns the number of current threads.
    virtual size_t currentThreads() const = 0;
};
}
#endif
