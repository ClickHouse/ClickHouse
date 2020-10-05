#pragma once

namespace DB
{

class IRoutineServer
{
public:
    virtual ~IRoutineServer() = default;

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    virtual void start() = 0;

    /// Stops the server. Once the server has been stopped, it cannot be restarted.
    virtual void stop() = 0;

    /// Returns the number of currently handled connections.
    virtual int currentConnections() const = 0;
};

}
