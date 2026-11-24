#pragma once

#include <memory>
#include <mutex>
#include <vector>

namespace DB::Pulsar
{

class Connection;

/// Keeps weak references to every connection a client opens, so `Client::close` can shut them all down
/// from the shutdown thread and unblock any consumer parked in `receive`. It never keeps a connection
/// alive: the producers and consumers own them. Kept in its own header, free of the protobuf headers,
/// so the engine-facing client headers do not pull the generated protocol code into the whole build.
class ConnectionRegistry
{
public:
    void add(const std::shared_ptr<Connection> & connection);
    void closeAll();

private:
    std::mutex mutex;
    std::vector<std::weak_ptr<Connection>> connections;
};

}
