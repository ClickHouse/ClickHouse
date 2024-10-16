#pragma once

#include <Client/Connection.h>
#include <Storages/MergeTree/RequestResponse.h>

namespace DB
{

/// Base class for working with multiple replicas (connections)
/// from one shard within a single thread
class IConnections : boost::noncopyable
{
public:
    /// Send all scalars to replicas.
    virtual void sendScalarsData(Scalars & data) = 0;
    /// Send all content of external tables to replicas.
    virtual void sendExternalTablesData(std::vector<ExternalTablesData> & data) = 0;

    /// Send request to replicas.
    virtual void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id,
        UInt64 stage,
        ClientInfo & client_info,
        bool with_pending_data) = 0;

    virtual void sendReadTaskResponse(const String &) = 0;
    virtual void sendMergeTreeReadTaskResponse(const ParallelReadResponse & response) = 0;

    /// Get packet from any replica.
    virtual Packet receivePacket() = 0;

    /// Version of `receivePacket` function without locking.
    virtual Packet receivePacketUnlocked(AsyncCallback async_callback) = 0;

    /// Break all active connections.
    virtual void disconnect() = 0;

    /// Send a request to replicas to cancel the request
    virtual void sendCancel() = 0;

    /// Send parts' uuids to replicas to exclude them from query processing
    virtual void sendIgnoredPartUUIDs(const std::vector<UUID> & uuids) = 0;

    /** On each replica, read and skip all packets to EndOfStream or Exception.
      * Returns EndOfStream if no exception has been received. Otherwise
      * returns the last received packet of type Exception.
      */
    virtual Packet drain() = 0;

    /// Get the replica addresses as a string.
    virtual std::string dumpAddresses() const = 0;

    struct ReplicaInfo
    {
        size_t number_of_current_replica{0};
    };

    /// This is needed in max_parallel_replicas case.
    /// We create a RemoteQueryExecutor for each replica
    virtual void setReplicaInfo(ReplicaInfo value) = 0;

    /// Returns the number of replicas.
    virtual size_t size() const = 0;

    /// Check if there are any valid replicas.
    virtual bool hasActiveConnections() const = 0;

    virtual ~IConnections() = default;

    virtual void setAsyncCallback(AsyncCallback) {}
};

}
