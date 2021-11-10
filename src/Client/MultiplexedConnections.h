#pragma once

#include <mutex>
#include <Common/Throttler.h>
#include <Client/Connection.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <IO/ConnectionTimeouts.h>
#include <Client/IConnections.h>

namespace DB
{


/** To retrieve data directly from multiple replicas (connections) from one shard
  * within a single thread. As a degenerate case, it can also work with one connection.
  * It is assumed that all functions except sendCancel are always executed in one thread.
  *
  * The interface is almost the same as Connection.
  */
class MultiplexedConnections final : public IConnections
{
public:
    /// Accepts ready connection.
    MultiplexedConnections(Connection & connection, const Settings & settings_, const ThrottlerPtr & throttler_);

    /// Accepts a vector of connections to replicas of one shard already taken from pool.
    MultiplexedConnections(
        std::vector<IConnectionPool::Entry> && connections,
        const Settings & settings_, const ThrottlerPtr & throttler_);

    void sendScalarsData(Scalars & data) override;
    void sendExternalTablesData(std::vector<ExternalTablesData> & data) override;

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id,
        UInt64 stage,
        const ClientInfo & client_info,
        bool with_pending_data) override;

    void sendReadTaskResponse(const String &) override;

    Packet receivePacket() override;

    void disconnect() override;

    void sendCancel() override;

    /// Send parts' uuids to replicas to exclude them from query processing
    void sendIgnoredPartUUIDs(const std::vector<UUID> & uuids) override;

    Packet drain() override;

    std::string dumpAddresses() const override;

    /// Without locking, because sendCancel() does not change this number.
    size_t size() const override { return replica_states.size(); }

    /// Without locking, because sendCancel() does not change the state of the replicas.
    bool hasActiveConnections() const override { return active_connection_count > 0; }

private:
    Packet receivePacketUnlocked(AsyncCallback async_callback) override;

    /// Internal version of `dumpAddresses` function without locking.
    std::string dumpAddressesUnlocked() const;

    /// Description of a single replica.
    struct ReplicaState
    {
        Connection * connection = nullptr;
        ConnectionPool::Entry pool_entry;
    };

    /// Get a replica where you can read the data.
    ReplicaState & getReplicaForReading();

    /// Mark the replica as invalid.
    void invalidateReplica(ReplicaState & replica_state);

private:
    const Settings & settings;

    /// The current number of valid connections to the replicas of this shard.
    size_t active_connection_count = 0;

    std::vector<ReplicaState> replica_states;
    std::unordered_map<int, size_t> fd_to_replica_state_idx;

    /// Connection that received last block.
    Connection * current_connection = nullptr;

    bool sent_query = false;
    bool cancelled = false;

    /// A mutex for the sendCancel function to execute safely
    /// in separate thread.
    mutable std::mutex cancel_mutex;

    friend struct RemoteQueryExecutorRoutine;
};

}
