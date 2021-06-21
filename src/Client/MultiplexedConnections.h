#pragma once

#include <mutex>
#include <Common/Throttler.h>
#include <Client/Connection.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <IO/ConnectionTimeouts.h>

namespace DB
{


/** To retrieve data directly from multiple replicas (connections) from one shard
  * within a single thread. As a degenerate case, it can also work with one connection.
  * It is assumed that all functions except sendCancel are always executed in one thread.
  *
  * The interface is almost the same as Connection.
  */
class MultiplexedConnections final : private boost::noncopyable
{
public:
    /// Accepts ready connection.
    MultiplexedConnections(Connection & connection, const Settings & settings_, const ThrottlerPtr & throttler_);

    /// Accepts a vector of connections to replicas of one shard already taken from pool.
    MultiplexedConnections(
        std::vector<IConnectionPool::Entry> && connections,
        const Settings & settings_, const ThrottlerPtr & throttler_);

    /// Send all scalars to replicas.
    void sendScalarsData(Scalars & data);
    /// Send all content of external tables to replicas.
    void sendExternalTablesData(std::vector<ExternalTablesData> & data);

    /// Send request to replicas.
    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id,
        UInt64 stage,
        const ClientInfo & client_info,
        bool with_pending_data);

    /// Get packet from any replica.
    Packet receivePacket();

    /// Break all active connections.
    void disconnect();

    /// Send a request to the replica to cancel the request
    void sendCancel();

    /** On each replica, read and skip all packets to EndOfStream or Exception.
      * Returns EndOfStream if no exception has been received. Otherwise
      * returns the last received packet of type Exception.
      */
    Packet drain();

    /// Get the replica addresses as a string.
    std::string dumpAddresses() const;

    /// Returns the number of replicas.
    /// Without locking, because sendCancel() does not change this number.
    size_t size() const { return replica_states.size(); }

    /// Check if there are any valid replicas.
    /// Without locking, because sendCancel() does not change the state of the replicas.
    bool hasActiveConnections() const { return active_connection_count > 0; }

private:
    /// Internal version of `receivePacket` function without locking.
    Packet receivePacketUnlocked();

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
};

}
