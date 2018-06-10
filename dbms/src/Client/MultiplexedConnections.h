#pragma once

#include <Common/Throttler.h>
#include <Client/Connection.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <mutex>

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

    /** Accepts a vector of connections to replicas of one shard already taken from pool.
      * If the append_extra_info flag is set, additional information appended to each received block.
      */
    MultiplexedConnections(
            std::vector<IConnectionPool::Entry> && connections,
            const Settings & settings_, const ThrottlerPtr & throttler_, bool append_extra_info);

    /// Send all content of external tables to replicas.
    void sendExternalTablesData(std::vector<ExternalTablesData> & data);

    /// Send request to replicas.
    void sendQuery(
        const String & query,
        const String & query_id = "",
        UInt64 stage = QueryProcessingStage::Complete,
        const ClientInfo * client_info = nullptr,
        bool with_pending_data = false);

    /// Get packet from any replica.
    Connection::Packet receivePacket();

    /// Get information about the last received packet.
    BlockExtraInfo getBlockExtraInfo() const;

    /// Break all active connections.
    void disconnect();

    /// Send a request to the replica to cancel the request
    void sendCancel();

    /** On each replica, read and skip all packets to EndOfStream or Exception.
      * Returns EndOfStream if no exception has been received. Otherwise
      * returns the last received packet of type Exception.
      */
    Connection::Packet drain();

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
    Connection::Packet receivePacketUnlocked();

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
    /// Information about the last received block, if supported.
    std::unique_ptr<BlockExtraInfo> block_extra_info;

    bool sent_query = false;

    bool cancelled = false;

    /// A mutex for the sendCancel function to execute safely
    /// in separate thread.
    mutable std::mutex cancel_mutex;
};

}
