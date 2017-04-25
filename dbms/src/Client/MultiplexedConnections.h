#pragma once

#include <Common/Throttler.h>
#include <Client/Connection.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Poco/ScopedLock.h>
#include <mutex>

namespace DB
{


/** To retrieve data directly from multiple replicas (connections) from one or several shards
  * within a single thread. As a degenerate case, it can also work with one connection.
  * It is assumed that all functions except sendCancel are always executed in one thread.
  *
  * The interface is almost the same as Connection.
  */
class MultiplexedConnections final : private boost::noncopyable
{
public:
    /// Accepts ready connection.
    MultiplexedConnections(Connection * connection_, const Settings * settings_, ThrottlerPtr throttler_);

    /** Accepts a pool from which it will be necessary to get one or more connections.
      * If the append_extra_info flag is set, additional information appended to each received block.
      * If the get_all_replicas flag is set, all connections are selected.
      */
    MultiplexedConnections(
            ConnectionPoolWithFailover & pool_, const Settings * settings_, ThrottlerPtr throttler_,
            bool append_extra_info, PoolMode pool_mode_, const QualifiedTableName * main_table = nullptr);

    /** Accepts pools, one for each shard, from which one will need to get one or more connections.
      * If the append_extra_info flag is set, additional information appended to each received block.
      * If the do_broadcast flag is set, all connections are received.
      */
    MultiplexedConnections(
            const ConnectionPoolWithFailoverPtrs & pools_, const Settings * settings_, ThrottlerPtr throttler_,
            bool append_extra_info, PoolMode pool_mode_, const QualifiedTableName * main_table = nullptr);

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

    /// Get information about the last received package.
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
    size_t size() const { return replica_map.size(); }

    /// Check if there are any valid replicas.
    /// Without locking, because sendCancel() does not change the state of the replicas.
    bool hasActiveConnections() const { return active_connection_total_count > 0; }

private:
    /// Connections of the 1st shard, then the connections of the 2nd shard, etc.
    using Connections = std::vector<Connection *>;

    /// The state of the connections of one shard.
    struct ShardState
    {
        /// The number of connections allocated, i.e. replicas for this shard.
        size_t allocated_connection_count;
        /// The current number of valid connections to the replicas of this shard.
        size_t active_connection_count;
    };

    /// Description of a single replica.
    struct ReplicaState
    {
        size_t connection_index;
        /// The owner of this replica.
        ShardState * shard_state;
    };

    /// Replicas hashed by id of the socket.
    using ReplicaMap = std::unordered_map<int, ReplicaState>;

    /// The state of each shard.
    using ShardStates = std::vector<ShardState>;

private:
    void initFromShard(ConnectionPoolWithFailover & pool, const QualifiedTableName * main_table);

    void registerShards();

    /// Register replicas of one shard.
    void registerReplicas(size_t index_begin, size_t index_end, ShardState & shard_state);

    /// Internal version of `receivePacket` function without locking.
    Connection::Packet receivePacketUnlocked();

    /// Internal version of `dumpAddresses` function without locking.
    std::string dumpAddressesUnlocked() const;

    /// Get a replica where you can read the data.
    ReplicaMap::iterator getReplicaForReading();

    /** Check if there are any data that can be read on any of the replicas.
      * Returns one such replica if it exists.
      */
    ReplicaMap::iterator waitForReadEvent();

    /// Mark the replica as invalid.
    void invalidateReplica(ReplicaMap::iterator it);

private:
    const Settings * settings;

    Connections connections;
    ReplicaMap replica_map;
    ShardStates shard_states;

    /// If not nullptr, then it is used to restrict network traffic.
    ThrottlerPtr throttler;

    std::vector<ConnectionPool::Entry> pool_entries;

    /// Connection that received last block.
    Connection * current_connection;
    /// Information about the last received block, if supported.
    std::unique_ptr<BlockExtraInfo> block_extra_info;

    /// The current number of valid connections to replicas.
    size_t active_connection_total_count = 0;
    /// The query is run in parallel on multiple replicas.
    bool supports_parallel_execution;

    bool sent_query = false;

    bool cancelled = false;

    PoolMode pool_mode = PoolMode::GET_MANY;

    /// A mutex for the sendCancel function to execute safely
    /// in separate thread.
    mutable std::mutex cancel_mutex;
};

}
