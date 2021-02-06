#pragma once
#if defined(OS_LINUX)

#include <functional>
#include <queue>
#include <Client/HedgedConnectionsFactory.h>
#include <Client/IConnections.h>

namespace DB
{

/** To receive data from multiple replicas (connections) from one shard asynchronously,
  * The principe of Hedged Connections is used to reduce tail latency:
  * (if we don't receive data from replica for a long time, we try to get new replica
  * and send query to it, without cancelling working with previous replica). This class
  * supports all functionality that MultipleConnections has.
  */
class HedgedConnections : public IConnections
{
public:
    struct ReplicaState
    {
        Connection * connection = nullptr;
        std::unordered_map<int, ConnectionTimeoutDescriptorPtr> active_timeouts;
    };

    struct ReplicaLocation
    {
        size_t offset;
        size_t index;
    };

    struct OffsetState
    {
        std::vector<ReplicaState> replicas;
        size_t active_connection_count;
        bool first_packet_of_data_received;
    };

    HedgedConnections(const ConnectionPoolWithFailoverPtr & pool_,
                      const Settings & settings_,
                      const ConnectionTimeouts & timeouts_,
                      const ThrottlerPtr & throttler,
                      PoolMode pool_mode,
                      std::shared_ptr<QualifiedTableName> table_to_check_ = nullptr);

    void sendScalarsData(Scalars & data) override;

    void sendExternalTablesData(std::vector<ExternalTablesData> & data) override;

    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id,
        UInt64 stage,
        const ClientInfo & client_info,
        bool with_pending_data) override;

    Packet receivePacket() override;

    Packet receivePacketUnlocked(AsyncCallback async_callback) override;

    void disconnect() override;

    void sendCancel() override;

    void sendIgnoredPartUUIDs(const std::vector<UUID> & uuids) override;

    Packet drain() override;

    std::string dumpAddresses() const override;

    size_t size() const override { return offset_states.size(); }

    bool hasActiveConnections() const override { return active_connection_count > 0; }

private:
    /// We will save actions with replicas in pipeline to perform them on the new replicas.
    class Pipeline
    {
    public:
        void add(std::function<void(ReplicaState &)> send_function);

        void run(ReplicaState & replica);
    private:
        std::vector<std::function<void(ReplicaState &)>> pipeline;
    };

    Packet receivePacketFromReplica(ReplicaLocation & replica_location, AsyncCallback async_callback = {});

    Packet receivePacketImpl(AsyncCallback async_callback = {});

    void processReceivedFirstDataPacket(ReplicaLocation & replica_location);

    void processTimeoutEvent(ReplicaLocation & replica_location, ConnectionTimeoutDescriptorPtr timeout_descriptor);

    void tryGetNewReplica(bool start_new_connection);

    void finishProcessReplica(ReplicaState & replica, bool disconnect);

    int getReadyFileDescriptor(AsyncCallback async_callback = {});

    void addTimeoutToReplica(ConnectionTimeoutType type, ReplicaState & replica);

    void removeTimeoutsFromReplica(ReplicaState & replica);

    void removeTimeoutFromReplica(ConnectionTimeoutType type, ReplicaState & replica);


    HedgedConnectionsFactory hedged_connections_factory;

    /// All replicas in offset_states[offset] is responsible for process query
    /// with setting parallel_replica_offset = offset. In common situations
    /// replica_states[offset].replicas.size() = 1 (like in MultiplexedConnections).
    std::vector<OffsetState> offset_states;

    /// Map socket file descriptor to replica location (it's offset and index in OffsetState.replicas).
    std::unordered_map<int, ReplicaLocation> fd_to_replica_location;
    /// Map timeout file descriptor to replica location (it's offset and index in OffsetState.replicas).
    std::unordered_map<int, ReplicaLocation> timeout_fd_to_replica_location;

    /// A queue of offsets for new replicas. When we get RECEIVE_DATA_TIMEOUT from
    /// the replica, we push it's offset to this queue and start trying to get
    /// new replica.
    std::queue<int> offsets_queue;

    /// The current number of valid connections to the replicas of this shard.
    size_t active_connection_count;

    /// We count offsets which received first packet of data,
    /// it's needed to cancel choosing new replicas when all offsets
    /// received their first packet of data.
    size_t offsets_with_received_first_data_packet;

    Pipeline pipeline_for_new_replicas;

    /// New replica may not support two-level aggregation due to version incompatibility.
    /// If we didn't disabled it, we need to skip this replica.
    bool disable_two_level_aggregation = false;

    /// This flag means we need to get connection with new replica, but no replica is ready.
    /// When it's true, hedged_connections_factory.getFileDescriptor() is in epoll.
    bool next_replica_in_process = false;

    Epoll epoll;
    const Settings & settings;
    ThrottlerPtr throttler;
    Poco::Logger * log;
    bool sent_query = false;
    bool cancelled = false;

    mutable std::mutex cancel_mutex;
};

}
#endif
