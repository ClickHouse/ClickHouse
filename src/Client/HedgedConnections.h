#pragma once
#if defined(OS_LINUX)

#include <functional>
#include <queue>
#include <optional>

#include <Client/HedgedConnectionsFactory.h>
#include <Client/IConnections.h>
#include <Client/PacketReceiver.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/** To receive data from multiple replicas (connections) from one shard asynchronously.
  * The principe of Hedged Connections is used to reduce tail latency:
  * if we don't receive data from replica and there is no progress in query execution
  * for a long time, we try to get new replica and send query to it,
  * without cancelling working with previous replica. This class
  * supports all functionality that MultipleConnections has.
  */
class HedgedConnections : public IConnections
{
public:
    using PacketReceiverPtr = std::unique_ptr<PacketReceiver>;
    struct ReplicaState
    {
        explicit ReplicaState(Connection * connection_) : connection(connection_), packet_receiver(std::make_unique<PacketReceiver>(connection_))
        {
        }

        Connection * connection = nullptr;
        PacketReceiverPtr packet_receiver;
        TimerDescriptor change_replica_timeout;
        bool is_change_replica_timeout_expired = false;
    };

    struct OffsetState
    {
        /// Replicas with the same offset.
        std::vector<ReplicaState> replicas;
        /// An amount of active replicas. When can_change_replica is false,
        /// active_connection_count is always <= 1 (because we stopped working with
        /// other replicas with the same offset)
        size_t active_connection_count = 0;
        bool can_change_replica = true;

        /// This flag is true when this offset is in queue for
        /// new replicas. It's needed to process receive timeout
        /// (throw an exception when receive timeout expired and there is no
        /// new replica in process)
        bool next_replica_in_process = false;
    };

    /// We process events in epoll, so we need to determine replica by it's
    /// file descriptor. We store map fd -> replica location. To determine
    /// where replica is, we need a replica offset
    /// (the same as parallel_replica_offset), and index, which is needed because
    /// we can have many replicas with same offset (when receive_data_timeout has expired).
    struct ReplicaLocation
    {
        size_t offset;
        size_t index;
    };

    HedgedConnections(const ConnectionPoolWithFailoverPtr & pool_,
                      ContextPtr context_,
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
        ClientInfo & client_info,
        bool with_pending_data) override;

    void sendReadTaskResponse(const String &) override
    {
        throw Exception("sendReadTaskResponse in not supported with HedgedConnections", ErrorCodes::LOGICAL_ERROR);
    }

    void sendMergeTreeReadTaskResponse(PartitionReadResponse) override
    {
        throw Exception("sendMergeTreeReadTaskResponse in not supported with HedgedConnections", ErrorCodes::LOGICAL_ERROR);
    }

    Packet receivePacket() override;

    Packet receivePacketUnlocked(AsyncCallback async_callback, bool is_draining) override;

    void disconnect() override;

    void sendCancel() override;

    void sendIgnoredPartUUIDs(const std::vector<UUID> & uuids) override;

    Packet drain() override;

    std::string dumpAddresses() const override;

    size_t size() const override { return offset_states.size(); }

    bool hasActiveConnections() const override { return active_connection_count > 0; }

    void setReplicaInfo(ReplicaInfo value) override { replica_info = value; }

private:
    /// If we don't receive data from replica and there is no progress in query
    /// execution for receive_data_timeout, we are trying to get new
    /// replica and send query to it. Beside sending query, there are some
    /// additional actions like sendScalarsData or sendExternalTablesData and we need
    /// to perform these actions in the same order on the new replica. So, we will
    /// save actions with replicas in pipeline to perform them on the new replicas.
    class Pipeline
    {
    public:
        void add(std::function<void(ReplicaState &)> send_function);

        void run(ReplicaState & replica);
    private:
        std::vector<std::function<void(ReplicaState &)>> pipeline;
    };

    Packet receivePacketFromReplica(const ReplicaLocation & replica_location);

    ReplicaLocation getReadyReplicaLocation(AsyncCallback async_callback = {});

    bool resumePacketReceiver(const ReplicaLocation & replica_location);

    void disableChangingReplica(const ReplicaLocation & replica_location);

    void startNewReplica();

    void checkNewReplica();

    void processNewReplicaState(HedgedConnectionsFactory::State state, Connection * connection);

    void finishProcessReplica(ReplicaState & replica, bool disconnect);

    int getReadyFileDescriptor(AsyncCallback async_callback = {});

    HedgedConnectionsFactory hedged_connections_factory;

    /// All replicas in offset_states[offset] is responsible for process query
    /// with setting parallel_replica_offset = offset. In common situations
    /// replica_states[offset].replicas.size() = 1 (like in MultiplexedConnections).
    std::vector<OffsetState> offset_states;

    /// Map socket file descriptor to replica location (it's offset and index in OffsetState.replicas).
    std::unordered_map<int, ReplicaLocation> fd_to_replica_location;

    /// Map receive data timeout file descriptor to replica location.
    std::unordered_map<int, ReplicaLocation> timeout_fd_to_replica_location;

    /// A queue of offsets for new replicas. When we get RECEIVE_DATA_TIMEOUT from
    /// the replica, we push it's offset to this queue and start trying to get
    /// new replica.
    std::queue<int> offsets_queue;

    /// The current number of valid connections to the replicas of this shard.
    size_t active_connection_count;

    /// We count offsets in which we can't change replica anymore,
    /// it's needed to cancel choosing new replicas when we
    /// disabled replica changing in all offsets.
    size_t offsets_with_disabled_changing_replica;

    Pipeline pipeline_for_new_replicas;

    /// New replica may not support two-level aggregation due to version incompatibility.
    /// If we didn't disabled it, we need to skip this replica.
    bool disable_two_level_aggregation = false;

    /// We will save replica with last received packet
    /// (except cases when packet type is EndOfStream or Exception)
    /// to resume it's packet receiver when new packet is needed.
    std::optional<ReplicaLocation> replica_with_last_received_packet;

    Packet last_received_packet;

    Epoll epoll;
    ContextPtr context;
    const Settings & settings;

    /// The following two fields are from settings but can be referenced outside the lifetime of
    /// settings when connection is drained asynchronously.
    Poco::Timespan drain_timeout;
    bool allow_changing_replica_until_first_data_packet;

    ThrottlerPtr throttler;
    bool sent_query = false;
    bool cancelled = false;

    ReplicaInfo replica_info;

    mutable std::mutex cancel_mutex;
};

}
#endif
