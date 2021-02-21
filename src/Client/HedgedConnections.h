#pragma once
#if defined(OS_LINUX)

#include <functional>
#include <queue>
#include <Client/HedgedConnectionsFactory.h>
#include <Client/IConnections.h>
#include <Client/PacketReceiver.h>
#include <Common/FiberStack.h>
#include <Common/Fiber.h>

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
    using PacketReceiverPtr = std::unique_ptr<PacketReceiver>;
    struct ReplicaState
    {
        explicit ReplicaState(Connection * connection_) : connection(connection_), packet_receiver(std::make_unique<PacketReceiver>(connection_))
        {
        }

        Connection * connection = nullptr;
        PacketReceiverPtr packet_receiver;
        TimerDescriptor change_replica_timeout;
    };

    struct OffsetState
    {
        /// Replicas with the same offset.
        std::vector<ReplicaState> replicas;
        /// An amount of active replicas, when first_packet_of_data_received is true,
        /// active_connection_count is always <= 1 (because we stop working with
        /// other replicas when we receive first data packet from one of them)
        size_t active_connection_count = 0;
        bool first_packet_of_data_received = false;

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
    /// If we don't receive data from replica for receive_data_timeout, we are trying
    /// to get new replica and send query to it. Beside sending query, there are some
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

    void processReceivedFirstDataPacket(const ReplicaLocation & replica_location);

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

    /// We count offsets which received first packet of data,
    /// it's needed to cancel choosing new replicas when all offsets
    /// received their first packet of data.
    size_t offsets_with_received_first_data_packet;

    Pipeline pipeline_for_new_replicas;

    /// New replica may not support two-level aggregation due to version incompatibility.
    /// If we didn't disabled it, we need to skip this replica.
    bool disable_two_level_aggregation = false;

    Packet last_received_packet;

    Epoll epoll;
    const Settings & settings;
    ThrottlerPtr throttler;
    bool sent_query = false;
    bool cancelled = false;

    mutable std::mutex cancel_mutex;
};

}
#endif
