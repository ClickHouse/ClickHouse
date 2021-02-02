#pragma once
#if defined(OS_LINUX)

#include <Client/GetHedgedConnections.h>
#include <Client/IConnections.h>
#include <functional>
#include <queue>

namespace DB
{

class HedgedConnections : public IConnections
{
public:
    using ReplicaStatePtr = GetHedgedConnections::ReplicaStatePtr;

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

    Packet drain() override;

    std::string dumpAddresses() const override;

    size_t size() const override { return replicas.size(); }

    bool hasActiveConnections() const override { return !active_connections_count_by_offset.empty(); }

private:
    /// We will save actions with replicas in pipeline to perform them on the new replicas.
    class Pipeline
    {
    public:
        void add(std::function<void(ReplicaStatePtr &)> send_function);

        void run(ReplicaStatePtr & replica);
    private:
        std::vector<std::function<void(ReplicaStatePtr &)>> pipeline;
    };

    Packet receivePacketFromReplica(ReplicaStatePtr & replica, AsyncCallback async_callback = {});

    Packet receivePacketImpl(AsyncCallback async_callback = {});

    void processReceiveData(ReplicaStatePtr & replica);

    void processTimeoutEvent(ReplicaStatePtr & replica, TimerDescriptorPtr timeout_descriptor);

    void tryGetNewReplica();

    void finishProcessReplica(ReplicaStatePtr & replica, bool disconnect);

    int getReadyFileDescriptor(AsyncCallback async_callback = {});

    GetHedgedConnections get_hedged_connections;

    /// All replicas in replicas[offset] are responsible for process query
    /// with setting parallel_replica_offset = offset. In common situations
    /// replicas[offset].size() = 1 (like in MultiplexedConnections).
    std::vector<std::vector<ReplicaStatePtr>> replicas;

    /// Map socket file descriptor to replica.
    std::unordered_map<int, ReplicaStatePtr> fd_to_replica;
    /// Map timeout file descriptor to replica.
    std::unordered_map<int, ReplicaStatePtr> timeout_fd_to_replica;

    /// A queue of offsets for new replicas. When we get RECEIVE_DATA_TIMEOUT from
    /// the replica, we push it's offset to this queue and start trying to get
    /// new replica.
    std::queue<int> offsets_queue;

    /// Map offset to amount of active connections, responsible to this offset.
    std::unordered_map<size_t, size_t> active_connections_count_by_offset;

    std::unordered_set<size_t> offsets_with_received_data;

    Pipeline pipeline_for_new_replicas;

    /// New replica may not support two-level aggregation due to version incompatibility.
    /// If we didn't disabled it, we need to skip this replica.
    bool disable_two_level_aggregation = false;

    /// next_replica_in_process is true when get_hedged_connections.getFileDescriptor()
    /// is in epoll now and false otherwise.
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
