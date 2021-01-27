#pragma once

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
    using Replicas = GetHedgedConnections::Replicas;

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

    Packet receivePacketUnlocked(AsyncCallback async_callback = {}) override;

    void disconnect() override;

    void sendCancel() override;

    Packet drain() override;

    std::string dumpAddresses() const override;

    size_t size() const override;

    bool hasActiveConnections() const override { return !active_connections_count_by_offset.empty(); }

private:
    class Pipeline
    {
    public:
        void add(std::function<void(ReplicaStatePtr &)> send_function);

        void run(ReplicaStatePtr & replica);

        bool empty() const { return pipeline.empty(); }

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
    std::vector<std::vector<ReplicaStatePtr>> replicas;
    std::unordered_map<int, ReplicaStatePtr> fd_to_replica;
    std::unordered_map<int, ReplicaStatePtr> timeout_fd_to_replica;
    std::queue<int> offsets_queue;
    Epoll epoll;
    const Settings & settings;
    ThrottlerPtr throttler;
    Poco::Logger * log;
    Pipeline pipeline_for_new_replicas;
    bool sent_query = false;
    bool cancelled = false;
    std::unordered_map<size_t, size_t> active_connections_count_by_offset;
    bool next_replica_in_process = false;
    bool has_two_level_aggregation_incompatibility = false;
    std::unordered_set<size_t> offsets_with_received_data;

    mutable std::mutex cancel_mutex;
};

}
