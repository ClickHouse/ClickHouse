#pragma once

#include <Client/GetHedgedConnections.h>
#include <Client/IConnections.h>
#include <functional>

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

    bool hasActiveConnections() const override;

private:
    class Pipeline
    {
    public:
        void add(std::function<void(ReplicaStatePtr)> send_function);

        void run(ReplicaStatePtr replica);

        bool empty() const { return pipeline.empty(); }

    private:
        std::vector<std::function<void(ReplicaStatePtr)>> pipeline;
    };

    void processChosenSecondReplica();

    Packet receivePacketFromReplica(ReplicaStatePtr replica, AsyncCallback async_callback = {});

    Packet receivePacketImpl(AsyncCallback async_callback = {});

    void processReceiveData(ReplicaStatePtr replica);

    void processTimeoutEvent(ReplicaStatePtr & replica, TimerDescriptorPtr timeout_descriptor);

    void processGetHedgedConnectionsEvent();

    void removeReceiveTimeout(ReplicaStatePtr replica);

    void finishProcessReplica(ReplicaStatePtr replica, bool disconnect);

    GetHedgedConnections get_hedged_connections;
    Replicas replicas;
    Epoll epoll;
    const Settings & settings;
    ThrottlerPtr throttler;
    Poco::Logger * log;
    Pipeline second_replica_pipeline;
    bool sent_query = false;
    bool cancelled = false;

    mutable std::mutex cancel_mutex;
};

}
