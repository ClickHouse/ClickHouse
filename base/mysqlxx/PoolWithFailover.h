#pragma once

#include <mysqlxx/IPool.h>
#include <mysqlxx/Pool.h>

namespace mysqlxx
{

    struct ReplicaConfiguration
    {
        size_t priority;
        ConnectionConfiguration connection_configuration;
        PoolConfiguration pool_configuration;
    };

    using ReplicasConfigurations = std::vector<ReplicaConfiguration>;

    class PoolWithFailover final : public IPool
    {
    public:

        static std::shared_ptr<PoolWithFailover> create(const ReplicasConfigurations & pools_configuration);

        IPool::Entry getEntry() override;

        IPool::Entry tryGetEntry(size_t timeout_in_milliseconds) override;

    private:
        explicit PoolWithFailover(const ReplicasConfigurations & pools_configuration);

        void returnConnectionToPool(mysqlxx::Connection && connection) override;

        using PoolPtr = std::shared_ptr<Pool>;
        using Replicas = std::vector<PoolPtr>;

        /// [priority][index] -> replica. Highest priority is 0.
        using PriorityToReplicas = std::map<size_t, Replicas>;
        PriorityToReplicas priority_to_replicas;

        Poco::Logger & logger;
        /// Mutex for set of replicas.
        std::mutex mutex;
    };

}
