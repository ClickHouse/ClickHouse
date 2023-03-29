#pragma once

#include <Storages/MergeTree/ZooKeeperRetries.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

namespace DB
{

class WithRetries
{
public:
    using FaultyKeeper = Coordination::ZooKeeperWithFaultInjection::Ptr;
    using RenewerCallback = std::function<void(FaultyKeeper &)>;

    struct KeeperSettings
    {
        UInt64 keeper_max_retries{0};
        UInt64 keeper_retry_initial_backoff_ms{0};
        UInt64 keeper_retry_max_backoff_ms{0};
        UInt64 batch_size_for_keeper_multiread{10000};
        Float64 keeper_fault_injection_probability{0};
        UInt64 keeper_fault_injection_seed{42};
        UInt64 keeper_value_max_size{1048576};
    };

    class RetriesControlHolder
    {
    public:
        ZooKeeperRetriesInfo info;
        ZooKeeperRetriesControl retries_ctl;
        FaultyKeeper faulty_zookeeper;

    private:
        friend class WithRetries;
        RetriesControlHolder(const WithRetries * parent, const String & name);
    };

    RetriesControlHolder createRetriesControlHolder(const String & name);
    WithRetries(Poco::Logger * log, zkutil::GetZooKeeper get_zookeeper_, const KeeperSettings & settings, RenewerCallback callback);

    /// This will provide a special wrapper which is useful for testing
    FaultyKeeper getFaultyZooKeeper() const;
    /// Used to re-establish new connection inside a retry loop.
    void renewZooKeeper(FaultyKeeper my_faulty_zookeeper) const;

private:
    Poco::Logger * log;
    zkutil::GetZooKeeper get_zookeeper;
    KeeperSettings settings;
    RenewerCallback callback;
    ZooKeeperRetriesInfo global_zookeeper_retries_info;

    /// This is needed only to protect zookeeper object
    mutable std::mutex zookeeper_mutex;
    mutable zkutil::ZooKeeperPtr zookeeper;
};

}
