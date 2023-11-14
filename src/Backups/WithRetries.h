#pragma once

#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/KeeperRetriesController.h>

namespace DB
{

class ZooKeeperWithFaultInjection;
using ZooKeeperWithFaultInjectionPtr = std::shared_ptr<ZooKeeperWithFaultInjection>;

/// In backups every request to [Zoo]Keeper should be retryable
/// and this tiny class encapsulates all the machinery for make it possible -
/// a [Zoo]Keeper client which injects faults with configurable probability
/// and a retries controller which performs retries with growing backoff.
class WithRetries
{
public:
    using FaultyKeeper = ZooKeeperWithFaultInjectionPtr;
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

        static KeeperSettings fromSettings(const Settings & settings)
        {
            return KeeperSettings{
                .keeper_max_retries = settings.keeper_max_retries,
                .keeper_retry_initial_backoff_ms = settings.keeper_retry_initial_backoff_ms,
                .keeper_retry_max_backoff_ms = settings.keeper_retry_max_backoff_ms,
                .batch_size_for_keeper_multiread = settings.backup_restore_batch_size_for_keeper_multiread,
                .keeper_fault_injection_probability = settings.keeper_fault_injection_probability,
                .keeper_fault_injection_seed = settings.keeper_fault_injection_seed,
                .keeper_value_max_size = settings.backup_restore_keeper_value_max_size};
        }
    };

    /// For simplicity a separate KeeperRetriesInfo and a faulty Keeper client
    /// are stored in one place.
    /// This helps to avoid writing too much boilerplate each time we need to
    /// execute some operation (a set of requests) over [Zoo]Keeper with retries.
    /// Why KeeperRetriesInfo is separate for each operation?
    /// The reason is that backup usually takes long time to finish and it makes no sense
    /// to limit the overall number of retries (for example 1000) for the whole backup
    /// and have a continuously growing backoff.
    class RetriesControlHolder
    {
    public:
        KeeperRetriesInfo info;
        KeeperRetriesControl retries_ctl;
        FaultyKeeper faulty_zookeeper;

    private:
        friend class WithRetries;
        RetriesControlHolder(const WithRetries * parent, const String & name);
    };

    RetriesControlHolder createRetriesControlHolder(const String & name);
    WithRetries(Poco::Logger * log, zkutil::GetZooKeeper get_zookeeper_, const KeeperSettings & settings, RenewerCallback callback);

    /// Used to re-establish new connection inside a retry loop.
    void renewZooKeeper(FaultyKeeper my_faulty_zookeeper) const;
private:
    /// This will provide a special wrapper which is useful for testing
    FaultyKeeper getFaultyZooKeeper() const;

    Poco::Logger * log;
    zkutil::GetZooKeeper get_zookeeper;
    KeeperSettings settings;
    /// This callback is called each time when a new [Zoo]Keeper session is created.
    /// In backups it is primarily used to re-create an ephemeral node to signal the coordinator
    /// that the host is alive and able to continue writing the backup.
    /// Coordinator (or an initiator) of the backup also retries when it doesn't find an ephemeral node
    /// for a particular host.
    /// Again, this schema is not ideal. False-positives are still possible, but in worst case scenario
    /// it could lead just to a failed backup which could possibly be successful
    /// if there were a little bit more retries.
    RenewerCallback callback;
    KeeperRetriesInfo global_zookeeper_retries_info;

    /// This is needed only to protect zookeeper object
    mutable std::mutex zookeeper_mutex;
};

}
