#pragma once

#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>


namespace DB
{

struct BackupKeeperSettings;

/// In most situations Keeper should be retryable and this tiny class encapsulates all the machinery for make it possible -
/// a Keeper client which injects faults with configurable probability and a retries controller which performs retries with growing backoff.
class WithRetries
{
public:
    using FaultyKeeper = Coordination::ZooKeeperWithFaultInjection::Ptr;
    using RenewerCallback = std::function<void(FaultyKeeper)>;

    enum Kind
    {
        kNormal,
        kInitialization,
        kErrorHandling,
    };

    /// For simplicity a separate ZooKeeperRetriesInfo and a faulty [Zoo]Keeper client
    /// are stored in one place.
    /// This helps to avoid writing too much boilerplate each time we need to
    /// execute some operation (a set of requests) over [Zoo]Keeper with retries.
    /// Why ZooKeeperRetriesInfo is separate for each operation?
    /// The reason is that backup usually takes long time to finish and it makes no sense
    /// to limit the overall number of retries (for example 1000) for the whole backup
    /// and have a continuously growing backoff.
    class RetriesControlHolder
    {
    public:
        ZooKeeperRetriesControl retries_ctl;
        FaultyKeeper faulty_zookeeper;

    private:
        friend class WithRetries;
        RetriesControlHolder(const WithRetries * parent, const String & name);
        RetriesControlHolder(const WithRetries * parent, const String & name, Kind kind);
    };

    RetriesControlHolder createRetriesControlHolderForOperations(const String & name) const;
    RetriesControlHolder createRetriesControlHolderForBackup(const String & name, Kind kind = Kind::kNormal) const;

    WithRetries(
        LoggerPtr log,
        zkutil::GetZooKeeper get_zookeeper_,
        const BackupKeeperSettings & settings_,
        QueryStatusPtr process_list_element_,
        RenewerCallback callback = {});

    WithRetries(
        LoggerPtr log,
        zkutil::GetZooKeeper get_zookeeper_,
        const ZooKeeperRetriesInfo & info_,
        Float64 fault_injection_probability_,
        UInt64 fault_injection_seed_,
        RenewerCallback callback = {});

    ~WithRetries();

    /// Used to re-establish new connection inside a retry loop.
    void renewZooKeeper(RetriesControlHolder & holder) const;

    const BackupKeeperSettings & getBackupKeeperSettings() const;

private:
    /// This will provide a special wrapper which is useful for testing
    FaultyKeeper getFaultyZooKeeper() const;

    LoggerPtr log;
    zkutil::GetZooKeeper get_zookeeper;
    QueryStatusPtr process_list_element;

    /// Only one of the 2 is possible
    std::unique_ptr<BackupKeeperSettings> backup_settings;
    std::unique_ptr<ZooKeeperRetriesInfo> info;

    Float64 fault_injection_probability;
    UInt64 fault_injection_seed;

    /// This callback is called each time when a new [Zoo]Keeper session is created.
    /// In backups it is primarily used to re-create an ephemeral node to signal the coordinator
    /// that the host is alive and able to continue writing the backup.
    /// Coordinator (or an initiator) of the backup also retries when it doesn't find an ephemeral node
    /// for a particular host.
    /// Again, this schema is not ideal. False-positives are still possible, but in worst case scenario
    /// it could lead just to a failed backup which could possibly be successful
    /// if there were a little bit more retries.
    RenewerCallback callback;

    /// This is needed only to protect zookeeper object
    mutable std::mutex zookeeper_mutex;
    mutable zkutil::ZooKeeperPtr zookeeper;
};

}
