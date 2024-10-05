#pragma once

#include <Backups/BackupKeeperSettings.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>


namespace DB
{

/// In backups every request to [Zoo]Keeper should be retryable
/// and this tiny class encapsulates all the machinery for make it possible -
/// a [Zoo]Keeper client which injects faults with configurable probability
/// and a retries controller which performs retries with growing backoff.
class WithRetries
{
public:
    using FaultyKeeper = Coordination::ZooKeeperWithFaultInjection::Ptr;

    struct Reason
    {
        bool initialization = false;
        bool error_handling = false;
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
        ZooKeeperRetriesInfo info;
        ZooKeeperRetriesControl retries_ctl;
        FaultyKeeper faulty_zookeeper;

    private:
        friend class WithRetries;
        RetriesControlHolder(const WithRetries * parent, const String & name, const Reason & reason);
    };

    RetriesControlHolder createRetriesControlHolder(const String & name, const Reason & reason = {.initialization = false, .error_handling = false}) const;
    WithRetries(LoggerPtr log, zkutil::GetZooKeeper get_zookeeper_, const BackupKeeperSettings & settings, QueryStatusPtr process_list_element_);

    /// Used to re-establish new connection inside a retry loop.
    void renewZooKeeper(FaultyKeeper my_faulty_zookeeper) const;

    const BackupKeeperSettings & getKeeperSettings() const;
private:
    /// This will provide a special wrapper which is useful for testing
    FaultyKeeper getFaultyZooKeeper() const;

    LoggerPtr log;
    zkutil::GetZooKeeper get_zookeeper;
    BackupKeeperSettings settings;
    QueryStatusPtr process_list_element;

    /// This is needed only to protect zookeeper object
    mutable std::mutex zookeeper_mutex;
    mutable zkutil::ZooKeeperPtr zookeeper;
};

}
