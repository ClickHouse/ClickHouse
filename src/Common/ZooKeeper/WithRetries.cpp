#include <Common/ZooKeeper/WithRetries.h>
#include <Backups/BackupKeeperSettings.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

WithRetries::WithRetries(
    LoggerPtr log_,
    zkutil::GetZooKeeper get_zookeeper_,
    const BackupKeeperSettings & settings_,
    QueryStatusPtr process_list_element_,
    RenewerCallback callback_)
    : log(log_)
    , get_zookeeper(get_zookeeper_)
    , process_list_element(process_list_element_)
    , backup_settings(std::make_unique<BackupKeeperSettings>(settings_))
    , info(nullptr)
    , fault_injection_probability(settings_.fault_injection_probability)
    , fault_injection_seed(settings_.fault_injection_seed)
    , callback(callback_)
{
}

WithRetries::WithRetries(
    LoggerPtr log_,
    zkutil::GetZooKeeper get_zookeeper_,
    const ZooKeeperRetriesInfo & info_,
    Float64 fault_injection_probability_,
    UInt64 fault_injection_seed_,
    RenewerCallback callback_)
    : log(log_)
    , get_zookeeper(get_zookeeper_)
    , process_list_element(info_.query_status)
    , backup_settings(nullptr)
    , info(std::make_unique<ZooKeeperRetriesInfo>(info_))
    , fault_injection_probability(fault_injection_probability_)
    , fault_injection_seed(fault_injection_seed_)
    , callback(callback_)
{
}

WithRetries::~WithRetries() = default;

WithRetries::RetriesControlHolder::RetriesControlHolder(const WithRetries * parent, const String & name)
    : retries_ctl(name, parent->log, *parent->info)
    , faulty_zookeeper(parent->getFaultyZooKeeper())
{
}

WithRetries::RetriesControlHolder::RetriesControlHolder(const WithRetries * parent, const String & name, Kind kind)
    /// We don't use process_list_element while handling an error because the error handling can't be cancellable.
    : retries_ctl(
          name,
          parent->log,
          {(kind == kInitialization)      ? parent->backup_settings->max_retries_while_initializing
               : (kind == kErrorHandling) ? parent->backup_settings->max_retries_while_handling_error
                                          : parent->backup_settings->max_retries,
           static_cast<UInt64>(parent->backup_settings->retry_initial_backoff_ms.count()),
           static_cast<UInt64>(parent->backup_settings->retry_max_backoff_ms.count()),
           (kind == kErrorHandling) ? nullptr : parent->process_list_element})
    , faulty_zookeeper(parent->getFaultyZooKeeper())
{
}


WithRetries::RetriesControlHolder WithRetries::createRetriesControlHolderForOperations(const String & name) const
{
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tried to get a controller for normal operations, but this is a controller for backups");

    return RetriesControlHolder(this, name);
}

WithRetries::RetriesControlHolder WithRetries::createRetriesControlHolderForBackup(const String & name, Kind kind) const
{
    if (!backup_settings)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tried to get backup settings, but this is a controller for normal operations");
    return RetriesControlHolder(this, name, kind);
}

void WithRetries::renewZooKeeper(RetriesControlHolder & holder) const
{
    std::lock_guard lock(zookeeper_mutex);

    if (!zookeeper || zookeeper->expired())
    {
        zookeeper = get_zookeeper(holder.retries_ctl.getCurrentBackoffMs());
        holder.faulty_zookeeper->setKeeper(zookeeper);
        if (callback)
            callback(holder.faulty_zookeeper);
    }
    else
    {
        holder.faulty_zookeeper->setKeeper(zookeeper);
    }
}

const BackupKeeperSettings & WithRetries::getBackupKeeperSettings() const
{
    if (!backup_settings)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tried to get backup settings, but this is a controller for normal operations");
    return *backup_settings;
}

WithRetries::FaultyKeeper WithRetries::getFaultyZooKeeper() const
{
    zkutil::ZooKeeperPtr current_zookeeper;
    {
        std::lock_guard lock(zookeeper_mutex);
        current_zookeeper = zookeeper;
    }

    /// We need to create new instance of ZooKeeperWithFaultInjection each time and copy a pointer to ZooKeeper client there
    /// The reason is that ZooKeeperWithFaultInjection may reset the underlying pointer and there could be a race condition
    /// when the same object is used from multiple threads.
    auto faulty_zookeeper = ZooKeeperWithFaultInjection::createInstance(
        fault_injection_probability, fault_injection_seed, current_zookeeper, log->name(), log);

    return faulty_zookeeper;
}


}
