#include <Backups/WithRetries.h>

#include <mutex>


namespace DB
{

WithRetries::WithRetries(
    LoggerPtr log_, zkutil::GetZooKeeper get_zookeeper_, const BackupKeeperSettings & settings_, QueryStatusPtr process_list_element_, RenewerCallback callback_)
    : log(log_)
    , get_zookeeper(get_zookeeper_)
    , settings(settings_)
    , process_list_element(process_list_element_)
    , callback(callback_)
{}

WithRetries::RetriesControlHolder::RetriesControlHolder(const WithRetries * parent, const String & name, Kind kind)
    : info(  (kind == kInitialization) ? parent->settings.max_retries_while_initializing
           : (kind == kErrorHandling)  ? parent->settings.max_retries_while_handling_error
                                       : parent->settings.max_retries,
           parent->settings.retry_initial_backoff_ms.count(),
           parent->settings.retry_max_backoff_ms.count())
    /// We don't use process_list_element while handling an error because the error handling can't be cancellable.
    , retries_ctl(name, parent->log, info, (kind == kErrorHandling) ? nullptr : parent->process_list_element)
    , faulty_zookeeper(parent->getFaultyZooKeeper())
{}

WithRetries::RetriesControlHolder WithRetries::createRetriesControlHolder(const String & name, Kind kind) const
{
    return RetriesControlHolder(this, name, kind);
}

void WithRetries::renewZooKeeper(FaultyKeeper my_faulty_zookeeper) const
{
    std::lock_guard lock(zookeeper_mutex);

    if (!zookeeper || zookeeper->expired())
    {
        zookeeper = get_zookeeper();
        my_faulty_zookeeper->setKeeper(zookeeper);
        if (callback)
            callback(my_faulty_zookeeper);
    }
    else
    {
        my_faulty_zookeeper->setKeeper(zookeeper);
    }
}

const BackupKeeperSettings & WithRetries::getKeeperSettings() const
{
    return settings;
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
        settings.fault_injection_probability,
        settings.fault_injection_seed,
        current_zookeeper,
        log->name(),
        log);

    return faulty_zookeeper;
}


}
