#include <mutex>
#include <Backups/WithRetries.h>

namespace DB
{


WithRetries::WithRetries(Poco::Logger * log_, zkutil::GetZooKeeper get_zookeeper_, const KeeperSettings & settings_, RenewerCallback callback_)
    : log(log_)
    , get_zookeeper(get_zookeeper_)
    , settings(settings_)
    , callback(callback_)
    , global_zookeeper_retries_info(
        log->name(),
        log,
        settings.keeper_max_retries,
        settings.keeper_retry_initial_backoff_ms,
        settings.keeper_retry_max_backoff_ms)
{}

WithRetries::RetriesControlHolder::RetriesControlHolder(const WithRetries * parent, const String & name)
    : info(parent->global_zookeeper_retries_info)
    , retries_ctl(name, info, nullptr)
    , faulty_zookeeper(parent->getFaultyZooKeeper())
{}

WithRetries::RetriesControlHolder WithRetries::createRetriesControlHolder(const String & name)
{
    return RetriesControlHolder(this, name);
}

void WithRetries::renewZooKeeper(FaultyKeeper my_faulty_zookeeper) const
{
    std::lock_guard lock(zookeeper_mutex);

    if (!zookeeper || zookeeper->expired())
    {
        zookeeper = get_zookeeper();
        my_faulty_zookeeper->setKeeper(zookeeper);

        callback(my_faulty_zookeeper);
    }
}

WithRetries::FaultyKeeper WithRetries::getFaultyZooKeeper() const
{
    /// We need to create new instance of ZooKeeperWithFaultInjection each time a copy a pointer to ZooKeeper client there
    /// The reason is that ZooKeeperWithFaultInjection may reset the underlying pointer and there could be a race condition
    /// when the same object is used from multiple threads.
    auto faulty_zookeeper = ZooKeeperWithFaultInjection::createInstance(
        settings.keeper_fault_injection_probability,
        settings.keeper_fault_injection_seed,
        zookeeper,
        log->name(),
        log);

    return faulty_zookeeper;
}


}
