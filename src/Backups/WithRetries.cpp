#include <Backups/WithRetries.h>
#include <Core/Settings.h>

#include <mutex>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 backup_restore_keeper_max_retries;
    extern const SettingsUInt64 backup_restore_keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 backup_restore_keeper_retry_max_backoff_ms;
    extern const SettingsUInt64 backup_restore_batch_size_for_keeper_multiread;
    extern const SettingsFloat backup_restore_keeper_fault_injection_probability;
    extern const SettingsUInt64 backup_restore_keeper_fault_injection_seed;
    extern const SettingsUInt64 backup_restore_keeper_value_max_size;
    extern const SettingsUInt64 backup_restore_batch_size_for_keeper_multi;
}

WithRetries::KeeperSettings WithRetries::KeeperSettings::fromContext(ContextPtr context)
{
    return
    {
        .keeper_max_retries = context->getSettingsRef()[Setting::backup_restore_keeper_max_retries],
        .keeper_retry_initial_backoff_ms = context->getSettingsRef()[Setting::backup_restore_keeper_retry_initial_backoff_ms],
        .keeper_retry_max_backoff_ms = context->getSettingsRef()[Setting::backup_restore_keeper_retry_max_backoff_ms],
        .batch_size_for_keeper_multiread = context->getSettingsRef()[Setting::backup_restore_batch_size_for_keeper_multiread],
        .keeper_fault_injection_probability = context->getSettingsRef()[Setting::backup_restore_keeper_fault_injection_probability],
        .keeper_fault_injection_seed = context->getSettingsRef()[Setting::backup_restore_keeper_fault_injection_seed],
        .keeper_value_max_size = context->getSettingsRef()[Setting::backup_restore_keeper_value_max_size],
        .batch_size_for_keeper_multi = context->getSettingsRef()[Setting::backup_restore_batch_size_for_keeper_multi],
    };
}

WithRetries::WithRetries(
    LoggerPtr log_, zkutil::GetZooKeeper get_zookeeper_, const KeeperSettings & settings_, QueryStatusPtr process_list_element_, RenewerCallback callback_)
    : log(log_)
    , get_zookeeper(get_zookeeper_)
    , settings(settings_)
    , process_list_element(process_list_element_)
    , callback(callback_)
    , global_zookeeper_retries_info(
          settings.keeper_max_retries, settings.keeper_retry_initial_backoff_ms, settings.keeper_retry_max_backoff_ms)
{}

WithRetries::RetriesControlHolder::RetriesControlHolder(const WithRetries * parent, const String & name)
    : info(parent->global_zookeeper_retries_info)
    , retries_ctl(name, parent->log, info, parent->process_list_element)
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
    else
    {
        my_faulty_zookeeper->setKeeper(zookeeper);
    }
}

const WithRetries::KeeperSettings & WithRetries::getKeeperSettings() const
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
        settings.keeper_fault_injection_probability,
        settings.keeper_fault_injection_seed,
        current_zookeeper,
        log->name(),
        log);

    return faulty_zookeeper;
}


}
