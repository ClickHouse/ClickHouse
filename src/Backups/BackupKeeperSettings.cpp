#include <Backups/BackupKeeperSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 backup_restore_keeper_max_retries;
    extern const SettingsUInt64 backup_restore_keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 backup_restore_keeper_retry_max_backoff_ms;
    extern const SettingsUInt64 backup_restore_failure_after_host_disconnected_for_seconds;
    extern const SettingsUInt64 backup_restore_keeper_value_max_size;
    extern const SettingsUInt64 backup_restore_batch_size_for_keeper_multi;
    extern const SettingsUInt64 backup_restore_batch_size_for_keeper_multiread;
    extern const SettingsFloat backup_restore_keeper_fault_injection_probability;
    extern const SettingsUInt64 backup_restore_keeper_fault_injection_seed;
}

BackupKeeperSettings BackupKeeperSettings::fromContext(const ContextPtr & context)
{
    BackupKeeperSettings keeper_settings;

    const auto & settings = context->getSettingsRef();
    const auto & config = context->getConfigRef();

    keeper_settings.max_retries = settings[Setting::backup_restore_keeper_max_retries];
    keeper_settings.retry_initial_backoff_ms = std::chrono::milliseconds{settings[Setting::backup_restore_keeper_retry_initial_backoff_ms]};
    keeper_settings.retry_max_backoff_ms = std::chrono::milliseconds{settings[Setting::backup_restore_keeper_retry_max_backoff_ms]};
    keeper_settings.failure_after_host_disconnected_for_seconds = std::chrono::seconds{settings[Setting::backup_restore_failure_after_host_disconnected_for_seconds]};

    if (config.has("backups.sync_period_ms"))
        keeper_settings.sync_period_ms = std::chrono::milliseconds{config.getUInt64("backups.sync_period_ms")};

    if (config.has("backups.on_cluster_initialization_timeout_sec"))
        keeper_settings.on_cluster_initialization_timeout = std::chrono::seconds{config.getUInt64("backups.on_cluster_initialization_timeout_sec")};
    else if (config.has("backups.on_cluster_first_sync_timeout")) /// obsolete name
        keeper_settings.on_cluster_initialization_timeout = std::chrono::seconds{config.getUInt64("backups.on_cluster_first_sync_timeout") / 1000};

    if (config.has("backups.max_retries_while_initializing"))
        keeper_settings.max_retries_while_initializing = config.getUInt64("backups.max_retries_while_initializing");

    if (config.has("backups.on_cluster_error_handling_timeout_sec"))
        keeper_settings.on_cluster_error_handling_timeout = std::chrono::seconds{config.getUInt64("backups.on_cluster_error_handling_timeout_sec")};

    if (config.has("backups.max_retries_while_handling_error"))
        keeper_settings.max_retries_while_handling_error = config.getUInt64("backups.max_retries_while_handling_error");

    if (config.has("backups.max_attempts_after_bad_version"))
        keeper_settings.max_attempts_after_bad_version = config.getUInt64("backups.max_attempts_after_bad_version");

    keeper_settings.value_max_size = settings[Setting::backup_restore_keeper_value_max_size];
    keeper_settings.batch_size_for_multi = settings[Setting::backup_restore_batch_size_for_keeper_multi];
    keeper_settings.batch_size_for_multiread = settings[Setting::backup_restore_batch_size_for_keeper_multiread];
    keeper_settings.fault_injection_probability = settings[Setting::backup_restore_keeper_fault_injection_probability];
    keeper_settings.fault_injection_seed = settings[Setting::backup_restore_keeper_fault_injection_seed];

    return keeper_settings;
}

}
