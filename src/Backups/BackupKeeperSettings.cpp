#include <Backups/BackupKeeperSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace Setting
{
    extern const SettingsFloat backup_restore_keeper_fault_injection_probability;
    extern const SettingsUInt64 backup_restore_batch_size_for_keeper_multi;
    extern const SettingsUInt64 backup_restore_batch_size_for_keeper_multiread;
    extern const SettingsUInt64 backup_restore_failure_after_host_disconnected_for_seconds;
    extern const SettingsUInt64 backup_restore_finish_timeout_after_error_sec;
    extern const SettingsUInt64 backup_restore_keeper_fault_injection_seed;
    extern const SettingsUInt64 backup_restore_keeper_max_retries;
    extern const SettingsUInt64 backup_restore_keeper_max_retries_while_handling_error;
    extern const SettingsUInt64 backup_restore_keeper_max_retries_while_initializing;
    extern const SettingsUInt64 backup_restore_keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 backup_restore_keeper_retry_max_backoff_ms;
    extern const SettingsUInt64 backup_restore_keeper_value_max_size;
}

BackupKeeperSettings::BackupKeeperSettings(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    const auto & config = context->getConfigRef();

    max_retries = settings[Setting::backup_restore_keeper_max_retries];
    retry_initial_backoff_ms = std::chrono::milliseconds{settings[Setting::backup_restore_keeper_retry_initial_backoff_ms]};
    retry_max_backoff_ms = std::chrono::milliseconds{settings[Setting::backup_restore_keeper_retry_max_backoff_ms]};

    failure_after_host_disconnected_for_seconds = std::chrono::seconds{settings[Setting::backup_restore_failure_after_host_disconnected_for_seconds]};
    max_retries_while_initializing = settings[Setting::backup_restore_keeper_max_retries_while_initializing];
    max_retries_while_handling_error = settings[Setting::backup_restore_keeper_max_retries_while_handling_error];
    finish_timeout_after_error = std::chrono::seconds(settings[Setting::backup_restore_finish_timeout_after_error_sec]);

    if (config.has("backups.sync_period_ms"))
        sync_period_ms = std::chrono::milliseconds{config.getUInt64("backups.sync_period_ms")};
    else
        sync_period_ms = std::chrono::milliseconds{5000};

    if (config.has("backups.max_attempts_after_bad_version"))
        max_attempts_after_bad_version = config.getUInt64("backups.max_attempts_after_bad_version");
    else
        max_attempts_after_bad_version = 10;

    value_max_size = settings[Setting::backup_restore_keeper_value_max_size];
    batch_size_for_multi = settings[Setting::backup_restore_batch_size_for_keeper_multi];
    batch_size_for_multiread = settings[Setting::backup_restore_batch_size_for_keeper_multiread];
    fault_injection_probability = settings[Setting::backup_restore_keeper_fault_injection_probability];
    fault_injection_seed = settings[Setting::backup_restore_keeper_fault_injection_seed];
}

}
