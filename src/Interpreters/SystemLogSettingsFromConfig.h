#pragma once

#include <Common/Exception.h>
#include <Common/SystemLogBase.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <optional>
#include <string>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <class R>
R getConfigOptionTemplated(const Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    if constexpr (std::is_same_v<std::string, R>)
        return config.getString(path);
    if constexpr (std::is_same_v<size_t, R>)
        return config.getUInt64(path);
    if constexpr (std::is_same_v<bool, R>)
        return config.getBool(path);
}

/// `$table_prefix.$directive` takes precedence over `system_tables.$directive`.
template <class R>
std::optional<R> getSystemTableOption(const char * directive, const Poco::Util::AbstractConfiguration & config, const std::string & table_prefix)
{
    const std::string table_config_path = table_prefix + "." + directive;
    if (config.has(table_config_path))
        return std::make_optional(getConfigOptionTemplated<R>(config, table_config_path));

    const std::string system_config_path = std::string("system_tables.") + directive;
    if (config.has(system_config_path))
        return std::make_optional(getConfigOptionTemplated<R>(config, system_config_path));

    return std::nullopt;
}

template <typename TSystemLog>
void readSystemLogQueueSettingsFromConfig(SystemLogQueueSettings & queue_settings, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    queue_settings.flush_interval_milliseconds = getSystemTableOption<size_t>("flush_interval_milliseconds", config, config_prefix).value_or(TSystemLog::getDefaultFlushIntervalMilliseconds());

    queue_settings.max_size_rows = getSystemTableOption<size_t>("max_size_rows", config, config_prefix).value_or(TSystemLog::getDefaultMaxSize());
    if (queue_settings.max_size_rows < 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "max_size_rows for {} is {} should be 1 at least",
                        config_prefix,
                        queue_settings.max_size_rows);

    queue_settings.reserved_size_rows = getSystemTableOption<size_t>("reserved_size_rows", config, config_prefix).value_or(TSystemLog::getDefaultReservedSize());
    if (queue_settings.max_size_rows < queue_settings.reserved_size_rows)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "max_size_rows for {} is {} should be >= reserved_size_rows ({})",
                        config_prefix,
                        queue_settings.max_size_rows,
                        queue_settings.reserved_size_rows);
    }

    queue_settings.buffer_size_rows_flush_threshold = getSystemTableOption<size_t>("buffer_size_rows_flush_threshold", config, config_prefix).value_or(queue_settings.max_size_rows / 2);

    queue_settings.notify_flush_on_crash = getSystemTableOption<bool>("flush_on_crash", config, config_prefix).value_or(TSystemLog::shouldNotifyFlushOnCrash());
}

}
