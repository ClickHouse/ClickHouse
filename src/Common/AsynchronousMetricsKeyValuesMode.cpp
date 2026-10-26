#include <Common/AsynchronousMetricsKeyValuesMode.h>

#include <Core/SettingsEnums.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

AsynchronousMetricsKeyValuesMode getAsynchronousMetricsKeyValuesMode(const Poco::Util::AbstractConfiguration & config)
{
    /// The value is read from the live configuration and not from `Context::getServerSettings()`, because the
    /// latter is a snapshot taken at startup which `SYSTEM RELOAD CONFIG` does not refresh, and this setting
    /// is meant to be switchable on a running server. The default below must be kept in sync with the
    /// declaration of the setting in `ServerSettings`.
    static const std::string key = "asynchronous_metrics_key_values_mode";

    if (!config.has(key))
        return AsynchronousMetricsKeyValuesMode::KeyValues;

    return SettingFieldAsynchronousMetricsKeyValuesModeTraits::fromString(config.getString(key));
}

}
