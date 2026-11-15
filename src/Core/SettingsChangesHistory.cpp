#include <Core/SettingsChangesHistory.h>

#include <Common/Exception.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void addSettingChangesHistory(
    VersionToSettingsChangesMap & history,
    std::string_view setting_name,
    std::initializer_list<SettingsChangesHistory::SettingChangeRecord> records)
{
    std::optional<ClickHouseVersion> newer_version;
    for (const auto & record : records)
    {
        ClickHouseVersion version(record.version);
        if (newer_version && version >= *newer_version)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The history of setting '{}' must list each version once, newest first, but '{}' follows '{}'",
                setting_name, version.toString(), newer_version->toString());
        newer_version = version;

        history[version].push_back(
            {String(setting_name), record.previous_value, record.new_value, String(record.reason), record.compatibility_mode});
    }
}

}
