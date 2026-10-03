#pragma once

#include <Core/BaseSettingsFwdMacros.h>
#include <Core/SettingsFields.h>

#include <limits>

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}
namespace DB
{
class ASTSetQuery;
class ASTStorage;
struct SettingChange;
struct DatabaseReplicatedSettingsImpl;

/// List of available types supported in ReplicatedSettings object
#define DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(CLASS_NAME, M) \
    M(CLASS_NAME, Bool) \
    M(CLASS_NAME, Float) \
    M(CLASS_NAME, String) \
    M(CLASS_NAME, UInt64) \
    M(CLASS_NAME, NonZeroUInt64) \
    M(CLASS_NAME, NonZeroUInt32)

DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatedSettings, DECLARE_SETTING_TRAIT)

struct DatabaseReplicatedSettings
{
    DatabaseReplicatedSettings();
    DatabaseReplicatedSettings(const DatabaseReplicatedSettings & settings);
    DatabaseReplicatedSettings(DatabaseReplicatedSettings && settings) noexcept;
    ~DatabaseReplicatedSettings();

    DATABASE_REPLICATED_SETTINGS_SUPPORTED_TYPES(DatabaseReplicatedSettings, DECLARE_SETTING_SUBSCRIPT_OPERATOR)

    void loadFromQuery(ASTStorage & storage_def, bool loading_from_existing_metadata);
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
    void applyChange(const SettingChange & change);

    /// Validates `logs_to_keep` in the `SETTINGS` clause of a `Replicated` database definition against
    /// the 32-bit DDL log counter. An out-of-range value is rejected with `BAD_ARGUMENTS`, or, with
    /// `clamp_on_overflow`, replaced in place by the maximum with a warning in the logs.
    static void checkOrClampLogsToKeep(ASTSetQuery & settings, bool clamp_on_overflow);

    String toString() const;
    bool has(std::string_view name) const;

    static bool hasBuiltin(std::string_view name);

    static UInt32 parseLogsToKeepFromKeeper(const String & logs_to_keep_str, UInt64 * keeper_logs_to_keep = nullptr);

    static constexpr UInt64 MAX_LOGS_TO_KEEP = std::numeric_limits<UInt32>::max();

private:
    std::unique_ptr<DatabaseReplicatedSettingsImpl> impl;
};

}
