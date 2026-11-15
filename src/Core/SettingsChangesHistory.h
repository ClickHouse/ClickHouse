#pragma once

#include <Core/Field.h>

#include <Common/ClickHouseVersion.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <initializer_list>
#include <string_view>

namespace DB
{

namespace SettingsChangesHistory
{
    struct SettingChange
    {
        enum class CompatibilitySetting
        {
            /// Restore `previous_value` when `compatibility` requests an older version.
            Apply,
            /// Block rollback of this change and all earlier changes to the same setting.
            Ignore,
        };

        String name;
        Field previous_value;
        Field new_value;
        String reason;
        CompatibilitySetting compatibility_mode = CompatibilitySetting::Apply;
    };

    using SettingsChanges = VectorWithMemoryTracking<SettingChange>;

    /// One change of one setting, written as a trailing argument of the `DECLARE` of that setting.
    struct SettingChangeRecord
    {
        std::string_view version;
        Field previous_value;
        Field new_value;
        std::string_view reason;
        SettingChange::CompatibilitySetting compatibility_mode = SettingChange::CompatibilitySetting::Apply;
    };
}

using VersionToSettingsChangesMap = MapWithMemoryTracking<ClickHouseVersion, SettingsChangesHistory::SettingsChanges>;

/// Adds the records of one setting to `history`. The records must be newest first, one per version.
void addSettingChangesHistory(
    VersionToSettingsChangesMap & history,
    std::string_view setting_name,
    std::initializer_list<SettingsChangesHistory::SettingChangeRecord> records);

/// Both return a reference to a static map that is filled once and never changes afterwards, so a
/// pointer to a change, or to one of its values, stays valid for the lifetime of the process.
const VersionToSettingsChangesMap & getSettingsChangesHistory();
const VersionToSettingsChangesMap & getMergeTreeSettingsChangesHistory();

}
