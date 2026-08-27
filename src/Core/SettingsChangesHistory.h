#pragma once

#include <Core/Field.h>

#include <Common/ClickHouseVersion.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <map>
#include <vector>

namespace DB
{

namespace SettingsChangesHistory
{
    struct SettingChange
    {
        enum class CompatibilityMode
        {
            RollbackToOld,
            StartUsingNew,
        };

        String name;
        Field previous_value;
        Field new_value;
        String reason;
        CompatibilityMode compatibility_mode = CompatibilityMode::RollbackToOld;
    };

    using SettingsChanges = VectorWithMemoryTracking<SettingChange>;
}

using VersionToSettingsChangesMap = MapWithMemoryTracking<ClickHouseVersion, SettingsChangesHistory::SettingsChanges>;

const VersionToSettingsChangesMap & getSettingsChangesHistory();
const VersionToSettingsChangesMap & getMergeTreeSettingsChangesHistory();

}
