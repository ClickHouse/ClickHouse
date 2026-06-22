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
        String name;
        Field previous_value;
        Field new_value;
        String reason;
    };

    using SettingsChanges = VectorWithMemoryTracking<SettingChange>;
}

/// Process-wide settings-change history: a single `static` table built once (lazily, often first on a
/// query thread applying `compatibility`) with the full history across all versions, then read-only and
/// never freed.
using VersionToSettingsChangesMap = MapWithMemoryTracking<ClickHouseVersion, SettingsChangesHistory::SettingsChanges>;

const VersionToSettingsChangesMap & getSettingsChangesHistory();
const VersionToSettingsChangesMap & getMergeTreeSettingsChangesHistory();

}
