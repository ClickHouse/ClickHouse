#pragma once

#include <Core/Field.h>
#include <map>
#include <vector>


namespace DB
{

class ClickHouseVersion
{
public:
    explicit ClickHouseVersion(std::string_view version);

    String toString() const;

    bool operator<(const ClickHouseVersion & other) const { return components < other.components; }
    bool operator>=(const ClickHouseVersion & other) const { return components >= other.components; }

private:
    std::vector<size_t> components;
};

namespace SettingsChangesHistory
{
    struct SettingChange
    {
        String name;
        Field previous_value;
        Field new_value;
        String reason;
    };

    using SettingsChanges = std::vector<SettingChange>;
}

using VersionToSettingsChangesMap = std::map<ClickHouseVersion, SettingsChangesHistory::SettingsChanges>;

const VersionToSettingsChangesMap & getSettingsChangesHistory();
const VersionToSettingsChangesMap & getMergeTreeSettingsChangesHistory();

}
