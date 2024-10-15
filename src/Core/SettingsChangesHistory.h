#pragma once

#include <Core/Field.h>
#include <map>
#include <vector>


namespace DB
{

class ClickHouseVersion
{
public:
    /// NOLINTBEGIN(google-explicit-constructor)
    ClickHouseVersion(const String & version);
    ClickHouseVersion(const char * version);
    /// NOLINTEND(google-explicit-constructor)

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

const std::map<ClickHouseVersion, SettingsChangesHistory::SettingsChanges> & getSettingsChangesHistory();
const std::map<ClickHouseVersion, SettingsChangesHistory::SettingsChanges> & getMergeTreeSettingsChangesHistory();

}
