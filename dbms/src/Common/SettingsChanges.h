#pragma once

#include <Core/Field.h>


namespace DB
{

struct SettingChange
{
    String name;
    Field value;

    friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs) { return (lhs.name == rhs.name) && (lhs.value == rhs.value); }
    friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }
};

using SettingsChanges = std::vector<SettingChange>;

}
