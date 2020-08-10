#pragma once

#include <Core/Field.h>


namespace DB
{

struct SettingChange
{
    String name;
    Field value;
    SettingChange() {}

    SettingChange(const String & name_, const Field value_)
    : name(name_)
    , value(value_) {}

    friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs) { return (lhs.name == rhs.name) && (lhs.value == rhs.value); }
    friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }
};

using SettingsChanges = std::vector<SettingChange>;

}
