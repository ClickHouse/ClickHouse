#pragma once

#include <Core/Field.h>

#include <vector>

namespace DB
{

struct SettingChange
{
    String name;
    Field value;

    SettingChange() = default;
    SettingChange(std::string_view name_, const Field & value_) : name(name_), value(value_) {}
    SettingChange(std::string_view name_, Field && value_) : name(name_), value(std::move(value_)) {}

    friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs) { return (lhs.name == rhs.name) && (lhs.value == rhs.value); }
    friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }
};


class SettingsChanges : public std::vector<SettingChange>
{
public:
    using std::vector<SettingChange>::vector;

    bool tryGet(std::string_view name, Field & out_value) const;
    const Field * tryGet(std::string_view name) const;
    Field * tryGet(std::string_view name);

    /// Inserts element if doesn't exists and returns true, otherwise just returns false
    bool insertSetting(std::string_view name, const Field & value);
    /// Sets element to value, inserts if doesn't exist
    void setSetting(std::string_view name, const Field & value);
    /// If element exists - removes it and returns true, otherwise returns false
    bool removeSetting(std::string_view name);
};

}
