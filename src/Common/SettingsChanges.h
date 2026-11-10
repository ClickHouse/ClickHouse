#pragma once

#include <Core/Field.h>

#include <vector>

namespace DB
{

struct SettingChange
{
    String name;
    Field value;

    /// Set when the change came from the shorthand form `SET name` with no value, which stands for
    /// `SET name = true` and is only meaningful for a Bool setting. The parser cannot tell whether
    /// the setting is Bool - it does not know the settings schema - so it records how the change was
    /// written and `BaseSettings::applyChange` rejects the shorthand for every other type.
    bool shorthand = false;

    SettingChange() = default;
    SettingChange(std::string_view name_, const Field & value_) : name(name_), value(value_) {}
    SettingChange(std::string_view name_, Field && value_) : name(name_), value(std::move(value_)) {}

    /// `shorthand` is part of the identity of a change: it is what makes the change rejected for a
    /// setting that is not Bool, so a shorthand change is not the same thing as an explicit `= true`.
    friend bool operator ==(const SettingChange & lhs, const SettingChange & rhs) { return (lhs.name == rhs.name) && (lhs.value == rhs.value) && (lhs.shorthand == rhs.shorthand); }
    friend bool operator !=(const SettingChange & lhs, const SettingChange & rhs) { return !(lhs == rhs); }
};


class SettingsChanges : public std::vector<SettingChange>
{
public:
    using std::vector<SettingChange>::vector;

    bool tryGet(std::string_view name, Field & out_value) const;
    const Field * tryGet(std::string_view name) const;
    Field * tryGet(std::string_view name);

    /// The whole change rather than its value, for consumers that also have to look at `shorthand`.
    const SettingChange * tryGetChange(std::string_view name) const;
    SettingChange * tryGetChange(std::string_view name);

    /// Inserts element if doesn't exists and returns true, otherwise just returns false
    bool insertSetting(std::string_view name, const Field & value);
    /// Sets element to value, inserts if doesn't exist
    void setSetting(std::string_view name, const Field & value);
    /// If element exists - removes it and returns true, otherwise returns false
    bool removeSetting(std::string_view name);

    String namesToString() const;
};

}
