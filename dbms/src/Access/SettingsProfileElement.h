#pragma once

#include <Core/Field.h>
#include <Core/UUID.h>
#include <optional>
#include <vector>


namespace DB
{
struct Settings;
struct SettingChange;
using SettingsChanges = std::vector<SettingChange>;
class SettingsConstraints;


struct SettingsProfileElement
{
    std::optional<UUID> parent_profile;
    String name;
    Field value;
    Field min_value;
    Field max_value;
    std::optional<bool> readonly;

    auto toTuple() const { return std::tie(parent_profile, name, value, min_value, max_value, readonly); }
    friend bool operator==(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return lhs.toTuple() == rhs.toTuple(); }
    friend bool operator!=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(lhs == rhs); }
    friend bool operator <(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return lhs.toTuple() < rhs.toTuple(); }
    friend bool operator >(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return rhs < lhs; }
    friend bool operator <=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(rhs < lhs); }
    friend bool operator >=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(lhs < rhs); }
};


class SettingsProfileElements : public std::vector<SettingsProfileElement>
{
public:
    void merge(const SettingsProfileElements & other);

    Settings toSettings() const;
    SettingsChanges toSettingsChanges() const;
    SettingsConstraints toSettingsConstraints() const;
};

}
