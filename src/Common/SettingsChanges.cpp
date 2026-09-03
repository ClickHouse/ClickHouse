#include <Common/SettingsChanges.h>

namespace DB
{
namespace
{
    SettingChange * find(SettingsChanges & changes, std::string_view name)
    {
        auto it = std::find_if(changes.begin(), changes.end(), [&name](const SettingChange & change) { return change.name == name; });
        if (it == changes.end())
            return nullptr;
        return &*it;
    }

    const SettingChange * find(const SettingsChanges & changes, std::string_view name)
    {
        auto it = std::find_if(changes.begin(), changes.end(), [&name](const SettingChange & change) { return change.name == name; });
        if (it == changes.end())
            return nullptr;
        return &*it;
    }
}

bool SettingsChanges::tryGet(std::string_view name, Field & out_value) const
{
    const auto * change = find(*this, name);
    if (!change)
        return false;
    out_value = change->value;
    return true;
}

const Field * SettingsChanges::tryGet(std::string_view name) const
{
    const auto * change = find(*this, name);
    if (!change)
        return nullptr;
    return &change->value;
}

Field * SettingsChanges::tryGet(std::string_view name)
{
    auto * change = find(*this, name);
    if (!change)
        return nullptr;
    return &change->value;
}

const SettingChange * SettingsChanges::tryGetChange(std::string_view name) const
{
    return find(*this, name);
}

SettingChange * SettingsChanges::tryGetChange(std::string_view name)
{
    return find(*this, name);
}

bool SettingsChanges::insertSetting(std::string_view name, const Field & value)
{
    auto it = std::find_if(begin(), end(), [&name](const SettingChange & change) { return change.name == name; });
    if (it != end())
        return false;
    emplace_back(name, value);
    return true;
}

bool SettingsChanges::removeSetting(std::string_view name)
{
    auto it = std::find_if(begin(), end(), [&name](const SettingChange & change) { return change.name == name; });
    if (it == end())
        return false;
    erase(it);
    return true;
}

void SettingsChanges::setSetting(std::string_view name, const Field & value)
{
    if (auto * setting_value = tryGet(name))
        *setting_value = value;
    else
        insertSetting(name, value);
}

void SettingsChanges::setSetting(const SettingChange & change)
{
    if (auto * existing_change = tryGetChange(change.name))
        *existing_change = change;
    else
        push_back(change);
}

void SettingsChanges::setSettings(const SettingsChanges & other)
{
    for (const auto & change : other)
        setSetting(change);
}

String SettingsChanges::namesToString() const
{
    String result;
    for (const auto & change : *this)
    {
        if (!result.empty())
            result += ", ";
        result += change.name;
    }
    return result;
}

}
