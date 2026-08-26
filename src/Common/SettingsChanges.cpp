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

bool SettingsChanges::insertSetting(std::string_view name, const Field & value)
{
    auto it = std::find_if(begin(), end(), [&name](const SettingChange & change) { return change.name == name; });
    if (it != end())
        return false;
    emplace_back(name, value);
    return true;
}

void SettingsChanges::setSetting(std::string_view name, const Field & value)
{
    if (auto * setting_value = tryGet(name))
        *setting_value = value;
    else
        insertSetting(name, value);
}

bool SettingsChanges::removeSetting(std::string_view name)
{
    auto it = std::find_if(begin(), end(), [&name](const SettingChange & change) { return change.name == name; });
    if (it == end())
        return false;
    erase(it);
    return true;
}

}
