#pragma once

#include <Storages/TableSetting.h>

namespace DB
{

/// Reads every setting of a `BaseSettings` instance into the common form both settings tables use.
///
/// The instance decides what is reported: a default-constructed one describes an engine, the one a
/// storage holds describes a table. `origin` is only as precise as the instance allows - a setting
/// that differs from its default is `Other` here, because this cannot tell a config section from a
/// named collection. A storage refines it, since only the storage knows where its values came from.
template <typename SettingsImplType>
TableSettings enumerateSettingsFromImpl(const SettingsImplType & impl)
{
    TableSettings result;
    for (const auto & setting : impl.all())
    {
        TableSetting described;
        described.name = setting.getName();
        described.value = setting.getValueString();
        described.default_value = setting.getDefaultValueString();
        described.type = setting.getTypeName();
        described.description = setting.getDescription();
        described.tier = setting.getTier();
        described.origin = setting.isValueChanged() ? TableSettingOrigin::Other : TableSettingOrigin::Default;
        result.push_back(std::move(described));
    }
    return result;
}

}
