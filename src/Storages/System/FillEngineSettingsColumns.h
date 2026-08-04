#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/SettingsTierType.h>

namespace DB
{

/// Helper template that fills the system.engine_settings columns from a default-constructed BaseSettings instance.
/// The columns (excluding engine_name, which the caller prepends) are:
///   name, value, default, changed, description, min, max, disallowed_values, readonly, type, is_obsolete, tier
template <typename SettingsImplType>
void fillEngineSettingsColumnsFromImpl(MutableColumns & columns)
{
    SettingsImplType impl;
    for (const auto & setting : impl.all())
    {
        size_t col = 0;
        columns[col++]->insert(setting.getName());
        columns[col++]->insert(setting.getValueString());
        columns[col++]->insert(setting.getDefaultValueString());
        columns[col++]->insert(setting.isValueChanged());
        columns[col++]->insert(setting.getDescription());
        columns[col++]->insertDefault(); // min (NULL)
        columns[col++]->insertDefault(); // max (NULL)
        columns[col++]->insert(Array{}); // disallowed_values
        columns[col++]->insert(UInt64(0)); // readonly
        columns[col++]->insert(setting.getTypeName());
        columns[col++]->insert(setting.getTier() == SettingsTierType::OBSOLETE);
        columns[col++]->insert(setting.getTier());
    }
}

}
