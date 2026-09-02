#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/SettingsTierType.h>
#include <Storages/System/MutableColumnsAndConstraints.h>

namespace DB
{

/// Fills the `system.engine_settings` columns from a `BaseSettings` instance. The columns
/// (excluding `engine_name`, which the caller prepends) are the same as `system.merge_tree_settings`:
///   name, value, default, changed, description, min, max, disallowed_values, readonly,
///   type, is_obsolete, tier
///
/// `min`, `max`, `disallowed_values` and `readonly` come from the user's settings constraints, which
/// exist only for `MergeTreeSettings` (see `SettingsConstraints::get`). For every other engine no
/// constraint can be declared, so "no bound" and "not read-only" are the correct answers rather than
/// missing ones.
template <typename SettingsImplType>
void fillEngineSettingsColumnsFromImpl(MutableColumnsAndConstraints & params, const SettingsImplType & impl)
{
    MutableColumns & columns = params.res_columns;
    for (const auto & setting : impl.all())
    {
        size_t col = 0;
        columns[col++]->insert(setting.getName());
        columns[col++]->insert(setting.getValueString());
        columns[col++]->insert(setting.getDefaultValueString());
        columns[col++]->insert(setting.isValueChanged());
        columns[col++]->insert(setting.getDescription());
        columns[col++]->insertDefault(); // min
        columns[col++]->insertDefault(); // max
        columns[col++]->insert(Array{}); // disallowed_values
        columns[col++]->insert(UInt64(0)); // readonly
        columns[col++]->insert(setting.getTypeName());
        columns[col++]->insert(setting.getTier() == SettingsTierType::OBSOLETE);
        columns[col++]->insert(setting.getTier());
    }
}

/// For engines that have no server-level instance: the compiled defaults are what the engine uses,
/// so `value` equals `default` and nothing is `changed`.
template <typename SettingsImplType>
void fillEngineSettingsColumnsFromImpl(MutableColumnsAndConstraints & params)
{
    SettingsImplType impl;
    fillEngineSettingsColumnsFromImpl(params, impl);
}

}
