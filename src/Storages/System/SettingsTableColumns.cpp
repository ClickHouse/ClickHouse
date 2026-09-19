#include <Storages/System/SettingsTableColumns.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/SettingsTierType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

ColumnsDescription sharedSettingColumns()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Setting name."},
        {"value", std::make_shared<DataTypeString>(),
            "The value in effect. It need not be what a `CREATE` query states: it can come from the server configuration, "
            "the `compatibility` setting, a named collection or replicated metadata."},
        {"default", std::make_shared<DataTypeString>(), "Value the setting has when nothing sets it."},
        {"changed", std::make_shared<DataTypeUInt8>(),
            "1 if something other than the compiled default set this value. Not the same as `value` differing from "
            "`default`: assigning a setting the value it already had still counts."},
        {"description", std::make_shared<DataTypeString>(), "Setting description."},
        {"min", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "Minimum value the current user's settings constraints allow, or NULL if none is set. "
            "Only `MergeTree` settings can be constrained."},
        {"max", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
            "Maximum value the current user's settings constraints allow, or NULL if none is set. "
            "Only `MergeTree` settings can be constrained."},
        {"disallowed_values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Values the current user's settings constraints forbid, empty when none are. "
            "Only `MergeTree` settings can be constrained."},
        {"readonly", std::make_shared<DataTypeUInt8>(),
            "1 if the setting cannot be changed: a settings constraint of the current user makes it read-only, or the "
            "engine does not allow changing it on an existing table (for example `index_granularity`); 0 otherwise. "
            "Only `MergeTree` settings are ever read-only. It says nothing about the user's `ALTER` privileges."},
        {"type", std::make_shared<DataTypeString>(), "Setting type (implementation specific string value)."},
        {"is_obsolete", std::make_shared<DataTypeUInt8>(), "Shows whether a setting is obsolete."},
        {"tier", getSettingsTierEnum(), R"(
Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their
development and the expectations one might have when using them:
* PRODUCTION: The feature is stable, safe to use and does not have issues interacting with other PRODUCTION features.
* BETA: The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
* EXPERIMENTAL: The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
* PRIVATE PREVIEW: The feature is on a clear path to general availability. Its applicability is still limited and it is not recommended for production use.
* OBSOLETE: No longer supported. Either it is already removed or it will be removed in future releases.
)"},
        {"alias_for", std::make_shared<DataTypeString>(),
            "Empty on a setting's own row. A setting writable under more than one name also gets a row per other name, "
            "carrying the same values, with this naming the one it is declared under."},
    };
}

bool isSettingValueMasked(const SettingDescription & setting, bool show_secrets)
{
    return !show_secrets && !setting.masked_value.empty();
}

void writeSharedSettingColumns(
    SettingRowWriter & writer, std::string_view name, const SettingDescription & setting, bool is_masked, std::string_view alias_for)
{
    writer.put(name);
    writer.put(is_masked ? setting.masked_value : setting.value);
    writer.put(setting.default_value);
    writer.put(setting.origin != SettingOrigin::Default);
    writer.put(setting.comment);
    writer.put(setting.min_value);
    writer.put(setting.max_value);

    /// Built only when the column is wanted, because it is per-setting work rather than a copy.
    Array disallowed;
    if (writer.wants())
    {
        disallowed.reserve(setting.disallowed_values.size());
        for (const auto & disallowed_value : setting.disallowed_values)
            disallowed.emplace_back(disallowed_value);
    }
    writer.put(disallowed);

    writer.put(setting.readonly);
    writer.put(setting.type);
    writer.put(setting.tier == SettingsTierType::OBSOLETE);
    writer.put(setting.tier);
    writer.put(alias_for);
}

}
