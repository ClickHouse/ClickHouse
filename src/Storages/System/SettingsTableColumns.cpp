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
            "1 if a settings constraint makes the setting read-only, 0 if none does. Only `MergeTree` settings can be "
            "constrained. This says nothing about whether the engine accepts `ALTER TABLE ... MODIFY SETTING`, nor "
            "about the user's `ALTER` privileges."},
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

void insertSharedSettingColumns(
    MutableColumns & res_columns,
    const std::vector<UInt8> & columns_mask,
    size_t & src_index,
    size_t & res_index,
    std::string_view name,
    std::string_view value,
    const SettingDescription & setting,
    std::string_view alias_for)
{
    auto wanted = [&] { return columns_mask.empty() || columns_mask[src_index]; };

    auto put = [&](const auto & column_value)
    {
        if (wanted())
            res_columns[res_index++]->insert(column_value);
        ++src_index;
    };

    put(name);
    put(value);
    put(setting.default_value);
    put(setting.origin != SettingOrigin::Default);
    put(setting.comment);
    put(setting.min_value ? Field(*setting.min_value) : Field());
    put(setting.max_value ? Field(*setting.max_value) : Field());

    /// Built only when the column is wanted, because it is per-setting work rather than a copy.
    if (wanted())
    {
        Array disallowed;
        disallowed.reserve(setting.disallowed_values.size());
        for (const auto & disallowed_value : setting.disallowed_values)
            disallowed.emplace_back(disallowed_value);
        res_columns[res_index++]->insert(disallowed);
    }
    ++src_index;

    put(setting.readonly);
    put(setting.type);
    put(setting.tier == SettingsTierType::OBSOLETE);
    put(setting.tier);
    put(alias_for);
}

}
