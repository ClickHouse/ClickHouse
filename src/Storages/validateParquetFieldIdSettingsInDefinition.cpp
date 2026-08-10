#include <Storages/validateParquetFieldIdSettingsInDefinition.h>

#include "config.h"

#if USE_PARQUET

#include <Core/NamesAndTypes.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#include <boost/algorithm/string/predicate.hpp>

#include <algorithm>

#endif

namespace DB
{

#if USE_PARQUET
namespace
{

void validateParquetFieldIdSettingsInDefinitionImpl(
    const StorageFactory::Arguments & args,
    const String & format_name,
    const NamesAndTypesList & physical_columns,
    const FormatSettings & format_settings)
{
    if (!args.storage_def || !args.storage_def->settings)
        return;

    /// Only settings written in the definition itself express an intent to give this table its own
    /// `field_id`s, and only those reach the frozen `FormatSettings` in the first place — an ambient
    /// session or profile value is dropped by `getFormatSettingsForTableDefinition`.
    const auto is_set_in_definition = [&](std::string_view name)
    {
        return std::ranges::any_of(
            args.storage_def->settings->changes, [&](const auto & change) { return change.name == name; });
    };
    const bool column_field_ids_in_definition = is_set_in_definition("output_format_parquet_column_field_ids");
    const bool auto_assign_field_ids_in_definition = is_set_in_definition("output_format_parquet_auto_assign_field_ids");
    if (!(column_field_ids_in_definition && !format_settings.parquet.column_field_ids.empty())
        && !(auto_assign_field_ids_in_definition && format_settings.parquet.auto_assign_field_ids))
        return;

    /// Replaying a definition that was already accepted once must not be rejected, or an existing
    /// table would fail to load; for such tables the write-time checks still apply.
    const bool fresh_user_definition = args.mode == LoadingStrictnessLevel::CREATE
        || (args.mode == LoadingStrictnessLevel::ATTACH && !args.query.attach_short_syntax);
    if (!fresh_user_definition)
        return;

    /// The settings only affect Parquet output. A format that is still `auto` here is validated by
    /// the second phase once the engine has resolved it.
    if (!boost::iequals(format_name, "Parquet"))
        return;

    validateParquetColumnFieldIds(
        physical_columns,
        format_settings.parquet.column_field_ids,
        format_settings.parquet.auto_assign_field_ids,
        format_settings.parquet.write_geometadata);
}

}
#endif

void validateParquetFieldIdSettingsInDefinition(
    const StorageFactory::Arguments & args, const String & format_name, const FormatSettings & format_settings)
{
#if USE_PARQUET
    validateParquetFieldIdSettingsInDefinitionImpl(args, format_name, args.columns.getAllPhysical(), format_settings);
#else
    (void)args;
    (void)format_name;
    (void)format_settings;
#endif
}

void validateParquetFieldIdSettingsAfterSchemaInference(
    const StorageFactory::Arguments & args,
    const String & resolved_format_name,
    const NamesAndTypesList & resolved_columns,
    const FormatSettings & format_settings)
{
#if USE_PARQUET
    validateParquetFieldIdSettingsInDefinitionImpl(args, resolved_format_name, resolved_columns, format_settings);
#else
    (void)args;
    (void)resolved_format_name;
    (void)resolved_columns;
    (void)format_settings;
#endif
}

}
