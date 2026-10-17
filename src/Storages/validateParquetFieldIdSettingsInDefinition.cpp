#include <Storages/validateParquetFieldIdSettingsInDefinition.h>

#include "config.h"

#if USE_PARQUET

#include <Common/Exception.h>
#include <Core/NamesAndTypes.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#include <boost/algorithm/string/predicate.hpp>

#include <algorithm>

#endif

namespace DB
{

#if USE_PARQUET
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Replaying a definition that was already accepted once must not be rejected, or an existing
/// table would fail to load; for such tables the write-time checks still apply.
bool isFreshDefinition(const StorageFactory::Arguments & args, bool validate_secondary_create)
{
    return args.mode == LoadingStrictnessLevel::CREATE
        || (validate_secondary_create && args.mode == LoadingStrictnessLevel::SECONDARY_CREATE)
        || (args.mode == LoadingStrictnessLevel::ATTACH && !args.query.attach_short_syntax);
}

/// The message-queue engines take their format settings from the `SETTINGS` clause and from a named
/// collection alike, so both count as supplied by the definition.
bool messageQueueDefinitionSuppliesFieldIdSettings(const SettingsChanges & definition_format_changes)
{
    return std::ranges::any_of(
        definition_format_changes,
        [](const auto & change)
        {
            return change.name == "output_format_parquet_column_field_ids"
                || change.name == "output_format_parquet_auto_assign_field_ids";
        });
}

bool freshDefinitionSuppliesFieldIdSettingsImpl(
    const StorageFactory::Arguments & args,
    const FormatSettings & format_settings,
    bool validate_secondary_create)
{
    if (!args.storage_def || !args.storage_def->settings)
        return false;

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
        return false;

    return isFreshDefinition(args, validate_secondary_create);
}

void validateParquetFieldIdSettingsInDefinitionImpl(
    const StorageFactory::Arguments & args,
    const String & format_name,
    const NamesAndTypesList & physical_columns,
    const FormatSettings & format_settings,
    bool validate_secondary_create)
{
    if (!freshDefinitionSuppliesFieldIdSettingsImpl(args, format_settings, validate_secondary_create))
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
    const StorageFactory::Arguments & args,
    const String & format_name,
    const FormatSettings & format_settings,
    bool definition_columns_match_writer_header)
{
#if USE_PARQUET
    validateParquetFieldIdSettingsInDefinitionImpl(
        args,
        format_name,
        definition_columns_match_writer_header ? args.columns.getAllPhysical() : NamesAndTypesList{},
        format_settings,
        false);
#else
    (void)args;
    (void)format_name;
    (void)format_settings;
    (void)definition_columns_match_writer_header;
#endif
}

void validateParquetFieldIdSettingsInMessageQueueDefinition(
    const StorageFactory::Arguments & args,
    const String & format_name,
    const SettingsChanges & definition_format_changes,
    const NamesAndTypesList & writer_header_columns)
{
#if USE_PARQUET
    if (!boost::iequals(format_name, "Parquet"))
        return;

    /// Not `validateParquetFieldIdSettingsInDefinitionImpl`: it looks only at the `SETTINGS` clause,
    /// while here the settings may come from a named collection as well.
    if (!messageQueueDefinitionSuppliesFieldIdSettings(definition_format_changes) || !isFreshDefinition(args, false))
        return;

    const auto format_settings = getFormatSettingsForTableDefinition(args.getLocalContext(), &definition_format_changes);
    validateParquetColumnFieldIds(
        writer_header_columns,
        format_settings.parquet.column_field_ids,
        format_settings.parquet.auto_assign_field_ids,
        format_settings.parquet.write_geometadata);
#else
    (void)args;
    (void)format_name;
    (void)definition_format_changes;
    (void)writer_header_columns;
#endif
}

void rejectParquetFieldIdSettingsInDefinition(
    const StorageFactory::Arguments & args,
    const String & engine_name,
    const SettingsChanges & definition_format_changes)
{
#if USE_PARQUET
    if (!messageQueueDefinitionSuppliesFieldIdSettings(definition_format_changes) || !isFreshDefinition(args, false))
        return;

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "output_format_parquet_column_field_ids / output_format_parquet_auto_assign_field_ids cannot be set in a {} table "
        "definition, because the engine does not apply the format settings of its definition on INSERT. "
        "Set them in the INSERT query instead",
        engine_name);
#else
    (void)args;
    (void)engine_name;
    (void)definition_format_changes;
#endif
}

bool freshDefinitionSuppliesParquetFieldIdSettings(
    const StorageFactory::Arguments & args,
    const FormatSettings & format_settings,
    bool validate_secondary_create)
{
#if USE_PARQUET
    return freshDefinitionSuppliesFieldIdSettingsImpl(args, format_settings, validate_secondary_create);
#else
    (void)args;
    (void)format_settings;
    (void)validate_secondary_create;
    return false;
#endif
}

void validateParquetFieldIdSettingsWithResolvedHeader(
    const StorageFactory::Arguments & args,
    const String & resolved_format_name,
    const NamesAndTypesList & writer_header_columns,
    const FormatSettings & format_settings,
    bool validate_secondary_create)
{
#if USE_PARQUET
    validateParquetFieldIdSettingsInDefinitionImpl(
        args, resolved_format_name, writer_header_columns, format_settings, validate_secondary_create);
#else
    (void)args;
    (void)resolved_format_name;
    (void)writer_header_columns;
    (void)format_settings;
    (void)validate_secondary_create;
#endif
}

}
