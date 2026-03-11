#pragma once

#include <Common/SettingsChanges.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{
class ASTCreateQuery;
struct TimeSeriesSettings;

/// Normalizes a TimeSeries table definition.
/// Adds missing columns to the definition and reorders all the columns in the canonical way.
/// Computes and stores INNER COLUMNS for each inner target table.
/// Also adds engines of inner tables to the definition if they aren't specified yet.
/// Returns true if the query was modified.
bool normalizeTimeSeriesDefinition(
    ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup);

/// Computes and returns fully normalized TimeSeries settings for a table being created.
/// Loads settings from the `AS <other_table>` source (if any) and then from `create_query`,
/// fills in missing types and generators from columns or external target tables,
/// applies defaults, and validates the result.
TimeSeriesSettings getNormalizedTimeSeriesSettings(
    const ASTCreateQuery & create_query, const ContextPtr & context, const SettingsChanges & settings_changes = {});

/// Generates the canonical column list for the TimeSeries table from the given normalized settings.
ColumnsDescription generateTimeSeriesColumns(const TimeSeriesSettings & normalized_settings);

}
