#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
class ASTCreateQuery;

/// How a TimeSeries table definition is normalized, see `normalizeTimeSeriesDefinitionImpl`.
struct TimeSeriesDefinitionMode
{
    /// Whether the query creates a new table.
    bool is_new_table;

    /// Whether the definition may come from an older version, so it can be converted to the current form.
    bool can_convert;

    /// Whether the definitions of the inner target tables must be generated.
    bool generate_inner_tables;
};

/// Computes the mode of the normalization.
/// `generate_inner_tables` requests the generation of the inner target definitions for an existing table:
/// a stored definition without INNER COLUMNS and INNER ENGINE clauses (e.g. the inner table of a materialized
/// view, which stores only the `ENGINE = TimeSeries` clause) needs them generated even though it is an existing table.
inline TimeSeriesDefinitionMode getTimeSeriesDefinitionMode(
    LoadingStrictnessLevel mode, bool is_restore_from_backup, bool generate_inner_tables = false)
{
    const bool is_new_table = (mode <= LoadingStrictnessLevel::SECONDARY_CREATE) && !is_restore_from_backup;
    return {
        .is_new_table = is_new_table,
        /// The initial CREATE query is excluded: it must be written in the current form already.
        .can_convert = (mode != LoadingStrictnessLevel::CREATE) || is_restore_from_backup,
        .generate_inner_tables = is_new_table || generate_inner_tables,
    };
}

/// Normalizes a TimeSeries table definition.
/// Adds missing columns to the definition and reorders all the columns in the canonical way.
/// Computes and stores INNER COLUMNS for each inner target table.
/// Also adds engines of inner tables to the definition if they aren't specified yet.
/// This function reads the other tables and the query settings which the normalization needs
/// and then calls `normalizeTimeSeriesDefinitionImpl` (see normalizeTimeSeriesDefinitionImpl.h).
void normalizeTimeSeriesDefinition(
    ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup,
    bool generate_inner_tables = false);

}
