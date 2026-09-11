#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/ColumnsDescription.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <map>


namespace DB
{
class ASTCreateQuery;
struct Settings;

/// Normalizes a TimeSeries table definition.
/// Adds missing columns to the definition and reorders all the columns in the canonical way.
/// Computes and stores INNER COLUMNS for each inner target table.
/// Also adds engines of inner tables to the definition if they aren't specified yet.
/// This function reads the other tables and the query settings which the normalization needs (see the struct below)
/// and then calls `normalizeTimeSeriesDefinitionImpl`.
void normalizeTimeSeriesDefinition(
    ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup);

/// The information from outside the definition which the normalization of a new table needs.
/// The normalization of an existing table (on ATTACH) doesn't need it, so all the fields can be left empty then.
struct NormalizeTimeSeriesDefinitionInputs
{
    /// The CREATE query of the table from the clause `AS <other_table>`, as it is stored (not normalized).
    /// Required for a new table with that clause.
    boost::intrusive_ptr<const ASTCreateQuery> as_create_query;

    /// The columns of every external target table of the query, by the kind of the target.
    /// Required for a new table.
    std::map<ViewTarget::Kind, ColumnsDescription> external_target_columns;

    /// The query-level settings (the `default_table_engine` setting chooses the engines of the inner tables).
    /// Required for a new table.
    const Settings * query_settings = nullptr;
};

/// Does the normalization (see `normalizeTimeSeriesDefinition`) using only the definition and `inputs`,
/// without access to the database catalog.
void normalizeTimeSeriesDefinitionImpl(
    ASTCreateQuery & create_query, LoadingStrictnessLevel mode, bool is_restore_from_backup, const NormalizeTimeSeriesDefinitionInputs & inputs);

}
