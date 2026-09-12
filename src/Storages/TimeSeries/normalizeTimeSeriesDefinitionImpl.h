#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/ColumnsDescription.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <map>


namespace DB
{
class ASTCreateQuery;
struct Settings;

/// The parameters of the normalization of a TimeSeries table definition (see `normalizeTimeSeriesDefinitionImpl`).
/// The information from outside the definition is needed only for a new table; the normalization of an existing table
/// (on ATTACH) doesn't use it, so the corresponding fields can be left empty then.
struct NormalizeTimeSeriesDefinitionParams
{
    /// How the table is loaded: `CREATE` or `SECONDARY_CREATE` for a new table, `ATTACH` or stricter for an existing one.
    LoadingStrictnessLevel mode = LoadingStrictnessLevel::CREATE;

    /// Whether the table is restored from a backup: such a table is an existing one whatever `mode` is.
    bool is_restore_from_backup = false;

    /// Whether the query creates a new table. It's false on ATTACH and when restoring from a backup.
    bool isNewTable() const { return (mode <= LoadingStrictnessLevel::SECONDARY_CREATE) && !is_restore_from_backup; }

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

/// Normalizes a TimeSeries table definition using only the definition and `params`, without access to the database catalog.
/// Adds missing columns to the definition and reorders all the columns in the canonical way.
/// Computes and stores INNER COLUMNS for each inner target table.
/// Also adds engines of inner tables to the definition if they aren't specified yet.
/// `normalizeTimeSeriesDefinition` (see normalizeTimeSeriesDefinition.h) collects `params` from the database catalog
/// and the query context, then calls this function.
void normalizeTimeSeriesDefinitionImpl(ASTCreateQuery & create_query, const NormalizeTimeSeriesDefinitionParams & params);

}
