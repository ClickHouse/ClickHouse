#pragma once

#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
class ASTCreateQuery;

/// Normalizes a TimeSeries table definition.
/// Adds missing columns to the definition and reorders all the columns in the canonical way.
/// Computes and stores INNER COLUMNS for each inner target table.
/// Also adds engines of inner tables to the definition if they aren't specified yet.
/// This function reads the other tables and the query settings which the normalization needs
/// and then calls `normalizeTimeSeriesDefinitionImpl` (see normalizeTimeSeriesDefinitionImpl.h).
void normalizeTimeSeriesDefinition(
    ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup);

}
