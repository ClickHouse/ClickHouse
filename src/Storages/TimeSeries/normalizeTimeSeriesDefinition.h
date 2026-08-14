#pragma once

#include <optional>

#include <Core/UUID.h>
#include <DataTypes/IDataType.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTCreateQuery;
class IColumn;
struct TimeSeriesSettings;

/// Normalizes a TimeSeries table definition.
/// Adds missing columns to the definition and reorders all the columns in the canonical way.
/// Computes and stores INNER COLUMNS for each inner target table.
/// Also adds engines of inner tables to the definition if they aren't specified yet.
void normalizeTimeSeriesDefinition(
    ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup);

/// The id type of the samples table when it isn't specified explicitly: Tuple(UInt64, UUID),
/// where the first component is a hash of the metric name and the second is a hash of all tags
/// (see TimeSeriesIDGenerator::getDefault). The id is the leading component of the samples
/// table sorting key (id, timestamp).
DataTypePtr standardTimeSeriesIDDataType();

/// Raw data of an identifier column of the standard id type (see standardTimeSeriesIDDataType).
struct StandardTimeSeriesIDColumns
{
    const UInt64 * first = nullptr;
    const UUID * second = nullptr;
};

/// Returns the raw columns if `column` stores identifiers of the standard id type.
std::optional<StandardTimeSeriesIDColumns> tryGetStandardTimeSeriesIDColumns(const IColumn & column);

}
