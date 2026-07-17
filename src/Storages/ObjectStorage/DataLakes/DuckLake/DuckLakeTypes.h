#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>

#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{
namespace DuckLake
{

/// One row of the ducklake_column catalog table.
struct ColumnInfo
{
    Int64 column_id;
    std::optional<Int64> parent_column;
    Int64 column_order;
    String name;
    String type;
    bool nulls_allowed;
    Int64 begin_snapshot;
    std::optional<Int64> end_snapshot;

    bool isVisibleAt(Int64 snapshot_id) const
    {
        return begin_snapshot <= snapshot_id && (!end_snapshot.has_value() || *end_snapshot > snapshot_id);
    }
};

/// A node of the reconstructed column tree. Children are sorted by column_order.
struct ColumnNode
{
    ColumnInfo info;
    std::vector<ColumnNode> children;
};

/// Parse one entry of DuckLake's scalar type vocabulary (see ducklake_types.cpp in the DuckLake
/// repository): boolean, int8/16/32/64/128, uint8/16/32/64/128, float32/64, decimal(w,s), time,
/// time_ns, date, timestamp, timestamp_us/ms/ns/s, timestamptz, timestamptz_ns, varchar, blob,
/// uuid, json. Nested type tokens (struct/list/map) and unsupported types throw.
DataTypePtr parseScalarType(const String & type_text);

/// True if the type text is one of the nested type tokens (struct/list/map).
bool isNestedType(const String & type_text);

/// Reconstruct the forest of column trees from flat ducklake_column rows (all belonging to one
/// table), keeping only nodes visible at `snapshot_id`. Roots and children are sorted by
/// column_order. Throws BAD_ARGUMENTS on dangling parent references.
std::vector<ColumnNode> buildColumnTree(const std::vector<ColumnInfo> & rows, Int64 snapshot_id);

/// Build the ClickHouse type of one column node, recursing for struct/list/map and honoring
/// nulls_allowed at every level.
DataTypePtr getColumnType(const ColumnNode & node);

/// Top-level table schema from the visible column forest.
NamesAndTypesList getTableSchema(const std::vector<ColumnNode> & roots);

/// Build the ColumnMapper encoding (clickhouse_dotted_name -> column_id) for a table.
/// Follows the same contract as Iceberg's traverseSchema: a struct contributes itself and all
/// its elements ({s, s.x, s.y}), a list element is `<name>.element`, map children are
/// `<name>.key` / `<name>.value`.
///
/// The map covers every column_id ever assigned to the table, not just the currently visible
/// ones: old data files still carry field ids of dropped columns, and the Parquet reader throws
/// on any unknown field id below the reserved range (SchemaConverter.cpp). Inactive columns are
/// mapped to a synthetic name that is never requested.
std::unordered_map<String, Int64> buildFieldIdMap(const std::vector<ColumnInfo> & all_rows, Int64 snapshot_id);

}
}
