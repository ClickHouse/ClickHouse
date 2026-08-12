#pragma once

#include <DataTypes/IDataType.h>

#include <Parsers/IAST_fwd.h>

#include <Storages/KeyDescription.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Storages/MergeTree/MergeTreePartition.h>

#include <Core/Types.h>

namespace DB
{

class IMergeTreeDataPart;
class ASTAssignment;

struct RowExistsColumn
{
    static const String name;
    static const DataTypePtr type;
};

/// True only for the lightweight-delete marker assignment `_row_exists = 0` (what `DELETE FROM`
/// rewrites to). An arbitrary `_row_exists = <expr>` modifies the deletion mask and is a real update,
/// so it returns false. Used to govern `_row_exists = 0` by ALTER DELETE while keeping ALTER UPDATE
/// for any other assignment to the column.
bool isLightweightDeleteAssignment(const ASTAssignment & assignment);

struct BlockNumberColumn
{
    static const String name;
    static const DataTypePtr type;
    static const ASTPtr codec;
};

struct BlockOffsetColumn
{
    static const String name;
    static const DataTypePtr type;
    static const ASTPtr codec;
};

struct PartDataVersionColumn
{
    static const String name;
    static const DataTypePtr type;
};

struct PartitionIdColumn
{
    static const String name;
    static const DataTypePtr type;
};

struct PartitionValueColumn
{
    static const String name;
    static DataTypePtr type(const KeyDescription * partition_key);
};

/// Whether a column is a virtual column physically stored inside data parts, rather than computed
/// on the fly. Not managed by the column ID mapping, so column remapping passes it through.
/// Keep in sync with the `addPersistent` calls in `getMergeTreeVirtuals`.
inline bool isPersistentVirtualColumn(const String & column_name)
{
    return column_name == RowExistsColumn::name
        || column_name == BlockNumberColumn::name
        || column_name == BlockOffsetColumn::name;
}

/// The one registry of the virtual columns a MergeTree table exposes, with their types and
/// materialization places; `_partition_value` only when a partition key is present. Both
/// `MergeTreeData::createVirtuals` and `isVirtualColumn` derive from it, so they cannot drift.
VirtualColumnsDescription getMergeTreeVirtuals(const KeyDescription * partition_key);

/// Whether a column is any MergeTree virtual column (ephemeral or persistent) — the full set
/// registered in `getMergeTreeVirtuals`. Virtual columns are not managed by the column ID mapping.
bool isVirtualColumn(const String & column_name);

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part_or_projection);

}
