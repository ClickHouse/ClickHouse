#pragma once

#include <DataTypes/IDataType.h>

#include <Parsers/IAST_fwd.h>

#include <Storages/KeyDescription.h>
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

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part_or_projection);

/// True for the virtual columns whose value comes from the query plan (`ReadFromMergeTree` fills
/// them into `shared_virtual_fields`) rather than from the part reader, so they cannot be
/// materialized while reading a single part outside a `SELECT` plan.
bool isQueryPlanOnlyVirtualColumn(const String & column_name);

}
