#pragma once

#include <DataTypes/IDataType.h>

#include <Parsers/IAST_fwd.h>

#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreePartition.h>

#include <Core/Types.h>

namespace DB
{

class IMergeTreeDataPart;

struct RowExistsColumn
{
    static const String name;
    static const DataTypePtr type;
};

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

/// Some virtual columns (_sample_factor, _table, _database) get their value from the
/// query plan (ReadFromMergeTree fills them into shared_virtual_fields) rather than from
/// the part reader. They cannot be materialized when reading a single part outside of a
/// SELECT plan, so referencing them in a mutation predicate/expression must be rejected at
/// analysis time instead of failing during MergeTreeSequentialSource execution.
bool isQueryPlanOnlyVirtualColumn(const String & column_name);

}
