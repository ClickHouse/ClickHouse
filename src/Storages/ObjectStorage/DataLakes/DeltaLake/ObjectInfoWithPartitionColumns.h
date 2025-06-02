#pragma once
#include "config.h"

#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include "ExpressionVisitor.h"

namespace DB
{

/**
 * A class which adds partition values info to the DB::ObjectInfo.
 * As DeltaLake does not store partition columns values in the actual data files,
 * but instead in the data files directory names,
 * we need a way to pass the value through to the StorageObjectStorageSource.
 */
struct ObjectInfoWithPartitionColumns : public DB::ObjectInfo
{
#if USE_DELTA_KERNEL_RS
    template <typename... Args>
    explicit ObjectInfoWithPartitionColumns(
        std::unique_ptr<DeltaLake::ParsedExpression> expression_,
        const Names & partition_columns_,
        Args &&... args)
        : DB::ObjectInfo(std::forward<Args>(args)...)
        , expression(std::move(expression_))
        , partition_columns(partition_columns_)
    {
    }

    std::vector<DB::Field> getPartitionValues() const { return expression->getConstValues(partition_columns); }

    void apply(DB::Chunk & chunk, const DB::NamesAndTypesList & chunk_schema, const DB::Names & columns) const
    {
        if (expression)
            expression->apply(chunk, chunk_schema, columns);
    }

    const std::unique_ptr<DeltaLake::ParsedExpression> expression;
    const Names partition_columns;
#else
    void apply(DB::Chunk &, const DB::NamesAndTypesList &, const DB::Names &) const {}
#endif
};

}
