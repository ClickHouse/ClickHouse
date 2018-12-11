#include <Storages/StorageDistributed.h>

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTSelectQuery.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{

extern const int TYPE_MISMATCH;

}

namespace
{

/// the same as DistributedBlockOutputStream::createSelector, should it be static?
IColumn::Selector createSelector(const ClusterPtr cluster, const ColumnWithTypeAndName & result)
{
    const auto & slot_to_shard = cluster->getSlotToShard();

#define CREATE_FOR_TYPE(TYPE)                                   \
    if (typeid_cast<const DataType##TYPE *>(result.type.get())) \
        return createBlockSelector<TYPE>(*result.column, slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}

}

/// Returns a new cluster with fewer shards if constant folding for `sharding_key_expr` is possible
/// using constraints from "WHERE" condition, otherwise returns `nullptr`
ClusterPtr StorageDistributed::skipUnusedShards(ClusterPtr cluster, const SelectQueryInfo & query_info)
{
    const auto & select = typeid_cast<ASTSelectQuery &>(*query_info.query);

    if (!select.where_expression)
    {
        return nullptr;
    }

    const auto blocks = evaluateConstantExpressionAsBlock(select.where_expression, sharding_key_expr);

    // Can't get definite answer if we can skip any shards
    if (blocks.empty())
    {
        return nullptr;
    }

    std::set<int> shards;

    for (const auto & block : blocks)
    {
        if (!block.has(sharding_key_column_name))
            throw Exception("sharding_key_expr should evaluate as a single row", ErrorCodes::TYPE_MISMATCH);

        const auto result = block.getByName(sharding_key_column_name);
        const auto selector = createSelector(cluster, result);

        shards.insert(selector.begin(), selector.end());
    }

    return cluster->getClusterWithMultipleShards({shards.begin(), shards.end()});
}

}
