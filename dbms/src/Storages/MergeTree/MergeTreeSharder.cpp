#include <Storages/MergeTree/MergeTreeSharder.h>
#include <Storages/MergeTree/ReshardingJob.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/HashingWriteBuffer.h>
#include <Interpreters/createBlockSelector.h>
#include <Interpreters/ExpressionAnalyzer.h>

#include <ctime>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}

MergeTreeSharder::MergeTreeSharder(MergeTreeData & data_, const ReshardingJob & job_)
    : data(data_), job(job_), log(&Logger::get(data.getLogName() + " (Sharder)")),
    sharding_key_expr(ExpressionAnalyzer(job.sharding_key_expr, data.context, nullptr, data.getColumnsList()).getActions(false)),
    sharding_key_column_name(job.sharding_key_expr->getColumnName())
{
    for (size_t shard_no = 0; shard_no < job.paths.size(); ++shard_no)
    {
        const WeightedZooKeeperPath & weighted_path = job.paths[shard_no];
        slots.insert(slots.end(), weighted_path.second, shard_no);
    }
}

BlocksWithShardNum MergeTreeSharder::shardBlock(const Block & block)
{
    BlocksWithShardNum res;

    const auto num_cols = block.columns();

    /// cache column pointers for later reuse
    std::vector<const IColumn *> columns(num_cols);
    for (size_t i = 0; i < columns.size(); ++i)
        columns[i] = block.safeGetByPosition(i).column.get();

    auto selector = createSelector(block);

    /// Split block to num_shard smaller block, using 'selector'.

    const auto num_shards = job.paths.size();
    Blocks splitted_blocks(num_shards);

    for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
        splitted_blocks[shard_idx] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        Columns splitted_columns = block.getByPosition(col_idx_in_block).column->scatter(num_shards, selector);
        for (size_t shard_idx = 0; shard_idx < num_shards; ++shard_idx)
            splitted_blocks[shard_idx].getByPosition(col_idx_in_block).column = std::move(splitted_columns[shard_idx]);
    }

    for (size_t shard_no = 0; shard_no < num_shards; ++shard_no)
    {
        if (splitted_blocks[shard_no].rows())
            res.emplace_back(std::move(splitted_blocks[shard_no]), shard_no);
    }

    return res;
}


IColumn::Selector MergeTreeSharder::createSelector(Block block)
{
    sharding_key_expr->execute(block);
    const auto & key_column = block.getByName(sharding_key_column_name);
    size_t num_shards = job.paths.size();

#define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(key_column.type.get())) \
        return createBlockSelector<TYPE>(*key_column.column, num_shards, slots);

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
