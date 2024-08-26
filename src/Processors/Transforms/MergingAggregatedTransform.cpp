#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block MergingAggregatedTransform::appendGroupingIfNeeded(const Block & in_header, Block out_header)
{
    /// __grouping_set is neither GROUP BY key nor an aggregate function.
    /// It behaves like a GROUP BY key, but we cannot append it to keys
    /// because it changes hashing method and buckets for two level aggregation.
    /// Now, this column is processed "manually" by merging each group separately.
    if (in_header.has("__grouping_set"))
        out_header.insert(0, in_header.getByName("__grouping_set"));

    return out_header;
}

MergingAggregatedTransform::MergingAggregatedTransform(
    Block header_, AggregatingTransformParamsPtr params_, size_t max_threads_)
    : IAccumulatingTransform(header_, appendGroupingIfNeeded(header_, params_->getHeader()))
    , params(std::move(params_)), max_threads(max_threads_), has_grouping_sets(header_.has("__grouping_set"))
{
}

void MergingAggregatedTransform::addBlock(Block block)
{
    if (!has_grouping_sets)
    {
        auto & bucket_to_blocks = grouping_sets[0];
        bucket_to_blocks[block.info.bucket_num].emplace_back(std::move(block));
        return;
    }

    auto grouping_position = block.getPositionByName("__grouping_set");
    auto grouping_column = block.getByPosition(grouping_position).column;
    block.erase(grouping_position);

    /// Split a block by __grouping_set values.

    const auto * grouping_column_typed = typeid_cast<const ColumnUInt64 *>(grouping_column.get());
    if (!grouping_column_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected UInt64 column for __grouping_set, got {}", grouping_column->getName());

    /// Enumerate groups and fill the selector.
    std::map<UInt64, size_t> enumerated_groups;
    IColumn::Selector selector;

    const auto & grouping_data = grouping_column_typed->getData();
    size_t num_rows = grouping_data.size();
    UInt64 last_group = grouping_data[0];
    for (size_t row = 1; row < num_rows; ++row)
    {
        auto group = grouping_data[row];

        /// Optimization for equal ranges.
        if (last_group == group)
            continue;

        /// Optimization for single group.
        if (enumerated_groups.empty())
        {
            selector.reserve(num_rows);
            enumerated_groups.emplace(last_group, enumerated_groups.size());
        }

        /// Fill the last equal range.
        selector.resize_fill(row, enumerated_groups[last_group]);
        /// Enumerate new group if did not see it before.
        enumerated_groups.emplace(last_group, enumerated_groups.size());
    }

    /// Optimization for single group.
    if (enumerated_groups.empty())
    {
        auto & bucket_to_blocks = grouping_sets[last_group];
        bucket_to_blocks[block.info.bucket_num].emplace_back(std::move(block));
        return;
    }

    /// Fill the last equal range.
    selector.resize_fill(num_rows, enumerated_groups[last_group]);

    const size_t num_groups = enumerated_groups.size();
    Blocks splitted_blocks(num_groups);

    for (size_t group_id = 0; group_id < num_groups; ++group_id)
        splitted_blocks[group_id] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        MutableColumns splitted_columns = block.getByPosition(col_idx_in_block).column->scatter(num_groups, selector);
        for (size_t group_id = 0; group_id < num_groups; ++group_id)
            splitted_blocks[group_id].getByPosition(col_idx_in_block).column = std::move(splitted_columns[group_id]);
    }

    for (auto [group, group_id] : enumerated_groups)
    {
        auto & bucket_to_blocks = grouping_sets[group];
        auto & splitted_block = splitted_blocks[group_id];
        splitted_block.info = block.info;
        bucket_to_blocks[block.info.bucket_num].emplace_back(std::move(splitted_block));
    }
}

void MergingAggregatedTransform::appendGroupingColumn(UInt64 group, BlocksList & block_list)
{
    auto grouping_position = getOutputPort().getHeader().getPositionByName("__grouping_set");
    for (auto & block : block_list)
    {
        auto num_rows = block.rows();
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeUInt64>();
        col.name = "__grouping_set";
        col.column = ColumnUInt64::create(num_rows, group);
        block.insert(grouping_position, std::move(col));
    }
}

void MergingAggregatedTransform::consume(Chunk chunk)
{
    if (!consume_started)
    {
        consume_started = true;
        LOG_TRACE(log, "Reading blocks of partially aggregated data.");
    }

    size_t input_rows = chunk.getNumRows();
    if (!input_rows)
        return;

    total_input_rows += input_rows;
    ++total_input_blocks;

    if (chunk.getChunkInfos().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk info was not set for chunk in MergingAggregatedTransform.");

    if (auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>())
    {
        /** If the remote servers used a two-level aggregation method,
          * then blocks will contain information about the number of the bucket.
          * Then the calculations can be parallelized by buckets.
          * We decompose the blocks to the bucket numbers indicated in them.
          */
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        block.info.is_overflows = agg_info->is_overflows;
        block.info.bucket_num = agg_info->bucket_num;

        addBlock(std::move(block));
    }
    else if (chunk.getChunkInfos().get<ChunkInfoWithAllocatedBytes>())
    {
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        block.info.is_overflows = false;
        block.info.bucket_num = -1;

        addBlock(std::move(block));
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have AggregatedChunkInfo in MergingAggregatedTransform.");
}

Chunk MergingAggregatedTransform::generate()
{
    if (!generate_started)
    {
        generate_started = true;
        LOG_DEBUG(log, "Read {} blocks of partially aggregated data, total {} rows.", total_input_blocks, total_input_rows);

        /// Exception safety. Make iterator valid in case any method below throws.
        next_block = blocks.begin();

        for (auto & [group, group_blocks] : grouping_sets)
        {
            /// TODO: this operation can be made async. Add async for IAccumulatingTransform.
            AggregatedDataVariants data_variants;
            params->aggregator.mergeBlocks(std::move(group_blocks), data_variants, max_threads, is_cancelled);
            auto merged_blocks = params->aggregator.convertToBlocks(data_variants, params->final, max_threads);

            if (has_grouping_sets)
                appendGroupingColumn(group, merged_blocks);

            blocks.splice(blocks.end(), std::move(merged_blocks));
        }

        next_block = blocks.begin();
    }

    if (next_block == blocks.end())
        return {};

    auto block = std::move(*next_block);
    ++next_block;

    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = block.info.bucket_num;
    info->is_overflows = block.info.is_overflows;

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);

    chunk.getChunkInfos().add(std::move(info));

    return chunk;
}

}
