#include <Processors/Transforms/MergingAggregatedTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Common/logger_useful.h>
#include <Interpreters/ExpressionActions.h>
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

/// We should keep the order for GROUPING SET keys.
/// Initiator creates a separate Aggregator for every group, so should we do here.
/// Otherwise, two-level aggregation will split the data into different buckets,
/// and the result may have duplicating rows.
static ActionsDAG makeReorderingActions(const Block & in_header, const GroupingSetsParams & params)
{
    ActionsDAG reordering(in_header.getColumnsWithTypeAndName());
    auto & outputs = reordering.getOutputs();
    ActionsDAG::NodeRawConstPtrs new_outputs;
    new_outputs.reserve(in_header.columns() + params.used_keys.size() - params.used_keys.size());

    std::unordered_map<std::string_view, size_t> index;
    for (size_t pos = 0; pos < outputs.size(); ++pos)
        index.emplace(outputs[pos]->result_name, pos);

    for (const auto & used_name : params.used_keys)
    {
        auto & idx = index[used_name];
        new_outputs.push_back(outputs[idx]);
    }

    for (const auto & used_name : params.used_keys)
        index[used_name] = outputs.size();
    for (const auto & missing_name : params.missing_keys)
        index[missing_name] = outputs.size();

    for (const auto * output : outputs)
    {
        if (index[output->result_name] != outputs.size())
            new_outputs.push_back(output);
    }

    outputs.swap(new_outputs);
    return reordering;
}

MergingAggregatedTransform::~MergingAggregatedTransform() = default;

MergingAggregatedTransform::MergingAggregatedTransform(
    Block header_,
    Aggregator::Params params,
    bool final,
    GroupingSetsParamsList grouping_sets_params,
    size_t max_threads_)
    : IAccumulatingTransform(header_, appendGroupingIfNeeded(header_, params.getHeader(header_, final)))
    , max_threads(max_threads_)
{
    if (!grouping_sets_params.empty())
    {
        if (!header_.has("__grouping_set"))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot find __grouping_set column in header of MergingAggregatedTransform with grouping sets."
                "Header {}", header_.dumpStructure());

        auto in_header = header_;
        in_header.erase(header_.getPositionByName("__grouping_set"));
        auto out_header = params.getHeader(header_, final);

        grouping_sets.reserve(grouping_sets_params.size());
        for (const auto & grouping_set_params : grouping_sets_params)
        {
            size_t group = grouping_sets.size();

            auto reordering = makeReorderingActions(in_header, grouping_set_params);

            Aggregator::Params set_params(grouping_set_params.used_keys,
                params.aggregates,
                params.overflow_row,
                params.max_threads,
                params.max_block_size,
                params.min_hit_rate_to_use_consecutive_keys_optimization);

            auto transform_params = std::make_shared<AggregatingTransformParams>(reordering.updateHeader(in_header), std::move(set_params), final);

            auto creating = AggregatingStep::makeCreatingMissingKeysForGroupingSetDAG(
                transform_params->getHeader(),
                out_header,
                grouping_sets_params, group, false);

            auto & groupiung_set = grouping_sets.emplace_back();
            groupiung_set.reordering_key_columns_actions = std::make_shared<ExpressionActions>(std::move(reordering));
            groupiung_set.creating_missing_keys_actions = std::make_shared<ExpressionActions>(std::move(creating));
            groupiung_set.params = std::move(transform_params);
        }
    }
    else
    {
        auto & groupiung_set = grouping_sets.emplace_back();
        groupiung_set.params = std::make_shared<AggregatingTransformParams>(header_, std::move(params), final);
    }
}

void MergingAggregatedTransform::addBlock(Block block)
{
    if (grouping_sets.size() == 1)
    {
        auto bucket = block.info.bucket_num;
        if (grouping_sets[0].reordering_key_columns_actions)
            grouping_sets[0].reordering_key_columns_actions->execute(block);
        grouping_sets[0].bucket_to_blocks[bucket].emplace_back(std::move(block));
        return;
    }

    auto grouping_position = block.getPositionByName("__grouping_set");
    auto grouping_column = block.getByPosition(grouping_position).column;
    block.erase(grouping_position);

    /// Split a block by __grouping_set values.

    const auto * grouping_column_typed = typeid_cast<const ColumnUInt64 *>(grouping_column.get());
    if (!grouping_column_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected UInt64 column for __grouping_set, got {}", grouping_column->getName());

    IColumn::Selector selector;

    const auto & grouping_data = grouping_column_typed->getData();
    size_t num_rows = grouping_data.size();
    UInt64 last_group = grouping_data[0];
    UInt64 max_group = last_group;
    for (size_t row = 1; row < num_rows; ++row)
    {
        auto group = grouping_data[row];

        /// Optimization for equal ranges.
        if (last_group == group)
            continue;

        /// Optimization for single group.
        if (selector.empty())
            selector.reserve(num_rows);

        /// Fill the last equal range.
        selector.resize_fill(row, last_group);
        last_group = group;
        max_group = std::max(last_group, max_group);
    }

    if (max_group >= grouping_sets.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid group number {}. Number of groups {}.", last_group, grouping_sets.size());

    /// Optimization for single group.
    if (selector.empty())
    {
        auto bucket = block.info.bucket_num;
        grouping_sets[last_group].reordering_key_columns_actions->execute(block);
        grouping_sets[last_group].bucket_to_blocks[bucket].emplace_back(std::move(block));
        return;
    }

    /// Fill the last equal range.
    selector.resize_fill(num_rows, last_group);

    const size_t num_groups = max_group + 1;
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

    for (size_t group = 0; group < num_groups; ++group)
    {
        auto & splitted_block = splitted_blocks[group];
        splitted_block.info = block.info;
        grouping_sets[group].reordering_key_columns_actions->execute(splitted_block);
        grouping_sets[group].bucket_to_blocks[block.info.bucket_num].emplace_back(std::move(splitted_block));
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

        for (auto & grouping_set : grouping_sets)
        {
            auto & params = grouping_set.params;
            auto & bucket_to_blocks = grouping_set.bucket_to_blocks;
            AggregatedDataVariants data_variants;

            /// TODO: this operation can be made async. Add async for IAccumulatingTransform.
            params->aggregator.mergeBlocks(std::move(bucket_to_blocks), data_variants, max_threads, is_cancelled);
            auto merged_blocks = params->aggregator.convertToBlocks(data_variants, params->final, max_threads);

            if (grouping_set.creating_missing_keys_actions)
                for (auto & block : merged_blocks)
                    grouping_set.creating_missing_keys_actions->execute(block);

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
