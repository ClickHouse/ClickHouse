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
    SharedHeader header_, Aggregator::Params params, bool final, GroupingSetsParamsList grouping_sets_params)
    : IAccumulatingTransform(header_, std::make_shared<const Block>(appendGroupingIfNeeded(*header_, params.getHeader(*header_, final))))
{
    if (!grouping_sets_params.empty())
    {
        if (!header_->has("__grouping_set"))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot find __grouping_set column in header of MergingAggregatedTransform with grouping sets."
                "Header {}", header_->dumpStructure());

        auto in_header = *header_;
        in_header.erase(header_->getPositionByName("__grouping_set"));
        auto out_header = params.getHeader(*header_, final);

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
                params.min_hit_rate_to_use_consecutive_keys_optimization,
                params.serialize_string_with_zero_byte);

            auto transform_params = std::make_shared<AggregatingTransformParams>(std::make_shared<const Block>(reordering.updateHeader(in_header)), std::move(set_params), final);

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

void MergingAggregatedTransform::addChunk(Columns columns, size_t num_rows, Int32 bucket_num, bool is_overflows)
{
    if (grouping_sets.size() == 1)
    {
        if (grouping_sets[0].reordering_key_columns_actions)
        {
            auto block = getInputPort().getHeader().cloneWithColumns(columns);
            block.erase(block.getPositionByName("__grouping_set"));
            grouping_sets[0].reordering_key_columns_actions->execute(block);
            columns = block.getColumns();
            num_rows = block.rows();
        }
        grouping_sets[0].bucket_to_chunks[bucket_num].emplace_back(
            Chunk(std::move(columns), num_rows), bucket_num, is_overflows);
        return;
    }

    auto block = getInputPort().getHeader().cloneWithColumns(columns);
    auto grouping_position = block.getPositionByName("__grouping_set");
    auto grouping_column = block.getByPosition(grouping_position).column;
    block.erase(grouping_position);

    /// Split a block by __grouping_set values.

    const auto * grouping_column_typed = typeid_cast<const ColumnUInt64 *>(grouping_column.get());
    if (!grouping_column_typed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected UInt64 column for __grouping_set, got {}", grouping_column->getName());

    IColumn::Selector selector;

    const auto & grouping_data = grouping_column_typed->getData();
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
        grouping_sets[last_group].reordering_key_columns_actions->execute(block);
        grouping_sets[last_group].bucket_to_chunks[bucket_num].emplace_back(
            Chunk(block.getColumns(), block.rows()), bucket_num, is_overflows);
        return;
    }

    /// Fill the last equal range.
    selector.resize_fill(num_rows, last_group);

    const size_t num_groups = max_group + 1;
    Blocks split_blocks(num_groups);

    for (size_t group_id = 0; group_id < num_groups; ++group_id)
        split_blocks[group_id] = block.cloneEmpty();

    size_t columns_in_block = block.columns();
    for (size_t col_idx_in_block = 0; col_idx_in_block < columns_in_block; ++col_idx_in_block)
    {
        MutableColumns split_columns = block.getByPosition(col_idx_in_block).column->scatter(num_groups, selector);
        for (size_t group_id = 0; group_id < num_groups; ++group_id)
            split_blocks[group_id].getByPosition(col_idx_in_block).column = std::move(split_columns[group_id]);
    }

    for (size_t group = 0; group < num_groups; ++group)
    {
        auto & split_block = split_blocks[group];
        split_block.info = block.info;
        grouping_sets[group].reordering_key_columns_actions->execute(split_block);
        grouping_sets[group].bucket_to_chunks[bucket_num].emplace_back(
            Chunk(split_block.getColumns(), split_block.rows()), bucket_num, is_overflows);
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
        addChunk(chunk.detachColumns(), input_rows, agg_info->bucket_num, agg_info->is_overflows);
    }
    else if (chunk.getChunkInfos().get<ChunkInfoWithAllocatedBytes>())
    {
        addChunk(chunk.detachColumns(), input_rows, -1, false);
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
        next_chunk = chunks.begin();

        for (auto & grouping_set : grouping_sets)
        {
            auto & params = grouping_set.params;
            auto & bucket_to_chunks = grouping_set.bucket_to_chunks;
            AggregatedDataVariants data_variants;

            /// TODO: this operation can be made async. Add async for IAccumulatingTransform.
            params->aggregator.mergeBlocks(std::move(bucket_to_chunks), data_variants, is_cancelled);
            auto merged_chunks = params->aggregator.convertToChunks(data_variants, params->final);

            if (grouping_set.creating_missing_keys_actions)
            {
                auto res_header = params->params.getHeader(params->header, params->final);
                for (auto & agg_chunk : merged_chunks)
                {
                    auto block = res_header.cloneWithColumns(agg_chunk.chunk.detachColumns());
                    grouping_set.creating_missing_keys_actions->execute(block);
                    agg_chunk.chunk = Chunk(block.getColumns(), block.rows());
                }
            }

            chunks.splice(chunks.end(), std::move(merged_chunks));
        }

        next_chunk = chunks.begin();
    }

    if (next_chunk == chunks.end())
        return {};

    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = next_chunk->bucket_num;
    info->is_overflows = next_chunk->is_overflows;

    Chunk chunk = std::move(next_chunk->chunk);
    chunk.getChunkInfos().add(std::move(info));

    ++next_chunk;

    return chunk;
}

}
