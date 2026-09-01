#include <Processors/TTL/TTLAggregationAlgorithm.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/castColumn.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Core/Settings.h>

#include <Storages/MergeTree/MergeTreeSettings.h>

#include <limits>
#include <unordered_set>

namespace DB
{
namespace Setting
{
    extern const SettingsBool compile_aggregate_expressions;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool enable_software_prefetch_in_aggregation;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsFloat min_hit_rate_to_use_consecutive_keys_optimization;
    extern const SettingsBool optimize_group_by_constant_keys;
    extern const SettingsBool enable_packed_string_keys_in_aggregation;
    extern const SettingsBool enable_producing_buckets_out_of_order_in_aggregation;
    extern const SettingsBool serialize_string_in_memory_with_zero_byte;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 ttl_group_by_unsorted_max_bytes_before_external_group_by;
}

namespace
{

bool isCoveredByGroupByOrSet(const TTLDescription & description, const std::string & column_name)
{
    return std::ranges::contains(description.group_by_keys, column_name)
        || std::ranges::contains(description.set_parts | std::views::transform(&TTLAggregateDescription::column_name), column_name);
}

std::pair<AggregateDescription, TTLAggregateDescription> prepareAnyAggregate(const ColumnWithTypeAndName & column, const ContextPtr & context)
{
    AggregateDescription aggregate;
    aggregate.column_name = column.name;
    aggregate.argument_names = {column.name};
    AggregateFunctionProperties properties;
    aggregate.function = AggregateFunctionFactory::instance().get("any", NullsAction::EMPTY, {column.type}, {}, properties);

    TTLAggregateDescription set_part;
    set_part.column_name = column.name;
    set_part.expression_result_column_name = column.name;
    set_part.expression = std::make_shared<ExpressionActions>(ActionsDAG(NamesAndTypesList{{column.name, aggregate.function->getResultType()}}), ExpressionActionsSettings(context));

    return {std::move(aggregate), std::move(set_part)};
}

TTLDescription addImplicitlyAggregatedColumns(TTLDescription description, const Block & header, const ContextPtr & context)
{
    for (const auto & column : header)
    {
        if (isCoveredByGroupByOrSet(description, column.name))
            continue;

        auto [aggregate, set_part] = prepareAnyAggregate(column, context);

        description.aggregate_descriptions.push_back(std::move(aggregate));
        description.set_parts.push_back(std::move(set_part));
    }

    return description;
}

}

TTLAggregationAlgorithm::TTLAggregationAlgorithm(
    const TTLExpressions & ttl_expressions_,
    const TTLDescription & description_,
    const TTLInfo & old_ttl_info_,
    time_t current_time_,
    bool force_,
    const Block & header_,
    const MergeTreeData & storage_,
    bool input_sorted_by_group_by_keys_)
    : ITTLAlgorithm(ttl_expressions_, addImplicitlyAggregatedColumns(description_, header_, storage_.getContext()), old_ttl_info_, current_time_, force_)
    , header(header_)
    , input_sorted_by_group_by_keys(input_sorted_by_group_by_keys_)
{
    current_key_value.resize(description.group_by_keys.size());

    const auto & keys = description.group_by_keys;

    key_columns.resize(description.group_by_keys.size());
    AggregateDescriptions aggregates = description.aggregate_descriptions;

    columns_for_aggregator.resize(description.aggregate_descriptions.size());
    const Settings & settings = storage_.getContext()->getSettingsRef();
    /// Only the unsorted path can accumulate the whole part's expired keys at once, so only it gets
    /// the external-aggregation bound. The sorted path holds a single key run at a time and flushes
    /// on every key change, so its hash table never outgrows one group.
    const UInt64 max_bytes_before_external_group_by
        = (!input_sorted_by_group_by_keys_ && storage_.getContext()->getTempDataOnDisk())
        ? (*storage_.getSettings())[MergeTreeSetting::ttl_group_by_unsorted_max_bytes_before_external_group_by]
        : 0;

    /// Exact aggregation: every expired key must reach the written part, so no approximate cap
    /// applies here. The unsorted path holds all keys of the part at once, where a cap would either
    /// fail the merge (`throw`) or drop groups (`any`).
    Aggregator::Params params(
        keys,
        aggregates,
        /*overflow_row_=*/false,
        /*max_rows_to_group_by_=*/0,
        OverflowMode::THROW,
        /*group_by_two_level_threshold*/ 0,
        /// The `Aggregator` can spill only a two-level hash table, so the two-level conversion must
        /// trigger no later than the external bound; with both thresholds at 0 the table would stay
        /// single-level and the bound above would be dead letter.
        /*group_by_two_level_threshold_bytes*/ max_bytes_before_external_group_by,
        Aggregator::Params::getMaxBytesBeforeExternalGroupBy(
            max_bytes_before_external_group_by,
            /*max_bytes_ratio_before_external_group_by=*/0),
        settings[Setting::empty_result_for_aggregation_by_empty_set],
        storage_.getContext()->getTempDataOnDisk(),
        settings[Setting::max_threads],
        settings[Setting::min_free_disk_space_for_temporary_data],
        settings[Setting::compile_aggregate_expressions],
        settings[Setting::min_count_to_compile_aggregate_expression],
        settings[Setting::max_block_size],
        settings[Setting::enable_software_prefetch_in_aggregation],
        /*only_merge=*/false,
        settings[Setting::optimize_group_by_constant_keys],
        settings[Setting::min_hit_rate_to_use_consecutive_keys_optimization],
        /*stats_collecting_params_=*/{},
        settings[Setting::enable_producing_buckets_out_of_order_in_aggregation],
        settings[Setting::serialize_string_in_memory_with_zero_byte],
        /*enable_parallel_single_level_merge_=*/false,
        settings[Setting::enable_packed_string_keys_in_aggregation],
        /* enable_adaptive_aggregator */ false,
        /* adaptive_aggregator_freeze_threshold */ 0,
        /* adaptive_aggregator_freeze_threshold_bytes */ 0);

    aggregator = std::make_unique<Aggregator>(header, params);

    if (isMaxTTLExpired())
        new_ttl_info.ttl_finished = true;
}

void TTLAggregationAlgorithm::execute(Block & block)
{

    bool some_rows_were_aggregated = false;
    MutableColumns result_columns = header.cloneEmptyColumns();

    if (block.empty()) /// Empty block -- no more data, but we may still have some accumulated rows
    {
        /// The state may live in memory (aggregation_result) or, when the unsorted path spilled,
        /// in temporary files on disk -- possibly only there, with the in-memory table empty.
        if (!aggregation_result.empty() || aggregator->hasTemporaryData())
        {
            finalizeAggregates(result_columns);
            some_rows_were_aggregated = true;
        }
        else /// No block, all aggregated, just finish
        {
            return;
        }
    }
    else if (input_sorted_by_group_by_keys)
    {
        executeSorted(block, result_columns, some_rows_were_aggregated);
    }
    else
    {
        executeUnsorted(block, result_columns, some_rows_were_aggregated);
    }

    block = header.cloneWithColumns(std::move(result_columns));

    /// If some rows were aggregated we have to recalculate ttl info's
    if (some_rows_were_aggregated)
    {
        auto ttl_column_after_aggregation = executeExpressionAndGetColumn(ttl_expressions.expression, block, description.result_column);
        auto where_column_after_aggregation = executeExpressionAndGetColumn(ttl_expressions.where_expression, block, description.where_result_column);
        PaddedPODArray<Int64> timestamps;
        extractTimestamps(ttl_column_after_aggregation.get(), timestamps);
        for (size_t i = 0; i < block.rows(); ++i)
        {
            bool where_filter_passed = !where_column_after_aggregation || where_column_after_aggregation->getBool(i);
            if (where_filter_passed)
                new_ttl_info.update(timestamps[i]);
        }
    }
}

void TTLAggregationAlgorithm::executeSorted(Block & block, MutableColumns & result_columns, bool & some_rows_were_aggregated)
{
    const auto & column_names = header.getNames();
    MutableColumns aggregate_columns = header.cloneEmptyColumns();

    auto ttl_column = executeExpressionAndGetColumn(ttl_expressions.expression, block, description.result_column);
    auto where_column = executeExpressionAndGetColumn(ttl_expressions.where_expression, block, description.where_result_column);
    PaddedPODArray<Int64> timestamps;
    extractTimestamps(ttl_column.get(), timestamps);

    size_t rows_aggregated = 0;
    size_t current_key_start = 0;
    size_t rows_with_current_key = 0;

    for (size_t i = 0; i < block.rows(); ++i)
    {
        Int64 cur_ttl = timestamps[i];
        bool where_filter_passed = !where_column || where_column->getBool(i);
        bool ttl_expired = isTTLExpired(cur_ttl) && where_filter_passed;

        bool same_as_current = true;
        for (size_t j = 0; j < description.group_by_keys.size(); ++j)
        {
            const String & key_column = description.group_by_keys[j];
            const IColumn * values_column = block.getByName(key_column).column.get();
            if (!same_as_current || (*values_column)[i] != current_key_value[j])
            {
                values_column->get(i, current_key_value[j]);
                same_as_current = false;
            }
        }

        /// We are observing the row with new the aggregation key.
        /// In this case we definitely need to finish the current aggregation for the previuos key and
        /// write results to `result_columns`.
        const bool observing_new_key = !same_as_current;
        /// We are observing the row with the same aggregation key, but TTL is not expired anymore.
        /// In this case we need to finish aggregation here. The current row has to be written as is.
        const bool no_new_rows_to_aggregate_within_the_same_key = same_as_current && !ttl_expired;
        /// The aggregation for this aggregation key is done.
        const bool need_to_flush_aggregation_state = observing_new_key || no_new_rows_to_aggregate_within_the_same_key;

        if (need_to_flush_aggregation_state)
        {
            if (rows_with_current_key)
            {
                some_rows_were_aggregated = true;
                calculateAggregates(aggregate_columns, current_key_start, rows_with_current_key);
            }
            finalizeAggregates(result_columns);

            current_key_start = rows_aggregated;
            rows_with_current_key = 0;
        }

        if (ttl_expired)
        {
            ++rows_with_current_key;
            ++rows_aggregated;
            for (const auto & name : column_names)
            {
                const IColumn * values_column = block.getByName(name).column.get();
                auto & column = aggregate_columns[header.getPositionByName(name)];
                column->insertFrom(*values_column, i);
            }
        }
        else
        {
            for (const auto & name : column_names)
            {
                const IColumn * values_column = block.getByName(name).column.get();
                auto & column = result_columns[header.getPositionByName(name)];
                column->insertFrom(*values_column, i);
            }
        }
    }

    if (rows_with_current_key)
    {
        some_rows_were_aggregated = true;
        calculateAggregates(aggregate_columns, current_key_start, rows_with_current_key);
    }
}

void TTLAggregationAlgorithm::executeUnsorted(Block & block, MutableColumns & result_columns, bool & some_rows_were_aggregated)
{
    /// The input is not ordered by this TTL's group_by_keys (an earlier GROUP BY TTL's SET rewrote
    /// one of them), so the streaming flush-on-key-change used by executeSorted would finalize each
    /// non-contiguous run of a key as its own group and lose data. Accumulate every expired row into
    /// the aggregation state and never finalize mid-stream; the state carries across blocks and is
    /// flushed once on the end-of-stream empty block, so all rows of a key merge into one group
    /// regardless of order. Non-expired rows pass through unchanged, as in executeSorted.
    const auto & column_names = header.getNames();
    MutableColumns aggregate_columns = header.cloneEmptyColumns();

    auto ttl_column = executeExpressionAndGetColumn(ttl_expressions.expression, block, description.result_column);
    auto where_column = executeExpressionAndGetColumn(ttl_expressions.where_expression, block, description.where_result_column);
    PaddedPODArray<Int64> timestamps;
    extractTimestamps(ttl_column.get(), timestamps);

    size_t rows_to_aggregate = 0;
    for (size_t i = 0; i < block.rows(); ++i)
    {
        Int64 cur_ttl = timestamps[i];
        bool where_filter_passed = !where_column || where_column->getBool(i);
        bool ttl_expired = isTTLExpired(cur_ttl) && where_filter_passed;

        if (ttl_expired)
        {
            ++rows_to_aggregate;
            for (const auto & name : column_names)
            {
                const IColumn * values_column = block.getByName(name).column.get();
                auto & column = aggregate_columns[header.getPositionByName(name)];
                column->insertFrom(*values_column, i);
            }
        }
        else
        {
            for (const auto & name : column_names)
            {
                const IColumn * values_column = block.getByName(name).column.get();
                auto & column = result_columns[header.getPositionByName(name)];
                column->insertFrom(*values_column, i);
            }
        }
    }

    if (rows_to_aggregate)
    {
        some_rows_were_aggregated = true;
        calculateAggregates(aggregate_columns, 0, rows_to_aggregate);
    }
}

void TTLAggregationAlgorithm::calculateAggregates(const MutableColumns & aggregate_columns, size_t start_pos, size_t length)
{
    Columns aggregate_chunk;
    aggregate_chunk.reserve(aggregate_columns.size());
    for (const auto & name : header.getNames())
    {
        const auto & column = aggregate_columns[header.getPositionByName(name)];
        ColumnPtr chunk_column = column->cut(start_pos, length);
        aggregate_chunk.emplace_back(std::move(chunk_column));
    }

    aggregator->executeOnBlock(
        aggregate_chunk, /* row_begin= */ 0, length,
        aggregation_result, key_columns, columns_for_aggregator, no_more_keys,
        /* adaptive= */ nullptr);

}

void TTLAggregationAlgorithm::finalizeAggregates(MutableColumns & result_columns)
{
    if (aggregator->hasTemporaryData())
    {
        /// The unsorted path spilled partially-aggregated data to disk. Flush the in-memory
        /// remainder too and merge everything back bucket by bucket, so the memory high-water mark
        /// of the merge-back stays at one bucket instead of the whole part (the same scheme the
        /// external aggregation of a query uses). Every spilled block is two-level, so the
        /// remainder must be converted before it can be written.
        if (aggregation_result.isConvertibleToTwoLevel())
            aggregation_result.convertToTwoLevel();
        if (aggregation_result.hasData())
            aggregator->writeToTemporaryFile(aggregation_result);

        auto res_header = aggregator->getParams().getHeader(header, true);
        auto tmp_streams = aggregator->detachTemporaryData();

        std::vector<TemporaryBlockStreamReaderHolder> readers;
        std::vector<Block> heads;
        readers.reserve(tmp_streams.size());
        heads.reserve(tmp_streams.size());
        for (auto & tmp_stream : tmp_streams)
        {
            tmp_stream.finishWriting();
            readers.emplace_back(tmp_stream.getReadStream());
            heads.emplace_back(readers.back()->read());
        }

        /// Each stream holds one flush generation with its buckets in ascending order, so a k-way
        /// pass over the streams' heads visits each bucket exactly once. A block with no columns
        /// means the stream is exhausted; a zero-row bucket block does not.
        std::atomic<bool> is_cancelled{false};
        while (true)
        {
            constexpr Int32 no_bucket = std::numeric_limits<Int32>::max();
            Int32 min_bucket = no_bucket;
            for (const auto & head : heads)
                if (!head.empty())
                    min_bucket = std::min(min_bucket, head.info.bucket_num);
            if (min_bucket == no_bucket)
                break;

            Aggregator::AggregatedChunks bucket_chunks;
            for (size_t i = 0; i < heads.size(); ++i)
            {
                while (!heads[i].empty() && heads[i].info.bucket_num == min_bucket)
                {
                    Aggregator::AggregatedChunk bucket_chunk;
                    bucket_chunk.bucket_num = heads[i].info.bucket_num;
                    bucket_chunk.is_overflows = heads[i].info.is_overflows;
                    const size_t num_rows = heads[i].rows();
                    bucket_chunk.chunk = Chunk(heads[i].getColumns(), num_rows);
                    bucket_chunks.push_back(std::move(bucket_chunk));
                    heads[i] = readers[i]->read();
                }
            }

            auto merged = aggregator->mergeBlocks(bucket_chunks, /*final=*/true, is_cancelled, /*dataflow_cache_updater=*/nullptr);
            if (merged.chunk.getNumRows())
                appendAggregatedBlock(res_header.cloneWithColumns(merged.chunk.detachColumns()), result_columns);
        }
    }
    else if (!aggregation_result.empty())
    {
        auto aggregated_res = aggregator->convertToChunks(aggregation_result, true);
        auto res_header = aggregator->getParams().getHeader(header, true);

        for (auto & agg_chunk : aggregated_res)
            appendAggregatedBlock(res_header.cloneWithColumns(agg_chunk.chunk.detachColumns()), result_columns);
    }

    aggregation_result.invalidate();
}

void TTLAggregationAlgorithm::appendAggregatedBlock(Block agg_block, MutableColumns & result_columns)
{
    for (const auto & it : description.set_parts)
    {
        it.expression->execute(agg_block);

        /// The SET expression result type may diverge from the declared column type:
        /// aggregation strips LowCardinality, and the mismatch can be nested (e.g. a Tuple
        /// whose element wrapper differs). result_columns expects the declared type exactly,
        /// so coerce the result to it before inserting to keep the block structure valid.
        const auto & result_column_type = header.getByName(it.column_name).type;
        auto & column_with_type = agg_block.getByName(it.expression_result_column_name);
        if (!column_with_type.type->equals(*result_column_type))
        {
            column_with_type.column = castColumn(column_with_type, result_column_type);
            column_with_type.type = result_column_type;
        }
    }

    /// Since there might be intersecting columns between GROUP BY and SET, we prioritize
    /// the SET values over the GROUP BY because doing it the other way causes unexpected
    /// results.
    std::unordered_set<String> columns_added;
    for (const auto & it : description.set_parts)
    {
        /// insertRangeFrom requires the source to be of the same class as the destination.
        auto values_column = agg_block.getByName(it.expression_result_column_name).column->convertToFullIfWrapped();
        auto & result_column = result_columns[header.getPositionByName(it.column_name)];
        result_column->insertRangeFrom(*values_column, 0, agg_block.rows());
        columns_added.emplace(it.column_name);
    }

    for (const auto & name : description.group_by_keys)
    {
        if (!columns_added.contains(name))
        {
            /// Aggregation strips LowCardinality from GROUP BY keys too. This can be a
            /// subcolumn of a nested type, so coerce it back to the stream's declared
            /// type before inserting into the result column, just as for a SET result
            /// above.
            const auto & result_column_type = header.getByName(name).type;
            auto & column_with_type = agg_block.getByName(name);
            if (!column_with_type.type->equals(*result_column_type))
            {
                column_with_type.column = castColumn(column_with_type, result_column_type);
                column_with_type.type = result_column_type;
            }

            const IColumn * values_column = column_with_type.column.get();
            auto & result_column = result_columns[header.getPositionByName(name)];
            result_column->insertRangeFrom(*values_column, 0, agg_block.rows());
        }
    }
}

void TTLAggregationAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    if (new_ttl_info.finished())
    {
        data_part->ttl_infos.group_by_ttl[description.result_column] = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info);
        return;
    }
    data_part->ttl_infos.group_by_ttl[description.result_column] = old_ttl_info;
    data_part->ttl_infos.updatePartMinMaxTTL(old_ttl_info);
}

}
