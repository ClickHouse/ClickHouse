#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/TTL/TTLAggregationAlgorithm.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool compile_aggregate_expressions;
    extern const SettingsBool empty_result_for_aggregation_by_empty_set;
    extern const SettingsBool enable_software_prefetch_in_aggregation;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 max_bytes_before_external_group_by;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsUInt64 min_chunk_bytes_for_parallel_parsing;
    extern const SettingsUInt64 min_count_to_compile_aggregate_expression;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsBool optimize_group_by_constant_keys;
}

TTLAggregationAlgorithm::TTLAggregationAlgorithm(
    const TTLExpressions & ttl_expressions_,
    const TTLDescription & description_,
    const TTLInfo & old_ttl_info_,
    time_t current_time_,
    bool force_,
    const Block & header_,
    const MergeTreeData & storage_)
    : ITTLAlgorithm(ttl_expressions_, description_, old_ttl_info_, current_time_, force_)
    , header(header_)
{
    current_key_value.resize(description.group_by_keys.size());

    const auto & keys = description.group_by_keys;

    key_columns.resize(description.group_by_keys.size());
    AggregateDescriptions aggregates = description.aggregate_descriptions;

    columns_for_aggregator.resize(description.aggregate_descriptions.size());
    const Settings & settings = storage_.getContext()->getSettingsRef();

    Aggregator::Params params(
        keys,
        aggregates,
        false,
        settings[Setting::max_rows_to_group_by],
        settings[Setting::group_by_overflow_mode],
        /*group_by_two_level_threshold*/ 0,
        /*group_by_two_level_threshold_bytes*/ 0,
        settings[Setting::max_bytes_before_external_group_by],
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
        settings[Setting::min_chunk_bytes_for_parallel_parsing],
        /*stats_collecting_params=*/{});

    aggregator = std::make_unique<Aggregator>(header, params);

    if (isMaxTTLExpired())
        new_ttl_info.ttl_finished = true;
}

void TTLAggregationAlgorithm::execute(Block & block)
{

    bool some_rows_were_aggregated = false;
    MutableColumns result_columns = header.cloneEmptyColumns();

    if (!block) /// Empty block -- no more data, but we may still have some accumulated rows
    {
        if (!aggregation_result.empty()) /// Still have some aggregated data, let's update TTL
        {
            finalizeAggregates(result_columns);
            some_rows_were_aggregated = true;
        }
        else /// No block, all aggregated, just finish
        {
            return;
        }
    }
    else
    {
        const auto & column_names = header.getNames();
        MutableColumns aggregate_columns = header.cloneEmptyColumns();

        auto ttl_column = executeExpressionAndGetColumn(ttl_expressions.expression, block, description.result_column);
        auto where_column = executeExpressionAndGetColumn(ttl_expressions.where_expression, block, description.where_result_column);

        size_t rows_aggregated = 0;
        size_t current_key_start = 0;
        size_t rows_with_current_key = 0;

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column.get(), i);
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

    block = header.cloneWithColumns(std::move(result_columns));

    /// If some rows were aggregated we have to recalculate ttl info's
    if (some_rows_were_aggregated)
    {
        auto ttl_column_after_aggregation = executeExpressionAndGetColumn(ttl_expressions.expression, block, description.result_column);
        auto where_column_after_aggregation = executeExpressionAndGetColumn(ttl_expressions.where_expression, block, description.where_result_column);
        for (size_t i = 0; i < block.rows(); ++i)
        {
            bool where_filter_passed = !where_column_after_aggregation || where_column_after_aggregation->getBool(i);
            if (where_filter_passed)
                new_ttl_info.update(getTimestampByIndex(ttl_column_after_aggregation.get(), i));
        }
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
        aggregation_result, key_columns, columns_for_aggregator, no_more_keys);

}

void TTLAggregationAlgorithm::finalizeAggregates(MutableColumns & result_columns)
{
    if (!aggregation_result.empty())
    {
        auto aggregated_res = aggregator->convertToBlocks(aggregation_result, true, 1);

        for (auto & agg_block : aggregated_res)
        {
            for (const auto & it : description.set_parts)
                it.expression->execute(agg_block);

            for (const auto & name : description.group_by_keys)
            {
                const IColumn * values_column = agg_block.getByName(name).column.get();
                auto & result_column = result_columns[header.getPositionByName(name)];
                result_column->insertRangeFrom(*values_column, 0, agg_block.rows());
            }

            for (const auto & it : description.set_parts)
            {
                const IColumn * values_column = agg_block.getByName(it.expression_result_column_name).column.get();
                auto & result_column = result_columns[header.getPositionByName(it.column_name)];
                result_column->insertRangeFrom(*values_column, 0, agg_block.rows());
            }
        }
    }

    aggregation_result.invalidate();
}

void TTLAggregationAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    data_part->ttl_infos.group_by_ttl[description.result_column] = new_ttl_info;
    data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
}

}
