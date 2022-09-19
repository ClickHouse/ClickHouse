#include <Processors/TTL/TTLAggregationAlgorithm.h>
#include <Interpreters/Context.h>

namespace DB
{

TTLAggregationAlgorithm::TTLAggregationAlgorithm(
    const TTLDescription & description_,
    const TTLInfo & old_ttl_info_,
    time_t current_time_,
    bool force_,
    const Block & header_,
    const MergeTreeData & storage_)
    : ITTLAlgorithm(description_, old_ttl_info_, current_time_, force_)
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
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        0,
        0,
        settings.max_bytes_before_external_group_by,
        settings.empty_result_for_aggregation_by_empty_set,
        storage_.getContext()->getTemporaryVolume(),
        settings.max_threads,
        settings.min_free_disk_space_for_temporary_data,
        settings.compile_aggregate_expressions,
        settings.min_count_to_compile_aggregate_expression,
        settings.max_block_size);

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

        auto ttl_column = executeExpressionAndGetColumn(description.expression, block, description.result_column);
        auto where_column = executeExpressionAndGetColumn(description.where_expression, block, description.where_result_column);

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

            if (!same_as_current)
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
        auto ttl_column_after_aggregation = executeExpressionAndGetColumn(description.expression, block, description.result_column);
        auto where_column_after_aggregation = executeExpressionAndGetColumn(description.where_expression, block, description.where_result_column);
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
