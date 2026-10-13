#include <Processors/TTL/TTLDeleteAlgorithm.h>

namespace DB
{

TTLDeleteAlgorithm::TTLDeleteAlgorithm(
    const TTLExpressions & ttl_expressions_, const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : ITTLAlgorithm(ttl_expressions_, description_, old_ttl_info_, current_time_, force_)
{
    if (!isMinTTLExpired())
        new_ttl_info = old_ttl_info;

    if (isMaxTTLExpired())
        new_ttl_info.ttl_finished = true;
}

void TTLDeleteAlgorithm::execute(Block & block)
{
    if (block.empty() || !isMinTTLExpired())
        return;

    auto ttl_column = executeExpressionAndGetColumn(ttl_expressions.expression, block, description.result_column);
    auto where_column = executeExpressionAndGetColumn(ttl_expressions.where_expression, block, description.where_result_column);

    /// Decide once for the whole block, then filter the columns in one vectorized pass. A forced
    /// algorithm runs on every merge of a value-combining mode, where the common case is that
    /// nothing expires - and then the block is handed on untouched.
    const size_t rows = block.rows();
    PaddedPODArray<Int64> timestamps;
    extractTimestamps(ttl_column.get(), timestamps);

    IColumn::Filter filter(rows);
    size_t removed = 0;

    for (size_t i = 0; i < rows; ++i)
    {
        Int64 cur_ttl = timestamps[i];
        bool where_filter_passed = !where_column || where_column->getBool(i);
        bool remove = isTTLExpired(cur_ttl) && where_filter_passed;

        filter[i] = !remove;

        if (remove)
        {
            ++removed;
        }
        else if (where_filter_passed)
        {
            /// Update ttl info only if row passes the filter.
            /// Rows that don't pass the filter should not affect TTL.
            new_ttl_info.update(cur_ttl);
        }
    }

    rows_removed += removed;

    if (removed == 0)
        return;

    for (auto & column : block)
        column.column = column.column->filter(filter, rows - removed);
}

void TTLDeleteAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    if (ttl_expressions.where_expression)
        /// Rules sharing a time expression share this slot, so merge instead of overwriting.
        data_part->ttl_infos.rows_where_ttl[description.result_column].update(new_ttl_info);
    else
        data_part->ttl_infos.table_ttl = new_ttl_info;

    data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info);
}

}
