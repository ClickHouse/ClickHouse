#include <Processors/TTL/TTLDeleteAlgorithm.h>

namespace DB
{

TTLDeleteAlgorithm::TTLDeleteAlgorithm(
    const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : ITTLAlgorithm(description_, old_ttl_info_, current_time_, force_)
{
    if (!isMinTTLExpired())
        new_ttl_info = old_ttl_info;

    if (isMaxTTLExpired())
        new_ttl_info.finished = true;
}

void TTLDeleteAlgorithm::execute(Block & block)
{
    if (!block || !isMinTTLExpired())
        return;

    auto ttl_column = executeExpressionAndGetColumn(description.expression, block, description.result_column);
    auto where_column = executeExpressionAndGetColumn(description.where_expression, block, description.where_result_column);

    MutableColumns result_columns;
    const auto & column_names = block.getNames();

    result_columns.reserve(column_names.size());
    for (auto it = column_names.begin(); it != column_names.end(); ++it)
    {
        const IColumn * values_column = block.getByName(*it).column.get();
        MutableColumnPtr result_column = values_column->cloneEmpty();
        result_column->reserve(block.rows());

        for (size_t i = 0; i < block.rows(); ++i)
        {
            UInt32 cur_ttl = getTimestampByIndex(ttl_column.get(), i);
            bool where_filter_passed = !where_column || where_column->getBool(i);

            if (!isTTLExpired(cur_ttl) || !where_filter_passed)
            {
                new_ttl_info.update(cur_ttl);
                result_column->insertFrom(*values_column, i);
            }
            else if (it == column_names.begin())
                ++rows_removed;
        }

        result_columns.emplace_back(std::move(result_column));
    }

    block = block.cloneWithColumns(std::move(result_columns));
}

void TTLDeleteAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    if (description.where_expression)
        data_part->ttl_infos.rows_where_ttl[description.result_column] = new_ttl_info;
    else
        data_part->ttl_infos.table_ttl = new_ttl_info;

    data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
}

}
