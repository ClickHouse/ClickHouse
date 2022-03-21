#include <Processors/TTL/TTLColumnAlgorithm.h>

namespace DB
{

TTLColumnAlgorithm::TTLColumnAlgorithm(
    const TTLDescription & description_,
    const TTLInfo & old_ttl_info_,
    time_t current_time_,
    bool force_,
    const String & column_name_,
    const ExpressionActionsPtr & default_expression_,
    const String & default_column_name_,
    bool is_compact_part_)
    : ITTLAlgorithm(description_, old_ttl_info_, current_time_, force_)
    , column_name(column_name_)
    , default_expression(default_expression_)
    , default_column_name(default_column_name_)
    , is_compact_part(is_compact_part_)
{
    if (!isMinTTLExpired())
    {
        new_ttl_info = old_ttl_info;
        is_fully_empty = false;
    }

    if (isMaxTTLExpired())
        new_ttl_info.finished = true;
}

void TTLColumnAlgorithm::execute(Block & block)
{
    if (!block)
        return;

    /// If we read not all table columns. E.g. while mutation.
    if (!block.has(column_name))
        return;

    /// Nothing to do
    if (!isMinTTLExpired())
        return;

    /// Later drop full column
    if (isMaxTTLExpired() && !is_compact_part)
        return;

    auto default_column = executeExpressionAndGetColumn(default_expression, block, default_column_name);
    if (default_column)
        default_column = default_column->convertToFullColumnIfConst();

    auto ttl_column = executeExpressionAndGetColumn(description.expression, block, description.result_column);

    auto & column_with_type = block.getByName(column_name);
    const IColumn * values_column = column_with_type.column.get();
    MutableColumnPtr result_column = values_column->cloneEmpty();
    result_column->reserve(block.rows());

    for (size_t i = 0; i < block.rows(); ++i)
    {
        UInt32 cur_ttl = getTimestampByIndex(ttl_column.get(), i);
        if (isTTLExpired(cur_ttl))
        {
            if (default_column)
                result_column->insertFrom(*default_column, i);
            else
                result_column->insertDefault();
        }
        else
        {
            new_ttl_info.update(cur_ttl);
            is_fully_empty = false;
            result_column->insertFrom(*values_column, i);
        }
    }

    column_with_type.column = std::move(result_column);
}

void TTLColumnAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    data_part->ttl_infos.columns_ttl[column_name] = new_ttl_info;
    data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
    if (is_fully_empty)
        data_part->expired_columns.insert(column_name);
}

}
