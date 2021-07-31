#include <DataStreams/TTLUpdateInfoAlgorithm.h>

namespace DB
{

TTLUpdateInfoAlgorithm::TTLUpdateInfoAlgorithm(
    const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_)
    : ITTLAlgorithm(description_, old_ttl_info_, current_time_, force_)
{
}

void TTLUpdateInfoAlgorithm::execute(Block & block)
{
    if (!block)
        return;

    auto ttl_column = executeExpressionAndGetColumn(description.expression, block, description.result_column);
    for (size_t i = 0; i < block.rows(); ++i)
    {
        UInt32 cur_ttl = ITTLAlgorithm::getTimestampByIndex(ttl_column.get(), i);
        new_ttl_info.update(cur_ttl);
    }
}

void TTLUpdateInfoAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    if (description.mode == TTLMode::RECOMPRESS)
    {
        data_part->ttl_infos.recompression_ttl[description.result_column] = new_ttl_info;
    }
    else if (description.mode == TTLMode::MOVE)
    {
        data_part->ttl_infos.moves_ttl[description.result_column] = new_ttl_info;
    }
    else if (description.mode == TTLMode::GROUP_BY)
    {
        data_part->ttl_infos.group_by_ttl[description.result_column] = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
    }
    else if (description.mode == TTLMode::DELETE)
    {
        if (description.where_expression)
            data_part->ttl_infos.rows_where_ttl[description.result_column] = new_ttl_info;
        else
            data_part->ttl_infos.table_ttl = new_ttl_info;

        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
    }

}

}
