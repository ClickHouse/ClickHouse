#include <DataStreams/TTLUpdateInfoAlgorithm.h>

namespace DB
{

TTLUpdateInfoAlgorithm::TTLUpdateInfoAlgorithm(
    const TTLDescription & description_,
    const TTLUpdateType ttl_update_type_,
    const String ttl_update_key_,
    const TTLInfo & old_ttl_info_,
    time_t current_time_,
    bool force_)
    : ITTLAlgorithm(description_, old_ttl_info_, current_time_, force_)
    , ttl_update_type(ttl_update_type_)
    , ttl_update_key(ttl_update_key_)
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
    if (ttl_update_type == TTLUpdateType::RECOMPRESSION_TTL)
    {
        data_part->ttl_infos.recompression_ttl[ttl_update_key] = new_ttl_info;
    }
    else if (ttl_update_type == TTLUpdateType::MOVES_TTL)
    {
        data_part->ttl_infos.moves_ttl[ttl_update_key] = new_ttl_info;
    }
    else if (ttl_update_type == TTLUpdateType::GROUP_BY_TTL)
    {
        data_part->ttl_infos.group_by_ttl[ttl_update_key] = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
    }
    else if (ttl_update_type == TTLUpdateType::ROWS_WHERE_TTL)
    {
        data_part->ttl_infos.rows_where_ttl[ttl_update_key] = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
    }
    else if (ttl_update_type == TTLUpdateType::TABLE_TTL)
    {
        data_part->ttl_infos.table_ttl = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
    }
    else if (ttl_update_type == TTLUpdateType::COLUMNS_TTL)
    {
        data_part->ttl_infos.columns_ttl[ttl_update_key] = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info.min, new_ttl_info.max);
    }

}

}
