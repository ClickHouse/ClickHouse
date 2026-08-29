#include <Processors/TTL/TTLUpdateInfoAlgorithm.h>

namespace DB
{

TTLUpdateInfoAlgorithm::TTLUpdateInfoAlgorithm(
    const TTLExpressions & ttl_expressions_,
    const TTLDescription & description_,
    TTLUpdateField ttl_update_field_,
    String ttl_update_key_,
    const TTLInfo & old_ttl_info_,
    time_t current_time_,
    bool force_)
    : ITTLAlgorithm(ttl_expressions_, description_, old_ttl_info_, current_time_, force_)
    , ttl_update_field(ttl_update_field_)
    , ttl_update_key(ttl_update_key_)
{
}

void TTLUpdateInfoAlgorithm::execute(Block & block)
{
    if (block.empty())
        return;

    auto ttl_column = executeExpressionAndGetColumn(ttl_expressions.expression, block, description.result_column);

    const size_t rows = block.rows();
    PaddedPODArray<Int64> timestamps;
    extractTimestamps(ttl_column.get(), timestamps);

    for (size_t i = 0; i < rows; ++i)
        new_ttl_info.update(timestamps[i]);
}

void TTLUpdateInfoAlgorithm::finalize(const MutableDataPartPtr & data_part) const
{
    if (ttl_update_field == TTLUpdateField::RECOMPRESSION_TTL)
    {
        data_part->ttl_infos.recompression_ttl[ttl_update_key] = new_ttl_info;
    }
    else if (ttl_update_field == TTLUpdateField::MOVES_TTL)
    {
        data_part->ttl_infos.moves_ttl[ttl_update_key] = new_ttl_info;
    }
    else if (ttl_update_field == TTLUpdateField::GROUP_BY_TTL)
    {
        data_part->ttl_infos.group_by_ttl[ttl_update_key] = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info);
    }
    else if (ttl_update_field == TTLUpdateField::ROWS_WHERE_TTL)
    {
        data_part->ttl_infos.rows_where_ttl[ttl_update_key] = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info);
    }
    else if (ttl_update_field == TTLUpdateField::TABLE_TTL)
    {
        data_part->ttl_infos.table_ttl = new_ttl_info;
        /// Record the rows-TTL expression and time zone these timestamps were computed under
        /// (see `MergeTreeDataPartTTLInfos`) - unless some row's timestamp is exactly 0 (the epoch),
        /// which means "no TTL" to the rest of the machinery and is excluded from the stored `min`:
        /// with such a row the bounds are not a complete summary of the part and must not carry a
        /// fingerprint.
        if (new_ttl_info.has_epoch_timestamps)
        {
            data_part->ttl_infos.table_ttl_expression.clear();
            data_part->ttl_infos.table_ttl_timezone.clear();
        }
        else
        {
            data_part->ttl_infos.table_ttl_expression = getRowsTTLExpressionFingerprint(description);
            data_part->ttl_infos.table_ttl_timezone = getRowsTTLTimeZoneFingerprint(description, date_lut);
        }
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info);
    }
    else if (ttl_update_field == TTLUpdateField::COLUMNS_TTL)
    {
        data_part->ttl_infos.columns_ttl[ttl_update_key] = new_ttl_info;
        data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info);
    }

}

}
