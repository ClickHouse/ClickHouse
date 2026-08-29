#include <Processors/TTL/TTLDeleteAlgorithm.h>

namespace DB
{

TTLDeleteAlgorithm::TTLDeleteAlgorithm(
    const TTLExpressions & ttl_expressions_,
    const TTLDescription & description_,
    const TTLInfo & old_ttl_info_,
    String old_ttl_expression_fingerprint_,
    String old_ttl_timezone_fingerprint_,
    time_t current_time_,
    bool force_)
    : ITTLAlgorithm(ttl_expressions_, description_, old_ttl_info_, current_time_, force_)
    , old_ttl_expression_fingerprint(std::move(old_ttl_expression_fingerprint_))
    , old_ttl_timezone_fingerprint(std::move(old_ttl_timezone_fingerprint_))
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
    {
        data_part->ttl_infos.table_ttl = new_ttl_info;
        /// Record the rows-TTL expression and time zone these timestamps were computed under
        /// (see `MergeTreeDataPartTTLInfos`) - the current metadata fingerprint when this algorithm
        /// actually recomputed them by scanning the rows. When the min TTL is not expired (and the
        /// recomputation is not forced), `execute` never looks at the rows and `new_ttl_info` is just
        /// a copy of the incoming infos, so restore the fingerprint those bounds were computed under -
        /// `TTLTransform::finalize` cleared `data_part->ttl_infos` wholesale before this call, and
        /// stamping the current metadata expression instead could mislabel bounds computed under an
        /// older TTL expression.
        if (isMinTTLExpired())
        {
            /// A timestamp of exactly 0 (the epoch) means "no TTL" to the rest of the machinery and is
            /// excluded from the stored `min`, so if any surviving row computed to it, the bounds are
            /// not a complete summary of the part and must not carry a fingerprint.
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
        }
        else
        {
            data_part->ttl_infos.table_ttl_expression = old_ttl_expression_fingerprint;
            data_part->ttl_infos.table_ttl_timezone = old_ttl_timezone_fingerprint;
        }
    }

    data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info);
}

}
