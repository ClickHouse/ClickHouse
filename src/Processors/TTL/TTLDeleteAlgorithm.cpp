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
            Int64 cur_ttl = getTimestampByIndex(ttl_column.get(), i);
            bool where_filter_passed = !where_column || where_column->getBool(i);

            if (!isTTLExpired(cur_ttl) || !where_filter_passed)
            {
                /// Update ttl info only if row passes the filter.
                /// Rows that don't pass the filter should not affect TTL.
                if (where_filter_passed)
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
    if (ttl_expressions.where_expression)
        /// Rules sharing a time expression share this slot, so merge instead of overwriting.
        data_part->ttl_infos.rows_where_ttl[description.result_column].update(new_ttl_info);
    else
    {
        data_part->ttl_infos.table_ttl = new_ttl_info;
        /// Record the rows-TTL expression and time zone these timestamps were computed under
        /// (see `MergeTreeDataPartTTLInfos`) - but only when this algorithm actually recomputed them
        /// by scanning the rows. When the min TTL is not expired (and the recomputation is not forced),
        /// `execute` never looks at the rows and `new_ttl_info` is just a copy of the incoming infos,
        /// so the fingerprint those bounds were computed under is the one already propagated into
        /// `data_part->ttl_infos` by `MergeTreeDataPartTTLInfos::update` - stamping the current
        /// metadata expression over it could mislabel bounds computed under an older TTL expression.
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
                data_part->ttl_infos.table_ttl_expression = description.result_column;
                data_part->ttl_infos.table_ttl_timezone = getRowsTTLTimeZoneFingerprint(description);
            }
        }
    }

    data_part->ttl_infos.updatePartMinMaxTTL(new_ttl_info);
}

}
