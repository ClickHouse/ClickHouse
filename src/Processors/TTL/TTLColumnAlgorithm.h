#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>

namespace DB
{

/// Deletes (replaces to default) values in column according to column's TTL description.
/// If all values in column are replaced with defaults, this column won't be written to part.
class TTLColumnAlgorithm final : public ITTLAlgorithm
{
public:
    TTLColumnAlgorithm(
        const TTLExpressions & ttl_expressions_,
        const TTLDescription & description_,
        const TTLInfo & old_ttl_info_,
        time_t current_time_,
        bool force_,
        const String & column_name_,
        const ExpressionActionsPtr & default_expression_,
        const String & default_column_name_,
        bool is_compact_part_,
        bool earlier_set_can_expire_ = false
    );

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;

private:
    /// The precomputed per-part `old_ttl_info.min`/`max` proves "won't fire"/"whole block expired" only
    /// for the UNMODIFIED part. When an earlier `GROUP BY ... SET` in the same `TTLTransform` can rewrite
    /// a column this column TTL's expiry reads (moving it from future to past in this block), those proofs
    /// are void, so the fast paths keyed on them must be skipped and expiry recomputed per row.
    bool minMayBeExpired() const { return isMinTTLExpired() || earlier_set_can_expire; }

    const String column_name;
    const ExpressionActionsPtr default_expression;
    const String default_column_name;

    bool is_fully_empty = true;
    bool is_compact_part;
    bool earlier_set_can_expire;
};

}
