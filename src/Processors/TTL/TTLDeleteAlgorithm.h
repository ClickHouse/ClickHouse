#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>

namespace DB
{

/// Deletes rows according to table TTL description with
/// possible optional condition in 'WHERE' clause.
class TTLDeleteAlgorithm final : public ITTLAlgorithm
{
public:
    TTLDeleteAlgorithm(
        const TTLExpressions & ttl_expressions_,
        const TTLDescription & description_,
        const TTLInfo & old_ttl_info_,
        String old_ttl_expression_fingerprint_,
        String old_ttl_timezone_fingerprint_,
        time_t current_time_,
        bool force_);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;
    size_t getNumberOfRemovedRows() const { return rows_removed; }

private:
    /// The rows-TTL expression and time zone fingerprint the part's incoming bounds were computed
    /// under (see `MergeTreeDataPartTTLInfos`); restored when this algorithm does not rescan the rows.
    /// Empty (and unused) for TTL rules with a WHERE clause, which carry no fingerprint.
    String old_ttl_expression_fingerprint;
    String old_ttl_timezone_fingerprint;

    size_t rows_removed = 0;
};

}
