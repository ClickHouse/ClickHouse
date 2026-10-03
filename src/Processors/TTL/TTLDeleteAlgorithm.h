#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>

namespace DB
{

/// Deletes rows according to table TTL description with
/// possible optional condition in 'WHERE' clause.
/// With `keep_expired_rows_` nothing is deleted. The algorithm
/// only recalculates the TTL info of the rows, the expired
/// ones included, and never marks the TTL as finished, so the
/// part is selected for a TTL merge that may delete them.
class TTLDeleteAlgorithm final : public ITTLAlgorithm
{
public:
    TTLDeleteAlgorithm(const TTLExpressions & ttl_expressions_, const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_, bool keep_expired_rows_);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;
    size_t getNumberOfRemovedRows() const { return rows_removed; }
    size_t getNumberOfKeptExpiredRows() const { return rows_kept_expired; }

private:
    const bool keep_expired_rows;
    size_t rows_removed = 0;
    size_t rows_kept_expired{0};
};

}
