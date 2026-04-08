#pragma once

#include <Processors/TTL/ITTLAlgorithm.h>

namespace DB
{

/// Deletes rows according to table TTL description with
/// possible optional condition in 'WHERE' clause.
class TTLDeleteAlgorithm final : public ITTLAlgorithm
{
public:
    TTLDeleteAlgorithm(const TTLExpressions & ttl_expressions_, const TTLDescription & description_, const TTLInfo & old_ttl_info_, time_t current_time_, bool force_);

    void execute(Block & block) override;
    void finalize(const MutableDataPartPtr & data_part) const override;
    size_t getNumberOfRemovedRows() const { return rows_removed; }

    void setOverflowCheck(ExpressionActionsPtr expression, String result_column)
    {
        overflow_check_expression = std::move(expression);
        overflow_check_result_column = std::move(result_column);
    }

private:
    size_t rows_removed = 0;
    ExpressionActionsPtr overflow_check_expression;
    String overflow_check_result_column;
};

}
