#pragma once

#include <optional>

#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** Implements LIMIT [n] AFTER expr [UNTIL expr].
 * Outputs rows starting from the first row where start condition is true,
 * until the first row where end condition is true (exclusive) or limit is reached.
 * If no start condition: output from first row.
 * If no end condition: output until limit or stream end.
 * If there is no limit length: no row cap.
 */
class LimitRangeTransform : public ISimpleTransform
{
public:
    LimitRangeTransform(
        SharedHeader header_,
        ExpressionActionsPtr start_expression_,
        const String & start_column_name_,
        ExpressionActionsPtr end_expression_,
        const String & end_column_name_,
        std::optional<UInt64> limit_);

    String getName() const override { return "LimitRange"; }

    void transform(Chunk & chunk) override;

private:
    /// Find first row where condition column is true (non-zero). Returns num_rows if none.
    static size_t findFirstTrue(const ColumnPtr & column, size_t num_rows);

    /// Slice chunk to rows [start_row, end_row)
    static void sliceChunk(Chunk & chunk, size_t start_row, size_t end_row);

    ExpressionActionsPtr start_expression;
    String start_column_name;
    ExpressionActionsPtr end_expression;
    String end_column_name;
    std::optional<UInt64> limit;

    bool started = false;
    UInt64 rows_output = 0;

    size_t start_column_position = 0;
    size_t end_column_position = 0;
};

}
