#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class ExpressionActions;


/** Implements WHERE, HAVING operations.
  * A stream of blocks and an expression, which adds to the block one ColumnUInt8 column containing the filtering conditions, are passed as input.
  * The expression is evaluated and a stream of blocks is returned, which contains only the filtered rows.
  */
class FilterBlockInputStream : public IProfilingBlockInputStream
{
private:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
    /// filter_column_ - the number of the column with filter conditions.
    FilterBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_, ssize_t filter_column_);
    FilterBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_, const String & filter_column_name_);

    String getName() const override;
    String getID() const override;
    const Block & getTotals() override;

protected:
    Block readImpl() override;

private:
    ExpressionActionsPtr expression;
    ssize_t filter_column;
    String filter_column_name;

    bool is_first = true;
    bool filter_always_true = false;
    bool filter_always_false = false;
};

}
