#pragma once

#include <DataStreams/IBlockInputStream.h>


namespace DB
{

class ExpressionActions;

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%yandex%'
  * The expression processes each row independently of the others.
  */
class ExpressionBlockInputStream : public IBlockInputStream
{
public:
    using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

    ExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_);

    String getName() const override;
    Block getTotals() override;
    Block getHeader() const override;

protected:
    bool initialized = false;
    ExpressionActionsPtr expression;

    Block readImpl() override;

private:
    Block cached_header;
};

/// ExpressionBlockInputStream that could generate many out blocks for single input block.
class InflatingExpressionBlockInputStream : public ExpressionBlockInputStream
{
public:
    InflatingExpressionBlockInputStream(const BlockInputStreamPtr & input, const ExpressionActionsPtr & expression_)
    :   ExpressionBlockInputStream(input, expression_)
    {}

protected:
    Block readImpl() override;

private:
    ExtraBlockPtr not_processed;
    size_t action_number = 0;
};

}
