#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%yandex%'
  * The expression processes each row independently of the others.
  */
class ExpressionTransform : public ISimpleTransform
{
public:
    ExpressionTransform(
            const Block & header_,
            ExpressionActionsPtr expression_,
            bool on_totals_ = false,
            bool default_totals_ = false);

    String getName() const override { return "ExpressionTransform"; }

    static Block transformHeader(Block header, const ExpressionActionsPtr & expression);

protected:
    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;
    bool on_totals;
    /// This flag means that we have manually added totals to our pipeline.
    /// It may happen in case if joined subquery has totals, but out string doesn't.
    /// We need to join default values with subquery totals if we have them, or return empty chunk is haven't.
    bool default_totals;
    bool initialized = false;
};

}
