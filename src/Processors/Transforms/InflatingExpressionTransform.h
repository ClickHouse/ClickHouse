#pragma once
#include <Processors/ISimpleTransform.h>


namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class InflatingExpressionTransform : public ISimpleTransform
{
public:
    InflatingExpressionTransform(Block input_header, ExpressionActionsPtr expression_,
                                 bool on_totals_ = false, bool default_totals_ = false);

    String getName() const override { return "InflatingExpressionTransform"; }

    static Block transformHeader(Block header, const ExpressionActionsPtr & expression);

protected:
    void transform(Chunk & chunk) override;
    bool needInputData() const override { return !not_processed; }

private:
    ExpressionActionsPtr expression;
    bool on_totals;
    /// This flag means that we have manually added totals to our pipeline.
    /// It may happen in case if joined subquery has totals, but out string doesn't.
    /// We need to join default values with subquery totals if we have them, or return empty chunk is haven't.
    bool default_totals;
    bool initialized = false;

    ExtraBlockPtr not_processed;

    Block readExecute(Chunk & chunk);
};

}
