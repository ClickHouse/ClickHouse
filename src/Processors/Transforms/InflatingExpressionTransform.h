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

protected:
    void transform(Chunk & chunk) override;
    bool needInputData() const override { return !not_processed; }

private:
    ExpressionActionsPtr expression;
    bool on_totals;
    bool default_totals;
    bool initialized = false;

    ExtraBlockPtr not_processed;
    size_t action_number = 0;

    Block readExecute(Chunk & chunk);
};

}
