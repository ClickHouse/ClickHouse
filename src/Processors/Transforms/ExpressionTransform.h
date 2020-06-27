#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ExpressionTransform : public ISimpleTransform
{
public:
    ExpressionTransform(
            const Block & header_,
            ExpressionActionsPtr expression_,
            bool on_totals_ = false);

    String getName() const override { return "ExpressionTransform"; }

    static Block transformHeader(Block header, const ExpressionActionsPtr & expression);

protected:
    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;
    bool on_totals;
    bool initialized = false;
};

}
