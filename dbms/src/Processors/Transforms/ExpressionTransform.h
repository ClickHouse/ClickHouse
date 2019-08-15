#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ExpressionTransform : public ISimpleTransform
{
public:
    ExpressionTransform(const Block & header, ExpressionActionsPtr expression, bool on_totals = false, bool default_totals = false);

    String getName() const override { return "ExpressionTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;
    bool on_totals;
    bool default_totals;
};

}
