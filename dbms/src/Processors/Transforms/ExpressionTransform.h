#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ExpressionTransform : public ISimpleTransform
{
public:
    ExpressionTransform(const Block & header, ExpressionActionsPtr expression);

    String getName() const override { return "ExpressionTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;
};

}
