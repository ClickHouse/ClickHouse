#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;

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
            ExpressionActionsPtr expression_);

    String getName() const override { return "ExpressionTransform"; }

    static Block transformHeader(Block header, const ActionsDAG & expression);

protected:
    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;
};

}
