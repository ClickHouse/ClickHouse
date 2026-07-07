#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>


namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

using ConstraintsExpressions = std::vector<ExpressionActionsPtr>;

/** Check for constraints violation. If anything is found - throw an exception with detailed error message.
  * Otherwise just pass block to output unchanged.
  */

struct ConstraintsDescription;

class CheckConstraintsTransform final : public ExceptionKeepingTransform
{
public:
    CheckConstraintsTransform(
            const StorageID & table_,
            SharedHeader header,
            const ConstraintsDescription & constraints_,
            ContextPtr context_);

    String getName() const override { return "CheckConstraintsTransform"; }

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

private:
    StorageID table_id;
    const ASTs constraints_to_check;
    const ConstraintsExpressions expressions;
    ContextPtr context;
    size_t rows_written = 0;
    Chunk cur_chunk;
};
}
