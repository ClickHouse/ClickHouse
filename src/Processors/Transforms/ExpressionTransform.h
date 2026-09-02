#pragma once
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Core/Block_fwd.h>

#include <vector>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%clickhouse%'
  * The expression processes each row independently of the others.
  */
class ExpressionTransform final : public ISimpleTransform
{
public:
    ExpressionTransform(
        SharedHeader header_, ExpressionActionsPtr expression_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_ = nullptr);

    /// Use this overload when the transformed header is already known (computed once per step)
    /// to avoid recomputing it in every instance: the computation is linear in the size of the
    /// expression's DAG, and a step creates one transform per stream.
    ExpressionTransform(
        SharedHeader input_header_,
        SharedHeader transformed_header_,
        ExpressionActionsPtr expression_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_ = nullptr);

    String getName() const override { return "ExpressionTransform"; }

    static Block transformHeader(const Block & header, const ActionsDAG & expression);

protected:
    void onCancel() noexcept override;

    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;

    /// Mapping from required input slot to input-header position, precomputed once (the input header is fixed).
    /// Lets transform() run the expression positionally without rebuilding a Block name index per chunk.
    std::vector<ssize_t> input_positions;

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
};

class ConvertingTransform final : public ExceptionKeepingTransform
{
public:
    ConvertingTransform(
        SharedHeader header_,
        ExpressionActionsPtr expression_);

    String getName() const override { return "ConvertingTransform"; }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

private:
    ExpressionActionsPtr expression;
    Chunk cur_chunk;
};

}
