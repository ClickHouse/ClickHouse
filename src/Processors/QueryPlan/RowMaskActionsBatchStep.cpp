#include <Processors/QueryPlan/RowMaskActionsBatchStep.h>

#include <Common/ProfileEvents.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/SetSpillable.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <algorithm>

namespace ProfileEvents
{
    extern const Event RowMaskReadySubquerySetUnionBatches;
}

namespace DB
{

namespace
{

ITransformingStep::Traits getTraits(const std::vector<ActionsDAG> & actions)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = std::ranges::none_of(actions, [](const auto & dag)
            {
                return dag.hasArrayJoin();
            }),
        }
    };
}

bool containsCompiledFunction(const ActionsDAG::Node * node)
{
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->is_function_compiled)
        return true;

    return std::ranges::any_of(node->children, containsCompiledFunction);
}

NameSet getColumnsContainCompiledFunction(const ActionsDAG & dag)
{
    NameSet result;
    for (const auto * node : dag.getOutputs())
    {
        if (containsCompiledFunction(node))
            result.insert(node->result_name);
    }
    return result;
}

struct PreparedAction
{
    PreparedAction(SharedHeader input_header_, ExpressionActionsPtr expression_)
        : input_header(std::move(input_header_))
        , expression(std::move(expression_))
        , input_positions(expression->getInputPositions(*input_header))
    {
    }

    SharedHeader input_header;
    ExpressionActionsPtr expression;
    VectorWithMemoryTracking<ssize_t> input_positions;
};

using PreparedActions = VectorWithMemoryTracking<PreparedAction>;

SharedHeader prepareAction(
    PreparedActions & prepared,
    ActionsDAG dag,
    const SharedHeader & input_header,
    const SharedHeader & expected_header,
    const ExpressionActionsSettings & settings)
{
    auto expression = std::make_shared<ExpressionActions>(std::move(dag), settings);
    auto header = std::make_shared<const Block>(expression->getActionsDAG().updateHeader(*input_header));
    prepared.emplace_back(input_header, expression);

    /// Keep the per-expression conversion used by `ExpressionStep` after compilation.
    if (!blocksHaveEqualStructure(*header, *expected_header))
    {
        auto compiled_columns = getColumnsContainCompiledFunction(expression->getActionsDAG());
        auto convert_dag = ActionsDAG::makeConvertingActions(
            header->getColumnsWithTypeAndName(),
            expected_header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            nullptr, false, false, nullptr,
            &compiled_columns);
        auto convert_actions = std::make_shared<ExpressionActions>(std::move(convert_dag), settings);
        auto converted_header = std::make_shared<const Block>(convert_actions->getActionsDAG().updateHeader(*header));
        prepared.emplace_back(header, std::move(convert_actions));
        return converted_header;
    }
    return header;
}

class RowMaskActionsBatchTransform final : public ISimpleTransform
{
public:
    RowMaskActionsBatchTransform(
        SharedHeader input_header_,
        SharedHeader output_header_,
        std::shared_ptr<const PreparedActions> actions_,
        RowMaskActionsBatchStep::MakeUnion make_union_,
        const ExpressionActionsSettings & settings_)
        : ISimpleTransform(std::move(input_header_), std::move(output_header_), false)
        , actions(std::move(actions_))
        , make_union(std::move(make_union_))
        , settings(settings_)
    {
        for (const auto & action : *actions)
            set_spillables.add(action.expression->getActionsDAG());
    }

    String getName() const override { return "RowMaskActionsBatchTransform"; }
    std::span<ISpillable * const> getSpillables() override { return set_spillables.get(); }

    void transform(Chunk & chunk) override
    {
        if (make_union && !isCancelled())
        {
            /// A main-stream chunk implies that every original Set dependency has finished.
            auto dag = make_union();
            make_union = {};
            if (dag)
            {
                prepareAction(union_actions, std::move(*dag), getInputPort().getSharedHeader(), getOutputPort().getSharedHeader(), settings);
                ProfileEvents::increment(ProfileEvents::RowMaskReadySubquerySetUnionBatches);
            }
        }

        size_t num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        const auto & active_actions = union_actions.empty() ? *actions : union_actions;
        for (const auto & action : active_actions)
        {
            if (isCancelled())
                break;

            columns = action.expression->executeOnColumns(
                std::move(columns), *action.input_header, action.input_positions, num_rows, false, &getCancellationFlag());
        }

        if (isCancelled())
        {
            chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
            return;
        }
        chunk.setColumns(std::move(columns), num_rows);
    }

protected:
    void onCancel() noexcept override
    {
        ISimpleTransform::onCancel();
        for (const auto & action : *actions)
        {
            for (const auto & node : action.expression->getNodes())
            {
                if (node.type == ActionsDAG::ActionType::FUNCTION && node.function)
                    node.function->cancelExecution();
            }
        }
    }

private:
    std::shared_ptr<const PreparedActions> actions;
    RowMaskActionsBatchStep::MakeUnion make_union;
    ExpressionActionsSettings settings;
    /// Owned by the transform thread. The replacement uses only local fixed-width mask functions.
    PreparedActions union_actions;
    SetSpillables set_spillables;
};

}

RowMaskActionsBatchStep::RowMaskActionsBatchStep(
    SharedHeader input_header_, std::vector<ActionsDAG> actions_, MakeUnion make_union_)
    : ITransformingStep(input_header_, input_header_, getTraits(actions_))
    , actions(std::move(actions_))
    , make_union(std::move(make_union_))
{
    updateOutputHeader();
}

bool RowMaskActionsBatchStep::hasCorrelatedExpressions() const
{
    return std::ranges::any_of(actions, [](const auto & dag)
    {
        return dag.hasCorrelatedColumns();
    });
}

void RowMaskActionsBatchStep::updateOutputHeader()
{
    action_output_headers.clear();
    action_output_headers.reserve(actions.size());
    auto header = input_headers.front();
    for (const auto & dag : actions)
    {
        header = std::make_shared<const Block>(dag.updateHeader(*header));
        action_output_headers.push_back(header);
    }
    output_header = std::move(header);
}

void RowMaskActionsBatchStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto prepared_actions = std::make_shared<PreparedActions>();
    prepared_actions->reserve(actions.size());
    auto header = pipeline.getSharedHeader();

    for (size_t i = 0; i < actions.size(); ++i)
        header = prepareAction(
            *prepared_actions, std::move(actions[i]), header, action_output_headers[i], settings.getActionsSettings());

    const bool single_stream = pipeline.getNumStreams() == 1;
    pipeline.addSimpleTransform([&](const SharedHeader & input_header, Pipe::StreamType stream_type)
    {
        return std::make_shared<RowMaskActionsBatchTransform>(
            input_header, header, prepared_actions,
            single_stream && stream_type == Pipe::StreamType::Main ? std::move(make_union) : MakeUnion{},
            settings.getActionsSettings());
    });
}

}
