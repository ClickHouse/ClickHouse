#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPipeline.h>
#include <stack>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void QueryPlan::checkInitialized() const
{
    if (!isInitialized())
        throw Exception("QueryPlan was not initialized", ErrorCodes::LOGICAL_ERROR);
}

void QueryPlan::checkNotCompleted() const
{
    if (isCompleted())
        throw Exception("QueryPlan was already completed", ErrorCodes::LOGICAL_ERROR);
}

bool QueryPlan::isCompleted() const
{
    return isInitialized() && !root->step->hasOutputStream();
}

const DataStream & QueryPlan::getCurrentDataStream() const
{
    checkInitialized();
    checkNotCompleted();
    return root->step->getOutputStream();
}

void QueryPlan::unitePlans(QueryPlanStepPtr step, std::vector<QueryPlan> plans)
{
    if (isInitialized())
        throw Exception("Cannot unite plans because current QueryPlan is already initialized",
                        ErrorCodes::LOGICAL_ERROR);

    const auto & inputs = step->getInputStreams();
    size_t num_inputs = step->getInputStreams().size();
    if (num_inputs != plans.size())
    {
        throw Exception("Cannot unite QueryPlans using " + step->getName() +
                        " because step has different number of inputs. "
                        "Has " + std::to_string(plans.size()) + " plans "
                        "and " + std::to_string(num_inputs) + " inputs", ErrorCodes::LOGICAL_ERROR);
    }

    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i].header;
        const auto & plan_header = plans[i].getCurrentDataStream().header;
        if (!blocksHaveEqualStructure(step_header, plan_header))
            throw Exception("Cannot unite QueryPlans using " + step->getName() + " because "
                            "it has incompatible header with plan " + root->step->getName() + " "
                            "plan header: " + plan_header.dumpStructure() +
                            "step header: " + step_header.dumpStructure(), ErrorCodes::LOGICAL_ERROR);
    }

    for (auto & plan : plans)
        nodes.splice(nodes.end(), std::move(plan.nodes));

    nodes.emplace_back(Node{.step = std::move(step)});
    root = &nodes.back();

    for (auto & plan : plans)
        root->children.emplace_back(plan.root);
}

void QueryPlan::addStep(QueryPlanStepPtr step)
{
    checkNotCompleted();

    size_t num_input_streams = step->getInputStreams().size();

    if (num_input_streams == 0)
    {
        if (isInitialized())
            throw Exception("Cannot add step " + step->getName() + " to QueryPlan because "
                            "step has no inputs, but QueryPlan is already initialised", ErrorCodes::LOGICAL_ERROR);

        nodes.emplace_back(Node{.step = std::move(step)});
        root = &nodes.back();
        return;
    }

    if (num_input_streams == 1)
    {
        if (!isInitialized())
            throw Exception("Cannot add step " + step->getName() + " to QueryPlan because "
                            "step has input, but QueryPlan is not initialised", ErrorCodes::LOGICAL_ERROR);

        const auto & root_header = root->step->getOutputStream().header;
        const auto & step_header = step->getInputStreams().front().header;
        if (!blocksHaveEqualStructure(root_header, step_header))
            throw Exception("Cannot add step " + step->getName() + " to QueryPlan because "
                            "it has incompatible header with root step " + root->step->getName() + " "
                            "root header: " + root_header.dumpStructure() +
                            "step header: " + step_header.dumpStructure(), ErrorCodes::LOGICAL_ERROR);

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}});
        root = &nodes.back();
        return;
    }

    throw Exception("Cannot add step " + step->getName() + " to QueryPlan because it has " +
                    std::to_string(num_input_streams) + " inputs but " + std::to_string(isInitialized() ? 1 : 0) +
                    " input expected", ErrorCodes::LOGICAL_ERROR);
}

QueryPipelinePtr QueryPlan::buildQueryPipeline()
{
    checkInitialized();

    struct Frame
    {
        Node * node;
        QueryPipelines pipelines = {};
    };

    QueryPipelinePtr last_pipeline;

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (last_pipeline)
            frame.pipelines.emplace_back(std::move(last_pipeline));

        size_t next_child = frame.pipelines.size();
        if (next_child == frame.node->children.size())
        {
            bool limit_max_threads = frame.pipelines.empty();
            last_pipeline = frame.node->step->updatePipeline(std::move(frame.pipelines));

            if (limit_max_threads)
                last_pipeline->setMaxThreads(max_threads);

            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    return last_pipeline;
}

}
