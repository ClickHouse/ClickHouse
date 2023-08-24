#include <QueryCoordination/NewOptimizer/SubQueryPlan.h>
#include <Common/JSONBuilder.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>


namespace DB
{

void SubQueryPlan::addStep(QueryPlanStepPtr step)
{
    if (root)
    {
        const auto & root_header = root->step->getOutputStream().header;

        if (!step->getInputStreams().empty())
        {
            const auto & step_header = step->getInputStreams().front().header;
            if (!blocksHaveEqualStructure(root_header, step_header))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot add step {} to QueryPlan because it has incompatible header with root step {} root header: {} step header: {}",
                    step->getName(),
                    root->step->getName(),
                    root_header.dumpStructure(),
                    step_header.dumpStructure());
        }

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}});
        root = &nodes.back();
        return;
    }
    else
    {
        nodes.emplace_back(Node{.step = std::move(step)});
        root = &nodes.back();
        return;
    }
}

const DataStream & SubQueryPlan::getCurrentDataStream() const
{
    return root->step->getOutputStream();
}

void SubQueryPlan::unitePlans(QueryPlanStepPtr step, std::vector<std::unique_ptr<SubQueryPlan>> plans)
{
    const auto & inputs = step->getInputStreams();
    size_t num_inputs = step->getInputStreams().size();
    if (num_inputs != plans.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unite QueryPlans using {} because step has different number of inputs. Has {} plans and {} inputs",
            step->getName(),
            plans.size(),
            num_inputs);

    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i].header;
        const auto & plan_header = plans[i]->getCurrentDataStream().header;
        if (!blocksHaveEqualStructure(step_header, plan_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot unite QueryPlans using {} because it has incompatible header with plan {} plan header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                plan_header.dumpStructure(),
                step_header.dumpStructure());
    }

    for (auto & plan : plans)
        nodes.splice(nodes.end(), std::move(plan->nodes));

    nodes.emplace_back(Node{.step = std::move(step)});
    root = &nodes.back();

    for (auto & plan : plans)
        root->children.emplace_back(plan->root);
}

static void explainStep(
    const IQueryPlanStep & step,
    IQueryPlanStep::FormatSettings & settings,
    const SubQueryPlan::ExplainPlanOptions & options)
{
    std::string prefix(settings.offset, ' ');
    settings.out << prefix;
    settings.out << step.getName();

    const auto & description = step.getStepDescription();
    if (options.description && !description.empty())
        settings.out <<" (" << description << ')';

    settings.out.write('\n');

    if (options.header)
    {
        settings.out << prefix;

        if (!step.hasOutputStream())
            settings.out << "No header";
        else if (!step.getOutputStream().header)
            settings.out << "Empty header";
        else
        {
            settings.out << "Header: ";
            bool first = true;

            for (const auto & elem : step.getOutputStream().header)
            {
                if (!first)
                    settings.out << "\n" << prefix << "        ";

                first = false;
                elem.dumpNameAndType(settings.out);
            }
        }
        settings.out.write('\n');

    }

    if (options.sorting)
    {
        if (step.hasOutputStream())
        {
            settings.out << prefix << "Sorting (" << step.getOutputStream().sort_scope << ")";
            if (step.getOutputStream().sort_scope != DataStream::SortScope::None)
            {
                settings.out << ": ";
                dumpSortDescription(step.getOutputStream().sort_description, settings.out);
            }
            settings.out.write('\n');
        }
    }

    if (options.actions)
        step.describeActions(settings);

    if (options.indexes)
        step.describeIndexes(settings);
}

void SubQueryPlan::explainPlan(WriteBuffer & buffer, const SubQueryPlan::ExplainPlanOptions & options) const
{
    buffer.write('\n');

    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        bool is_description_printed = false;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (!frame.is_description_printed)
        {
            settings.offset = (stack.size() - 1) * settings.indent;
            explainStep(*frame.node->step, settings, options);
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

}
