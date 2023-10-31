#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <QueryCoordination/Optimizer/StepTree.h>
#include <Common/JSONBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void StepTree::checkInitialized() const
{
    if (!isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StepTree was not initialized");
}

void StepTree::checkNotCompleted() const
{
    if (isCompleted())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StepTree was already completed");
}

bool StepTree::isCompleted() const
{
    return isInitialized() && !root->step->hasOutputStream();
}

void StepTree::addStep(QueryPlanStepPtr step)
{
    checkNotCompleted();

    if (root)
    {
        const auto & root_header = root->step->getOutputStream().header;

        if (!step->getInputStreams().empty())
        {
            const auto & step_header = step->getInputStreams().front().header;
            if (!blocksHaveEqualStructure(root_header, step_header))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot add step {} to StepTree because it has incompatible header with root step {} root header: {} step header: {}",
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

const DataStream & StepTree::getCurrentDataStream() const
{
    checkInitialized();
    checkNotCompleted();
    return root->step->getOutputStream();
}

void StepTree::unitePlans(QueryPlanStepPtr step, std::vector<std::shared_ptr<StepTree>> & plans)
{
    if (isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite step tree because current QueryPlan is already initialized");

    const auto & inputs = step->getInputStreams();
    size_t num_inputs = step->getInputStreams().size();
    if (num_inputs != plans.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unite StepTree using {} because step has different number of inputs. Has {} plans and {} inputs",
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
                "Cannot unite StepTrees using {} because it has incompatible header with plan {} plan header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                plan_header.dumpStructure(),
                step_header.dumpStructure());
    }

    for (auto & plan : plans)
        nodes.splice(nodes.end(), plan->nodes);

    nodes.emplace_back(Node{.step = step});
    root = &nodes.back();

    for (auto & plan : plans)
        root->children.emplace_back(plan->root);
}

static void explainStep(
    const IQueryPlanStep & step,
    IQueryPlanStep::FormatSettings & settings,
    const StepTree::ExplainPlanOptions & options)
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

static void explainStep(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const StepTree::ExplainPlanOptions & options)
{
    map.add("Node Type", step.getName());

    if (options.description)
    {
        const auto & description = step.getStepDescription();
        if (!description.empty())
            map.add("Description", description);
    }

    if (options.header && step.hasOutputStream())
    {
        auto header_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & output_column : step.getOutputStream().header)
        {
            auto column_map = std::make_unique<JSONBuilder::JSONMap>();
            column_map->add("Name", output_column.name);
            if (output_column.type)
                column_map->add("Type", output_column.type->getName());

            header_array->add(std::move(column_map));
        }

        map.add("Header", std::move(header_array));
    }

    if (options.actions)
        step.describeActions(map);

    if (options.indexes)
        step.describeIndexes(map);
}

JSONBuilder::ItemPtr StepTree::explainPlan(const StepTree::ExplainPlanOptions & options)
{
    checkInitialized();
    struct Frame
    {
        Node * node = {};
        size_t next_child = 0;
        std::unique_ptr<JSONBuilder::JSONMap> node_map = {};
        std::unique_ptr<JSONBuilder::JSONArray> children_array = {};
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    std::unique_ptr<JSONBuilder::JSONMap> tree;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (frame.next_child == 0)
        {
            if (!frame.node->children.empty())
                frame.children_array = std::make_unique<JSONBuilder::JSONArray>();

            frame.node_map = std::make_unique<JSONBuilder::JSONMap>();
            explainStep(*frame.node->step, *frame.node_map, options);
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
        else
        {
            if (frame.children_array)
                frame.node_map->add("Plans", std::move(frame.children_array));

            tree.swap(frame.node_map);
            stack.pop();

            if (!stack.empty())
                stack.top().children_array->add(std::move(tree));
        }
    }

    return tree;
}

void StepTree::explainPlan(WriteBuffer & buffer, const StepTree::ExplainPlanOptions & options) const
{
    checkInitialized();
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
