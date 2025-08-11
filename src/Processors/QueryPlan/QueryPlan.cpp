#include <stack>

#include <Common/JSONBuilder.h>

#include <IO/Operators.h>
#include <IO/WriteBuffer.h>

#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/QueryPlanVisitor.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Planner/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SettingsChanges ExplainPlanOptions::toSettingsChanges() const
{
    SettingsChanges changes;
    changes.emplace_back("header", int(header));
    changes.emplace_back("description", int(description));
    changes.emplace_back("actions", int(actions));
    changes.emplace_back("indexes", int(indexes));
    changes.emplace_back("projections", int(projections));
    changes.emplace_back("sorting", int(sorting));
    changes.emplace_back("distributed", int(distributed));

    return changes;
}

QueryPlan::QueryPlan() = default;
QueryPlan::~QueryPlan() = default;
QueryPlan::QueryPlan(QueryPlan &&) noexcept = default;
QueryPlan & QueryPlan::operator=(QueryPlan &&) noexcept = default;

void QueryPlan::checkInitialized() const
{
    if (!isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlan was not initialized");
}

void QueryPlan::checkNotCompleted() const
{
    if (isCompleted())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlan was already completed");
}

bool QueryPlan::isCompleted() const
{
    return isInitialized() && !root->step->hasOutputHeader();
}

const Header & QueryPlan::getCurrentHeader() const
{
    checkInitialized();
    checkNotCompleted();
    return root->step->getOutputHeader();
}

void QueryPlan::unitePlans(QueryPlanStepPtr step, std::vector<std::unique_ptr<QueryPlan>> plans)
{
    if (isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite plans because current QueryPlan is already initialized");

    const auto & inputs = step->getInputHeaders();
    size_t num_inputs = step->getInputHeaders().size();
    if (num_inputs != plans.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unite QueryPlans using {} because step has different number of inputs. Has {} plans and {} inputs",
            step->getName(),
            plans.size(),
            num_inputs);

    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i];
        const auto & plan_header = plans[i]->getCurrentHeader();
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

    for (auto & plan : plans)
    {
        max_threads = std::max(max_threads, plan->max_threads);
        resources = std::move(plan->resources);
    }
}

void QueryPlan::addStep(QueryPlanStepPtr step)
{
    checkNotCompleted();

    size_t num_input_streams = step->getInputHeaders().size();

    if (num_input_streams == 0)
    {
        if (isInitialized())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlan because step has no inputs, but QueryPlan is already initialized",
                step->getName());

        nodes.emplace_back(Node{.step = std::move(step)});
        root = &nodes.back();
        return;
    }

    if (num_input_streams == 1)
    {
        if (!isInitialized())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlan because step has input, but QueryPlan is not initialized",
                step->getName());

        const auto & root_header = root->step->getOutputHeader();
        const auto & step_header = step->getInputHeaders().front();
        if (!blocksHaveEqualStructure(root_header, step_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlan because it has incompatible header with root step {} root header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                root_header.dumpStructure(),
                step_header.dumpStructure());

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}});
        root = &nodes.back();
        return;
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot add step {} to QueryPlan because it has {} inputs but {} input expected",
        step->getName(),
        num_input_streams,
        isInitialized() ? 1 : 0);
}

QueryPipelineBuilderPtr QueryPlan::buildQueryPipeline(
    const QueryPlanOptimizationSettings & optimization_settings,
    const BuildQueryPipelineSettings & build_pipeline_settings,
    bool do_optimize)
{
    checkInitialized();
    if (do_optimize)
        optimize(optimization_settings);

    struct Frame
    {
        Node * node = {};
        QueryPipelineBuilders pipelines = {};
    };

    QueryPipelineBuilderPtr last_pipeline;

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (last_pipeline)
        {
            frame.pipelines.emplace_back(std::move(last_pipeline));
            last_pipeline = nullptr;
        }

        size_t next_child = frame.pipelines.size();
        if (next_child == frame.node->children.size())
        {
            bool limit_max_threads = frame.pipelines.empty();
            last_pipeline = frame.node->step->updatePipeline(std::move(frame.pipelines), build_pipeline_settings);

            if (limit_max_threads && max_threads)
                last_pipeline->limitMaxThreads(max_threads);

            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    last_pipeline->setProgressCallback(build_pipeline_settings.progress_callback);
    last_pipeline->setProcessListElement(build_pipeline_settings.process_list_element);
    last_pipeline->addResources(std::move(resources));
    last_pipeline->setConcurrencyControl(getConcurrencyControl());

    return last_pipeline;
}

static void explainStep(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const ExplainPlanOptions & options)
{
    map.add("Node Type", step.getName());
    map.add("Node Id", step.getUniqID());

    if (options.description)
    {
        const auto & description = step.getStepDescription();
        if (!description.empty())
            map.add("Description", description);
    }

    if (options.header && step.hasOutputHeader())
    {
        auto header_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & output_column : step.getOutputHeader())
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

    if (options.projections)
        step.describeProjections(map);
}

JSONBuilder::ItemPtr QueryPlan::explainPlan(const ExplainPlanOptions & options) const
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
            auto child_plans = frame.node->step->getChildPlans();

            if (!frame.children_array && !child_plans.empty())
                frame.children_array = std::make_unique<JSONBuilder::JSONArray>();

            for (const auto & child_plan : child_plans)
                frame.children_array->add(child_plan->explainPlan(options));

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

static void explainStep(
    IQueryPlanStep & step,
    IQueryPlanStep::FormatSettings & settings,
    const ExplainPlanOptions & options)
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

        if (!step.hasOutputHeader())
            settings.out << "No header";
        else if (!step.getOutputHeader())
            settings.out << "Empty header";
        else
        {
            settings.out << "Header: ";
            bool first = true;

            for (const auto & elem : step.getOutputHeader())
            {
                if (!first)
                    settings.out << "\n" << prefix << "        ";

                first = false;
                elem.dumpNameAndType(settings.out);
                if (elem.column && isColumnLazy(*elem.column.get()))
                    settings.out << " (Lazy)";
            }
        }
        settings.out.write('\n');

    }

    if (options.sorting)
    {
        if (const auto & sort_description = step.getSortDescription(); !sort_description.empty())
        {
            settings.out << prefix << "Sorting: ";
            dumpSortDescription(sort_description, settings.out);
            settings.out.write('\n');
        }
    }

    if (options.actions)
        step.describeActions(settings);

    if (options.indexes)
        step.describeIndexes(settings);

    if (options.projections)
        step.describeProjections(settings);

    if (options.distributed)
        step.describeDistributedPlan(settings, options);
}

std::string debugExplainStep(IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    ExplainPlanOptions options{.actions = true};
    IQueryPlanStep::FormatSettings settings{.out = out};
    explainStep(step, settings, options);
    return out.str();
}

void QueryPlan::explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options, size_t indent) const
{
    checkInitialized();

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
            settings.offset = (indent + stack.size() - 1) * settings.indent;
            explainStep(*frame.node->step, settings, options);
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
        else
        {
            auto child_plans = frame.node->step->getChildPlans();

            for (const auto & child_plan : child_plans)
                child_plan->explainPlan(buffer, options, indent + stack.size());

            stack.pop();
        }
    }
}

static void explainPipelineStep(IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings)
{
    settings.out << String(settings.offset, settings.indent_char) << "(" << step.getName() << ")\n";

    size_t current_offset = settings.offset;
    step.describePipeline(settings);
    if (current_offset == settings.offset)
        settings.offset += settings.indent;
}

void QueryPlan::explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options) const
{
    checkInitialized();

    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        size_t offset = 0;
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
            settings.offset = frame.offset;
            explainPipelineStep(*frame.node->step, settings);
            frame.offset = settings.offset;
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child], frame.offset});
            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

void QueryPlan::optimize(const QueryPlanOptimizationSettings & optimization_settings)
{
    /// optimization need to be applied before "mergeExpressions" optimization
    /// it removes redundant sorting steps, but keep underlying expressions,
    /// so "mergeExpressions" optimization handles them afterwards
    if (optimization_settings.remove_redundant_sorting)
        QueryPlanOptimizations::tryRemoveRedundantSorting(root);

    QueryPlanOptimizations::optimizeTreeFirstPass(optimization_settings, *root, nodes);
    QueryPlanOptimizations::optimizeTreeSecondPass(optimization_settings, *root, nodes);
    if (optimization_settings.build_sets)
        QueryPlanOptimizations::addStepsToBuildSets(optimization_settings, *this, *root, nodes);
}

void QueryPlan::explainEstimate(MutableColumns & columns) const
{
    checkInitialized();

    struct EstimateCounters
    {
        std::string database_name;
        std::string table_name;
        UInt64 parts = 0;
        UInt64 rows = 0;
        UInt64 marks = 0;
    };

    using CountersPtr = std::shared_ptr<EstimateCounters>;
    std::unordered_map<std::string, CountersPtr> counters;
    using processNodeFuncType = std::function<void(const Node * node)>;
    processNodeFuncType process_node = [&counters, &process_node] (const Node * node)
    {
        if (!node)
            return;
        if (const auto * step = dynamic_cast<ReadFromMergeTree*>(node->step.get()))
        {
            const auto & id = step->getStorageID();
            auto key = id.database_name + "." + id.table_name;
            auto it = counters.find(key);
            if (it == counters.end())
            {
                it = counters.insert({key, std::make_shared<EstimateCounters>(id.database_name, id.table_name)}).first;
            }
            it->second->parts += step->getSelectedParts();
            it->second->rows += step->getSelectedRows();
            it->second->marks += step->getSelectedMarks();
        }
        for (const auto * child : node->children)
            process_node(child);
    };
    process_node(root);

    for (const auto & counter : counters)
    {
        size_t index = 0;
        const auto & database_name = counter.second->database_name;
        const auto & table_name = counter.second->table_name;
        columns[index++]->insertData(database_name.c_str(), database_name.size());
        columns[index++]->insertData(table_name.c_str(), table_name.size());
        columns[index++]->insert(counter.second->parts);
        columns[index++]->insert(counter.second->rows);
        columns[index++]->insert(counter.second->marks);
    }
}

// static void validatePlan(QueryPlan::Node * root, QueryPlan::Nodes & nodes)
// {
//     std::unordered_set<const QueryPlan::Node *> used;
//     std::stack<const QueryPlan::Node *> stack;

//     std::unordered_set<const QueryPlan::Node *> known;
//     for (const auto & node : nodes)
//         known.emplace(&node);

//     stack.push(root);
//     while (!stack.empty())
//     {
//         const auto * node = stack.top();
//         used.insert(node);
//         stack.pop();

//         if (!known.contains(node))
//             throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} {} is not known", node->step->getName(), reinterpret_cast<const void *>(node));

//         for (auto * child : node->children)
//         {
//             stack.push(child);
//         }
//     }

//     for (const auto * node : known)
//         if (!used.contains(node))
//             throw Exception(ErrorCodes::LOGICAL_ERROR, "Node {} {} is not used", node->step->getName(), reinterpret_cast<const void *>(node));
// }

QueryPlan QueryPlan::extractSubplan(Node * root, Nodes & nodes)
{
    std::unordered_set<Node *> used;
    std::stack<Node *> stack;

    stack.push(root);
    used.insert(root);
    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();

        for (auto * child : node->children)
        {
            used.insert(child);
            stack.push(child);
        }
    }

    QueryPlan new_plan;
    new_plan.root = root;

    auto it = nodes.begin();
    while (it != nodes.end())
    {
        auto curr = it;
        ++it;

        if (used.contains(&*curr))
            new_plan.nodes.splice(new_plan.nodes.end(), nodes, curr);
    }

    // {
    //     WriteBufferFromOwnString buf;
    //     new_plan.explainPlan(buf, {.header=true, .actions=true});
    //     std::cerr << buf.stringView() << std::endl;
    // }

    // validatePlan(new_plan.root, new_plan.nodes);

    return new_plan;
}

std::pair<QueryPlan::Nodes, QueryPlanResourceHolder> QueryPlan::detachNodesAndResources(QueryPlan && plan)
{
    return {std::move(plan.nodes), std::move(plan.resources)};
}

QueryPlan QueryPlan::extractSubplan(Node * subplan_root)
{
    std::unordered_set<Node *> used;
    std::stack<Node *> stack;

    stack.push(subplan_root);
    used.insert(subplan_root);
    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();

        for (auto * child : node->children)
        {
            used.insert(child);
            stack.push(child);
        }
    }

    QueryPlan new_plan;
    new_plan.root = subplan_root;

    auto it = nodes.begin();
    while (it != nodes.end())
    {
        auto curr = it;
        ++it;

        if (used.contains(&*curr))
            new_plan.nodes.splice(new_plan.nodes.end(), nodes, curr);
    }

    return new_plan;
}

QueryPlan QueryPlan::clone() const
{
    QueryPlan result;

    struct Frame
    {
        Node * node;
        Node * clone;
        std::vector<Node *> children = {};
    };

    result.nodes.emplace_back(Node{ .step = {}, .children = {} });
    result.root = &result.nodes.back();

    std::vector<Frame> nodes_to_process{ Frame{ .node = root, .clone = result.root } };

    while (!nodes_to_process.empty())
    {
        auto & frame = nodes_to_process.back();
        if (frame.children.size() == frame.node->children.size())
        {
            frame.clone->step = frame.node->step->clone();
            frame.clone->children = std::move(frame.children);
            nodes_to_process.pop_back();
        }
        else
        {
            size_t next_child = frame.children.size();
            auto * child = frame.node->children[next_child];

            result.nodes.emplace_back(Node{ .step = {} });
            result.nodes.back().children.reserve(child->children.size());
            auto * child_clone = &result.nodes.back();

            frame.children.push_back(child_clone);

            nodes_to_process.push_back(Frame{ .node = child, .clone = child_clone });
        }
    }

    return result;
}

}
