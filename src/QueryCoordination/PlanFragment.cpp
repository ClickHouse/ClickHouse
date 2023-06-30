#include <QueryCoordination/PlanFragment.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlanVisitor.h>
#include <Common/JSONBuilder.h>
#include <Interpreters/Context.h>
#include <memory>


namespace DB
{


void PlanFragment::unitePlanFragments(QueryPlanStepPtr step, std::vector<std::shared_ptr<PlanFragment>> fragments, StorageLimitsList storage_limits_)
{
    if (isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite plans because current QueryPlan is already initialized");

    size_t num_inputs = step->getInputStreams().size();
    if (num_inputs != fragments.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unite QueryPlans using {} because step has different number of inputs. Has {} plans and {} inputs",
            step->getName(),
            fragments.size(),
            num_inputs);

    const auto & inputs = step->getInputStreams();
    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i].header;
        const auto & plan_header = fragments[i]->getCurrentDataStream().header;
        if (!blocksHaveEqualStructure(step_header, plan_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot unite PlanFragments using {} because it has incompatible header with plan {} plan header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                plan_header.dumpStructure(),
                step_header.dumpStructure());
    }

    std::vector<PlanNode *> child_exchange_nodes;
    for (auto & fragment : fragments)
    {
        auto exchange_step = std::make_shared<ExchangeDataStep>(fragment->getCurrentDataStream(), storage_limits_);
        nodes.emplace_back(makeNewNode(exchange_step, {fragment->getRootNode()}));

        exchange_step->setPlanID(nodes.back().plan_id);

        fragment->setDestination(&nodes.back());
        fragment->setOutputPartition(data_partition);

        child_exchange_nodes.emplace_back(&nodes.back());
    }

    nodes.emplace_back(makeNewNode(step, child_exchange_nodes));
    root = &nodes.back();

    for (auto & fragment : fragments)
    {
        max_threads = std::max(max_threads, fragment->max_threads);
        resources = std::move(fragment->resources);
    }

    setFragmentInPlanTree(root);
    setCluster(fragments[0]->getCluster());
}


void PlanFragment::unitePlanFragments(
    QueryPlanStepPtr root_step,
    std::vector<QueryPlanStepPtr> child_steps,
    std::vector<std::shared_ptr<PlanFragment>> child_fragments,
    StorageLimitsList storage_limits_)
{
    size_t num_inputs = root_step->getInputStreams().size();
    if ((num_inputs - 1) != child_fragments.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unite PlanFragments using {} because step has different number of inputs. Has {} plans and {} inputs",
            root_step->getName(),
            child_fragments.size(),
            (num_inputs - 1));

    const auto & inputs = root_step->getInputStreams();
    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i].header;

        Block plan_header;
        if (i == 0) /// Most left
        {
            plan_header = nodes.back().step->getOutputStream().header;
        }
        else
        {
            plan_header = child_steps[i - 1]->getOutputStream().header;
        }

        if (!blocksHaveEqualStructure(step_header, plan_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot unite PlanFragments using {} because it has incompatible header with plan {} plan header: {} step header: {}",
                root_step->getName(),
                root->step->getName(),
                plan_header.dumpStructure(),
                step_header.dumpStructure());
    }

    std::vector<PlanNode *> child_nodes{&nodes.back()};
    for (size_t i = 0; i < child_fragments.size(); ++i)
    {
        auto & fragment = child_fragments[i];
        auto exchange_step = std::make_shared<ExchangeDataStep>(fragment->getCurrentDataStream(), storage_limits_);
        nodes.emplace_back(makeNewNode(exchange_step, {fragment->getRootNode()}));

        exchange_step->setPlanID(nodes.back().plan_id);

        fragment->setDestination(&nodes.back());
        fragment->setOutputPartition(data_partition);

        nodes.emplace_back(makeNewNode(child_steps[i], {&nodes.back()}));

        child_nodes.emplace_back(&nodes.back());

        max_threads = std::max(max_threads, fragment->max_threads);
        resources = std::move(fragment->resources);
    }

    nodes.emplace_back(makeNewNode(root_step, child_nodes));
    root = &nodes.back();

    setFragmentInPlanTree(root);
    setCluster(child_fragments[0]->getCluster());
}


void PlanFragment::addStep(QueryPlanStepPtr step)
{
    checkNotCompleted();

    size_t num_input_streams = step->getInputStreams().size();

    if (num_input_streams == 0)
    {
        if (isInitialized())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlan because step has no inputs, but QueryPlan is already initialized",
                step->getName());

        nodes.emplace_back(makeNewNode(step));
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

        const auto & root_header = root->step->getOutputStream().header;
        const auto & step_header = step->getInputStreams().front().header;
        if (!blocksHaveEqualStructure(root_header, step_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlan because it has incompatible header with root step {} root header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                root_header.dumpStructure(),
                step_header.dumpStructure());

        nodes.emplace_back(makeNewNode(step, {root}));
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

bool PlanFragment::isCompleted() const
{
    return isInitialized() && !root->step->hasOutputStream();
}

const DataStream & PlanFragment::getCurrentDataStream() const
{
    checkInitialized();
    checkNotCompleted();
    return root->step->getOutputStream();
}


void PlanFragment::checkInitialized() const
{
    if (!isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlan was not initialized");
}

void PlanFragment::checkNotCompleted() const
{
    if (isCompleted())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlan was already completed");
}


static void updateDataStreams(QueryPlan::Node & root)
{
    class UpdateDataStreams : public QueryPlanVisitor<UpdateDataStreams, false>
    {
    public:
        explicit UpdateDataStreams(QueryPlan::Node * root_) : QueryPlanVisitor<UpdateDataStreams, false>(root_) { }

        static bool visitTopDownImpl(QueryPlan::Node * /*current_node*/, QueryPlan::Node * /*parent_node*/) { return true; }

        static void visitBottomUpImpl(QueryPlan::Node * current_node, QueryPlan::Node * parent_node)
        {
            if (!parent_node || parent_node->children.size() != 1)
                return;

            if (!current_node->step->hasOutputStream())
                return;

            if (auto * parent_transform_step = dynamic_cast<ITransformingStep *>(parent_node->step.get()); parent_transform_step)
                parent_transform_step->updateInputStream(current_node->step->getOutputStream());
        }
    };

    UpdateDataStreams(&root).visit();
}

void PlanFragment::optimize(const QueryPlanOptimizationSettings & optimization_settings)
{
    /// optimization need to be applied before "mergeExpressions" optimization
    /// it removes redundant sorting steps, but keep underlying expressions,
    /// so "mergeExpressions" optimization handles them afterwards
    if (optimization_settings.remove_redundant_sorting)
        QueryPlanOptimizations::tryRemoveRedundantSorting(root);

    QueryPlanOptimizations::optimizeTreeFirstPass(optimization_settings, *root, nodes);
    QueryPlanOptimizations::optimizeTreeSecondPass(optimization_settings, *root, nodes);

    updateDataStreams(*root);
}

QueryPipelineBuilderPtr PlanFragment::buildQueryPipeline(
    const QueryPlanOptimizationSettings & optimization_settings,
    const BuildQueryPipelineSettings & build_pipeline_settings)
{
    checkInitialized();
    optimize(optimization_settings);

    struct Frame
    {
        Node * node = {};
        QueryPipelineBuilders pipelines = {};
    };

    QueryPipelineBuilderPtr last_pipeline;

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    std::unordered_set<Node *> all_nodes;
    for (auto & node : nodes)
    {
        all_nodes.insert(&node);
    }

    while (!stack.empty() && all_nodes.contains(stack.top().node))
    {
        auto & frame = stack.top();

        if (last_pipeline)
        {
            frame.pipelines.emplace_back(std::move(last_pipeline));
            last_pipeline = nullptr;
        }

        size_t next_child = frame.pipelines.size();
        if (next_child == frame.node->children.size() || !all_nodes.contains(frame.node->children[next_child])) /// children belong next fragment
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

    return last_pipeline;
}

static void explainStep(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const PlanFragment::ExplainPlanOptions & options)
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

JSONBuilder::ItemPtr PlanFragment::explainPlan(const ExplainPlanOptions & options)
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


static void explainStep(
    const IQueryPlanStep & step,
    IQueryPlanStep::FormatSettings & settings,
    const PlanFragment::ExplainPlanOptions & options)
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

void PlanFragment::explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options)
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

    std::unordered_set<Node *> all_nodes;
    for (auto & node : nodes)
    {
        all_nodes.insert(&node);
    }

    while (!stack.empty() && all_nodes.contains(stack.top().node))
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

void PlanFragment::explainPipeline(WriteBuffer & /*buffer*/, const ExplainPipelineOptions & /*options*/)
{

}

void PlanFragment::explainEstimate(MutableColumns & /*columns*/)
{

}

QueryPipeline PlanFragment::buildQueryPipeline(std::vector<DataSink::Channel> & channels, const String & local_host)
{
    auto builder = buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));

    QueryPipeline pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    if (auto dest_fragment = getDestFragment())
    {
        String query_id;
        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        {
            query_id = context->getCurrentQueryId();
        }
        else if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        {
            query_id = context->getInitialQueryId();
        }
        auto sink = std::make_shared<DataSink>(
            pipeline.getHeader(), channels, output_partition, local_host, query_id, dest_fragment->getFragmentId(), dest_node->plan_id);

        pipeline.complete(sink);
    }

    std::shared_ptr<const EnabledQuota> quota = context->getQuota();

    pipeline.setQuota(quota);

    return pipeline;
}

}
