#include <iostream>
#include <stack>
#include <Core/SortCursor.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryCoordination/Exchange/ExchangeDataStep.h>
#include <QueryCoordination/Fragments/Fragment.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

namespace DB
{

Fragment::Fragment(UInt32 fragment_id_, ContextMutablePtr context_)
    : fragment_id(fragment_id_), plan_id_counter(0), root(nullptr), dest_exchange_node(nullptr), dest_fragment_id(0), context(context_)
{
}

void Fragment::addStep(QueryPlanStepPtr step)
{
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

bool Fragment::isInitialized() const
{
    return root != nullptr;
} /// Tree is not empty


Fragment::Node Fragment::makeNewNode(QueryPlanStepPtr step, std::vector<PlanNode *> children_)
{
    Node node;
    node.step = step;
    node.plan_id = ++plan_id_counter;
    node.children = children_;
    return node;
}

const DataStream & Fragment::getCurrentDataStream() const
{
    return root->step->getOutputStream();
}

void Fragment::uniteFragments(QueryPlanStepPtr step, FragmentPtrs & fragments)
{
    if (isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite plans because current QueryPlan is already initialized");

    size_t num_inputs = step->getInputStreams().size();
    if (num_inputs != fragments.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unite fFragments using {} because step has different number of inputs. Has {} plans and {} inputs",
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
                "Cannot unite fFragments using {} because it has incompatible header with plan {} plan header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                plan_header.dumpStructure(),
                step_header.dumpStructure());
    }

    std::vector<PlanNode *> child_exchange_nodes;
    for (auto & fragment : fragments)
    {
        /// reset nodes plan_id
        for (auto & node : fragment->nodes)
        {
            node.plan_id = ++plan_id_counter;
            if (auto * exchange_step = typeid_cast<ExchangeDataStep *>(node.step.get()))
                exchange_step->setPlanID(node.plan_id);
        }

        nodes.splice(nodes.end(), std::move(fragment->nodes));

        for (auto & child_fragment : fragment->children)
        {
            /// update fragment_id
            auto * exchange_step = typeid_cast<ExchangeDataStep *>(child_fragment->dest_exchange_node->step.get());
            exchange_step->setFragmentId(fragment_id);

            child_fragment->setDestination(child_fragment->dest_exchange_node, shared_from_this());
        }
    }

    nodes.emplace_back(makeNewNode(step));
    root = &nodes.back();

    for (auto & fragment : fragments)
        root->children.emplace_back(fragment->root);
}

void Fragment::setDestination(Node * dest_exchange, FragmentPtr dest_fragment)
{
    dest_exchange_node = dest_exchange;
    dest_fragment_id = dest_fragment->fragment_id;
    dest_fragment->children.emplace_back(shared_from_this());

    dest_exchange->children.clear();
}

PlanNode * Fragment::getRoot() const
{
    return root;
}

const FragmentPtrs & Fragment::getChildren() const
{
    return children;
}

const Fragment::Nodes & Fragment::getNodes() const
{
    return nodes;
}

UInt32 Fragment::getFragmentID() const
{
    return fragment_id;
}

UInt32 Fragment::getDestFragmentID() const
{
    return dest_fragment_id;
}

bool Fragment::hasDestFragment() const
{
    return dest_exchange_node != nullptr;
}

UInt32 Fragment::getDestExchangeID() const
{
    return dest_exchange_node->plan_id;
}

const Fragment::Node * Fragment::getDestExchangeNode() const
{
    return dest_exchange_node;
}

QueryPipelineBuilderPtr Fragment::buildQueryPipeline(
    const QueryPlanOptimizationSettings & /*optimization_settings*/, const BuildQueryPipelineSettings & build_pipeline_settings)
{
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
        if (next_child == frame.node->children.size()) /// children belong next fragment
        {
            last_pipeline = frame.node->step->updatePipeline(std::move(frame.pipelines), build_pipeline_settings);

            stack.pop();
        }
        else
        {
            stack.push(Frame{.node = frame.node->children[next_child]});
        }
    }

    last_pipeline->setProgressCallback(build_pipeline_settings.progress_callback);
    last_pipeline->setProcessListElement(build_pipeline_settings.process_list_element);

    return last_pipeline;
}

QueryPipeline Fragment::buildQueryPipeline(std::vector<ExchangeDataSink::Channel> & channels, const String & local_host)
{
    auto builder
        = buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));

    if (hasDestFragment())
    {
        String query_id;
        if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
            query_id = context->getCurrentQueryId();
        else if (context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
            query_id = context->getInitialQueryId();

        auto * exchange_data_step = typeid_cast<ExchangeDataStep *>(dest_exchange_node->step.get());

        if (exchange_data_step->sinkMerge() && builder->getNumStreams() > 1)
        {
            auto transform = std::make_shared<MergingSortedTransform>(
                builder->getHeader(),
                builder->getNumStreams(),
                exchange_data_step->getSortDescription(),
                context->getSettings().max_block_size,
                /*max_block_size_bytes=*/0,
                SortingQueueStrategy::Batch,
                0,
                true);

            builder->addTransform(transform);
        }

        QueryPipeline pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

        auto sink = std::make_shared<ExchangeDataSink>(
            pipeline.getHeader(),
            channels,
            exchange_data_step->getDistribution(),
            local_host,
            query_id,
            getDestFragmentID(),
            dest_exchange_node->plan_id);

        pipeline.complete(sink);

        return pipeline;
    }

    return QueryPipelineBuilder::getPipeline(std::move(*builder));
}

static void
explainStep(const IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings, const Fragment::ExplainFragmentOptions & options)
{
    std::string prefix(settings.offset, ' ');
    settings.out << prefix;
    settings.out << step.getName();

    const auto & description = step.getStepDescription();
    if (options.description && !description.empty())
        settings.out << " (" << description << ')';

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

void Fragment::dump(WriteBufferFromOwnString & buffer, const ExplainFragmentOptions & settings)
{
    buffer.write('\n');
    std::string str("Fragment " + std::to_string(fragment_id));

    if (dest_exchange_node)
    {
        str += ", Data to:";
        str += std::to_string(dest_fragment_id);
    }
    buffer.write(str.c_str(), str.size());
    buffer.write('\n');

    explainPlan(buffer, settings);

    for (const auto & child_fragment : children)
        child_fragment->dump(buffer, settings);
}

void Fragment::explainPlan(WriteBuffer & buffer, const ExplainFragmentOptions & options)
{
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
        all_nodes.insert(&node);

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
            if (all_nodes.contains(frame.node->children[frame.next_child]))
                stack.push(Frame{frame.node->children[frame.next_child]});

            ++frame.next_child;
        }
        else
            stack.pop();
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

void Fragment::explainPipeline(WriteBuffer & buffer, bool show_header)
{
    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = show_header};

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

}
