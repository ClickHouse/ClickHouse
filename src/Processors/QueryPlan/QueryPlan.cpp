#include <algorithm>
#include <memory>
#include <stack>

#include <Common/CurrentThread.h>
#include <Common/JSONBuilder.h>
#include <Common/logger_useful.h>

#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>

#include <Processors/ConcatProcessor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/GatherSendStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/QueryPlanVisitor.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Sources/ReadFromDistributedPlanSource.h>

#include <QueryPipeline/DistributedPlanExecutor.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Planner/Utils.h>

namespace ProfileEvents
{
    extern const Event QueryPlanOptimizeMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// A stage fragment is shipped to workers by serializing its query plan, so every step must support
/// serialization. Check up front (without serializing) so an unsupported plan fails early with a clear
/// message instead of late, mid-execution, with a generic error.
void assertFragmentSerializable(const QueryPlan & fragment, const String & stage_name)
{
    std::vector<const QueryPlan::Node *> stack;
    if (fragment.getRootNode())
        stack.push_back(fragment.getRootNode());
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (node->step && !node->step->isSerializable())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "make_distributed_plan cannot distribute this query: step '{}' in stage '{}' is not "
                "serializable for remote execution", node->step->getName(), stage_name);
        for (const auto * child : node->children)
            stack.push_back(child);
    }
}

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
    changes.emplace_back("input_headers", int(input_headers));
    changes.emplace_back("column_structure", int(column_structure));
    changes.emplace_back("pretty", int(pretty));
    changes.emplace_back("compact", int(compact));

    return changes;
}

QueryPlan::QueryPlan() = default;
QueryPlan::~QueryPlan() = default;
QueryPlan::QueryPlan(QueryPlan &&) noexcept = default;
QueryPlan & QueryPlan::operator=(QueryPlan &&) = default; /// NOLINT(hicpp-noexcept-move,performance-noexcept-move-constructor)

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

const SharedHeader & QueryPlan::getCurrentHeader() const
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
        if (!blocksHaveEqualStructure(*step_header, *plan_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot unite QueryPlans using {} because it has incompatible header with plan {} plan header: {} step header: {}",
                step->getName(),
                plans[i]->root->step->getName(),
                plan_header->dumpStructure(),
                step_header->dumpStructure());
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
        if (!blocksHaveEqualStructure(*root_header, *step_header))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot add step {} to QueryPlan because it has incompatible header with root step {} root header: {} step header: {}",
                step->getName(),
                root->step->getName(),
                root_header->dumpStructure(),
                step_header->dumpStructure());

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

    if (optimization_settings.make_distributed_plan)
        convertToDistributed(optimization_settings);

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
    last_pipeline->addResources(resources);
    last_pipeline->setConcurrencyControl(getConcurrencyControl());

    return last_pipeline;
}

static void formatIndexes(const IndexesDescription & desc, IQueryPlanStep::FormatSettings & format_settings)
{
    const auto & index_stats = desc.index_stats;

    if (index_stats.empty())
        return;

    if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
        return;

    const std::string & prefix = format_settings.detail_prefix;
    std::string indent(format_settings.base_indent, format_settings.indent_char);
    format_settings.out << prefix << "Indexes:\n";

    for (size_t i = 0; i < index_stats.size(); ++i)
    {
        const auto & stat = index_stats[i];
        if (stat.type == IndexType::None)
            continue;

        format_settings.out << prefix << indent << indexTypeToString(stat.type) << '\n';

        if (!stat.name.empty())
            format_settings.out << prefix << indent << indent << "Name: " << stat.name << '\n';

        if (!stat.description.empty())
            format_settings.out << prefix << indent << indent << "Description: " << stat.description << '\n';

        if (!stat.used_keys.empty())
        {
            format_settings.out << prefix << indent << indent << "Keys:" << '\n';
            for (const auto & used_key : stat.used_keys)
                format_settings.out << prefix << indent << indent << indent << used_key << '\n';
        }

        if (!stat.condition.empty())
            format_settings.out << prefix << indent << indent << "Condition: " << stat.condition << '\n';

        format_settings.out << prefix << indent << indent << "Parts: " << stat.num_parts_after;
        if (i)
            format_settings.out << '/' << index_stats[i - 1].num_parts_after;
        format_settings.out << '\n';

        format_settings.out << prefix << indent << indent << "Granules: " << stat.num_granules_after;
        if (i)
            format_settings.out << '/' << index_stats[i - 1].num_granules_after;
        format_settings.out << '\n';

        auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
        if (!search_algorithm.empty())
            format_settings.out << prefix << indent << indent << "Search Algorithm: " << search_algorithm << "\n";

        if (!stat.distributed.empty())
        {
            format_settings.out << prefix << indent << indent << "Distributed:" << '\n';

            if (format_settings.compact)
            {
                size_t total_parts_send = 0;
                size_t total_parts_received = 0;
                size_t total_granules_send = 0;
                size_t total_granules_received = 0;
                for (const auto & node_stat : stat.distributed)
                {
                    total_parts_send += node_stat.num_parts_send;
                    total_parts_received += node_stat.num_parts_received;
                    total_granules_send += node_stat.num_granules_send;
                    total_granules_received += node_stat.num_granules_received;
                }
                format_settings.out << prefix << indent << indent << indent << "Replicas: " << stat.distributed.size() << '\n';
                format_settings.out << prefix << indent << indent << indent << "Parts send: " << total_parts_send << '\n';
                format_settings.out << prefix << indent << indent << indent << "Parts received: " << total_parts_received << '\n';
                format_settings.out << prefix << indent << indent << indent << "Granules send: " << total_granules_send << '\n';
                format_settings.out << prefix << indent << indent << indent << "Granules received: " << total_granules_received << '\n';
            }
            else
            {
                for (const auto & node_stat : stat.distributed)
                {
                    format_settings.out << prefix << indent << indent << indent << "Address: " << node_stat.address << '\n';
                    format_settings.out << prefix << indent << indent << indent << "Parts send: " << node_stat.num_parts_send << '\n';
                    format_settings.out << prefix << indent << indent << indent << "Parts received: " << node_stat.num_parts_received << '\n';
                    format_settings.out << prefix << indent << indent << indent << "Granules send: " << node_stat.num_granules_send << '\n';
                    format_settings.out << prefix << indent << indent << indent << "Granules received: " << node_stat.num_granules_received << '\n';
                }
            }
        }
    }

    if (desc.tables_count > 1)
        format_settings.out << prefix << indent << "Tables: " << desc.tables_count << '\n';
    format_settings.out << prefix << indent << "Ranges: " << desc.selected_ranges << '\n';
}

static void formatIndexes(const IndexesDescription & desc, JSONBuilder::JSONMap & map, bool compact)
{
    const auto & index_stats = desc.index_stats;

    if (index_stats.empty())
        return;

    if (index_stats.size() == 1 && index_stats.front().type == IndexType::None)
        return;

    auto indexes_array = std::make_unique<JSONBuilder::JSONArray>();

    for (size_t i = 0; i < index_stats.size(); ++i)
    {
        const auto & stat = index_stats[i];
        if (stat.type == IndexType::None)
            continue;

        auto index_map = std::make_unique<JSONBuilder::JSONMap>();

        index_map->add("Type", indexTypeToString(stat.type));

        if (!stat.name.empty())
            index_map->add("Name", stat.name);

        if (!stat.description.empty())
            index_map->add("Description", stat.description);

        if (!stat.used_keys.empty())
        {
            auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

            for (const auto & used_key : stat.used_keys)
                keys_array->add(used_key);

            index_map->add("Keys", std::move(keys_array));
        }

        if (!stat.condition.empty())
            index_map->add("Condition", stat.condition);

        auto search_algorithm = searchAlgorithmToString(stat.search_algorithm);
        if (!search_algorithm.empty())
            index_map->add("Search Algorithm", search_algorithm);

        if (i)
            index_map->add("Initial Parts", index_stats[i - 1].num_parts_after);
        index_map->add("Selected Parts", stat.num_parts_after);

        if (i)
            index_map->add("Initial Granules", index_stats[i - 1].num_granules_after);
        index_map->add("Selected Granules", stat.num_granules_after);

        if (!stat.distributed.empty())
        {
            if (compact)
            {
                auto distributed_map = std::make_unique<JSONBuilder::JSONMap>();
                size_t total_parts_send = 0;
                size_t total_parts_received = 0;
                size_t total_granules_send = 0;
                size_t total_granules_received = 0;
                for (const auto & node_stat : stat.distributed)
                {
                    total_parts_send += node_stat.num_parts_send;
                    total_parts_received += node_stat.num_parts_received;
                    total_granules_send += node_stat.num_granules_send;
                    total_granules_received += node_stat.num_granules_received;
                }
                distributed_map->add("Replicas", stat.distributed.size());
                distributed_map->add("Parts send", total_parts_send);
                distributed_map->add("Parts received", total_parts_received);
                distributed_map->add("Granules send", total_granules_send);
                distributed_map->add("Granules received", total_granules_received);
                index_map->add("Distributed", std::move(distributed_map));
            }
            else
            {
                auto distributed_index_array = std::make_unique<JSONBuilder::JSONArray>();

                for (const auto & node_stat : stat.distributed)
                {
                    auto node_stat_map = std::make_unique<JSONBuilder::JSONMap>();
                    node_stat_map->add("Address", node_stat.address);
                    node_stat_map->add("Parts send", node_stat.num_parts_send);
                    node_stat_map->add("Parts received", node_stat.num_parts_received);
                    node_stat_map->add("Granules send", node_stat.num_granules_send);
                    node_stat_map->add("Granules received", node_stat.num_granules_received);
                    distributed_index_array->add(std::move(node_stat_map));
                }

                index_map->add("Distributed", std::move(distributed_index_array));
            }
        }

        indexes_array->add(std::move(index_map));
    }

    map.add("Indexes", std::move(indexes_array));

    if (desc.tables_count > 1)
        map.add("Tables", desc.tables_count);
}

static void collectIndexesFromPlan(const QueryPlan & plan, std::vector<IndexesDescription> & result)
{
    struct Frame
    {
        QueryPlan::Node * node;
        size_t next_child = 0;
    };

    if (!plan.isInitialized())
        return;

    std::stack<Frame> stack;
    stack.push(Frame{.node = plan.getRootNode()});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{.node = frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
        else
        {
            if (auto desc = frame.node->step->getIndexesDescription())
                result.push_back(std::move(*desc));

            auto child_plans = frame.node->step->getChildPlans();
            for (auto * child_plan : child_plans)
                collectIndexesFromPlan(*child_plan, result);

            stack.pop();
        }
    }
}

static IndexesDescription aggregateIndexes(const std::vector<IndexesDescription> & descriptions, size_t table_count)
{
    IndexesDescription aggregated;
    aggregated.selected_ranges = 0;

    /// Aggregate by index type, summing only numeric fields (parts, granules,
    /// distributed stats). Per-instance metadata (name, condition, keys, etc.)
    /// is intentionally not carried over — different child tables may have
    /// different skip indexes or conditions for the same index type, so
    /// showing any single one would be misleading.
    ///
    /// Note: the "Parts: X/Y" ratio in the output (current step vs previous
    /// step) can be misleading for heterogeneous index chains. For example,
    /// if table A has [PartitionMinMax, PrimaryKey] and table B has only
    /// [PrimaryKey], the aggregated PartitionMinMax count reflects only
    /// table A, while PrimaryKey reflects both — the ratio between them
    /// doesn't represent a true filtering step. This is acceptable for
    /// compact mode since Merge tables typically have uniform schemas.
    std::vector<IndexType> type_order;
    std::unordered_map<uint8_t, IndexStat> by_type;

    for (const auto & desc : descriptions)
    {
        /// Within a single description (one table), the same index type can
        /// appear multiple times (e.g. several Skip indexes). Each successive
        /// entry further reduces parts/granules. We need only the last (final)
        /// value per type from each table, then sum those across tables.
        /// Find the last index of each type, then iterate in original order
        /// (None → PrimaryKey → Skip) skipping non-last duplicates.
        std::unordered_map<uint8_t, size_t> last_index;
        for (size_t j = 0; j < desc.index_stats.size(); ++j)
            last_index[static_cast<uint8_t>(desc.index_stats[j].type)] = j;

        for (size_t j = 0; j < desc.index_stats.size(); ++j)
        {
            const auto & stat = desc.index_stats[j];
            auto key = static_cast<uint8_t>(stat.type);

            if (last_index[key] != j)
                continue;

            if (!by_type.contains(key))
                type_order.push_back(stat.type);

            auto & entry = by_type[key];
            entry.type = stat.type;
            entry.num_parts_after += stat.num_parts_after;
            entry.num_granules_after += stat.num_granules_after;
            entry.distributed.insert(entry.distributed.end(),
                stat.distributed.begin(), stat.distributed.end());
        }

        aggregated.selected_ranges += desc.selected_ranges;
    }

    for (auto type : type_order)
        aggregated.index_stats.push_back(std::move(by_type[static_cast<uint8_t>(type)]));

    aggregated.tables_count = table_count;

    return aggregated;
}

static void explainStep(IQueryPlanStep & step, JSONBuilder::JSONMap & map, const ExplainPlanOptions & options)
{
    map.add("Node Type", step.getName());
    map.add("Node Id", step.getUniqID());

    if (options.description)
    {
        const auto & description = step.getStepDescription();
        if (!description.empty())
            map.add("Description", description);
    }

    const auto dump_column = [](JSONBuilder::JSONArray & header_array, const ColumnWithTypeAndName & column)
    {
        auto column_map = std::make_unique<JSONBuilder::JSONMap>();
        column_map->add("Name", column.name);
        if (column.type)
            column_map->add("Type", column.type->getName());
        header_array.add(std::move(column_map));
    };

    if (options.header && step.hasOutputHeader())
    {
        auto header_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & output_column : *step.getOutputHeader())
            dump_column(*header_array, output_column);

        map.add("Header", std::move(header_array));
    }

    if (options.input_headers && !step.getInputHeaders().empty())
    {
        auto input_headers_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & input_header : step.getInputHeaders())
        {
            auto header_array = std::make_unique<JSONBuilder::JSONArray>();

            for (const auto & input_column : *input_header)
                dump_column(*header_array, input_column);

            input_headers_array->add(std::move(header_array));
        }

        map.add("Input Headers", std::move(input_headers_array));
    }

    if (options.actions)
        step.describeActions(map);

    if (options.indexes)
    {
        if (auto desc = step.getIndexesDescription())
            formatIndexes(*desc, map, options.compact);
    }

    if (options.projections)
        step.describeProjections(map);
}

static QueryPlan::Node * skipExpressions(QueryPlan::Node * node)
{
    while (node->step->getName() == "Expression" && !node->children.empty())
        node = node->children[0];
    return node;
}

JSONBuilder::ItemPtr QueryPlan::explainPlan(const ExplainPlanOptions & options) const
{
    checkInitialized();

    if (options.compact && options.indexes)
    {
        auto * node = skipExpressions(root);
        auto node_map = std::make_unique<JSONBuilder::JSONMap>();
        auto header_options = options;
        header_options.indexes = false;
        explainStep(*node->step, *node_map, header_options);

        std::vector<IndexesDescription> descriptions;
        collectIndexesFromPlan(*this, descriptions);

        if (!descriptions.empty())
        {
            auto aggregated = aggregateIndexes(descriptions, descriptions.size());
            formatIndexes(aggregated, *node_map, options.compact);
        }

        return node_map;
    }

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
    const ExplainPlanOptions & options,
    size_t max_description_length)
{

    settings.out << settings.header_prefix << step.getName();
    const auto & prefix = settings.detail_prefix;

    auto description = step.getStepDescription();

    String pretty_description;
    if (settings.pretty)
    {
        pretty_description = QueryPlanFormat::trimColumnIdentifier(description);
        description = pretty_description;
    }

    if (max_description_length)
        description = description.substr(0, max_description_length);
    if (options.description && !description.empty())
        settings.out <<" (" << description << ')';

    settings.out.write('\n');

    const auto dump_column = [&out = settings.out, dump_structure = options.column_structure](const ColumnWithTypeAndName & column)
    {
        if (dump_structure)
            column.dumpStructure(out);
        else
            column.dumpNameAndType(out);
    };

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

            for (const auto & elem : *step.getOutputHeader())
            {
                if (!first)
                    settings.out << '\n' << prefix << "        ";

                first = false;
                dump_column(elem);
            }
        }
        settings.out.write('\n');
    }

    if (options.input_headers)
    {
        const std::string_view input_headers_title = "Input headers: ";
        const std::string_view input_header_indent = "               ";
        settings.out << prefix << input_headers_title;

        bool first_input_header = true;
        size_t input_header_index = 0;

        if (step.getInputHeaders().empty())
        {
            settings.out << "No input headers";
        }
        else
        {
            for (const auto & input_header : step.getInputHeaders())
            {
                if (!first_input_header)
                    settings.out << '\n' << prefix << input_header_indent;
                first_input_header = false;

                settings.out << fmt::format("#{}", input_header_index);
                ++input_header_index;

                if (input_header->empty())
                {
                    settings.out << " Empty header";
                    continue;
                }

                for (const auto & elem : *input_header)
                {
                    settings.out << '\n' << prefix << input_header_indent;
                    dump_column(elem);
                }
            }
        }
        settings.out.write('\n');
    }

    if (options.sorting)
    {
        if (const auto & sort_description = step.getSortDescription(); !sort_description.empty())
        {
            settings.out << prefix << "Sorting: ";
            dumpSortDescription(sort_description, settings);
            settings.out.write('\n');
        }
    }

    if (options.actions)
        step.describeActions(settings);

    if (options.indexes)
    {
        if (auto desc = step.getIndexesDescription())
            formatIndexes(*desc, settings);
    }

    if (options.projections)
        step.describeProjections(settings);

    if (options.distributed)
        step.describeDistributedPlan(settings, options);
}

std::string debugExplainStep(IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    ExplainPlanOptions options{.actions = true};
    IQueryPlanStep::FormatSettings settings{.out = out, .header_prefix = "", .detail_prefix = "", .pretty_names = {}, .runtime_filter_names = {}};
    explainStep(step, settings, options, 0);
    return out.str();
}

std::string debugExplainPlan(const QueryPlan & plan)
{
    WriteBufferFromOwnString out;
    ExplainPlanOptions options{.header = true, .actions = true};
    plan.explainPlan(out, options);
    return out.str();
}

namespace ExplainPlan
{
    struct Frame
    {
        QueryPlan::Node * node = {};
        size_t next_child = 0;
        bool is_description_printed = false;
        bool is_last_child = true;
    };
};

static void buildTreeOffset(
    const std::deque<ExplainPlan::Frame> & frames,
    const ExplainPlan::Frame & current,
    IQueryPlanStep::FormatSettings & settings_format,
    const std::string & parent_tree_prefix = "",
    bool is_last_child_plan = true)
{
    if (frames.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Frames stack for building tree offset cannot be empty");

    settings_format.header_prefix = parent_tree_prefix;
    settings_format.detail_prefix = parent_tree_prefix;

    bool has_children = !current.node->children.empty() || !current.node->step->getChildPlans().empty();

    if (frames.size() == 1)
    {
        if (!parent_tree_prefix.empty())
        {
            settings_format.header_prefix += is_last_child_plan ? "└──" : "├──";
            settings_format.detail_prefix += is_last_child_plan ? "   " : "│  ";
        }
        settings_format.detail_prefix += has_children ? "│  " : "   ";
        return;
    }

    if (!parent_tree_prefix.empty())
    {
        settings_format.header_prefix += is_last_child_plan ? "   " : "│  ";
        settings_format.detail_prefix += is_last_child_plan ? "   " : "│  ";
    }

    for (size_t i = 0; i < frames.size() - 2; ++i)
    {
        const auto & segment = frames[i + 1].is_last_child ? "   " : "│  ";
        settings_format.header_prefix += segment;
        settings_format.detail_prefix += segment;
    }

    settings_format.header_prefix += current.is_last_child ? "└──" : "├──";
    settings_format.detail_prefix += current.is_last_child ? "   " : "│  ";
    settings_format.detail_prefix += has_children ? "│  " : "   ";
}

static void buildIndentOffset(const std::deque<ExplainPlan::Frame> & frames, IQueryPlanStep::FormatSettings & settings_format, size_t indent_offset)
{
    settings_format.offset = (frames.size() - 1 + indent_offset) * settings_format.base_indent;
    settings_format.header_prefix = std::string(settings_format.offset, settings_format.indent_char);
    settings_format.detail_prefix = settings_format.header_prefix;
}

void QueryPlan::explainPlan(
    WriteBuffer & buffer,
    const ExplainPlanOptions & options,
    size_t offset,
    size_t max_description_length,
    const std::string & parent_tree_prefix,
    bool is_last_child_plan) const
{
    checkInitialized();

    IQueryPlanStep::FormatSettings settings{
        .out = buffer,
        .header_prefix = "",
        .detail_prefix = "",
        .write_header = options.header,
        .compact = options.compact,
        .pretty = options.pretty,
        .pretty_names = {},
        .runtime_filter_names = {}
    };

    if (options.pretty)
    {
        std::unordered_map<FutureSet::Hash, String, PreparedSets::Hashing> subquery_set_names;
        QueryPlanFormat::buildPrettyNamesMap(*this, settings.pretty_names, settings.runtime_filter_names, subquery_set_names);
        for (const auto & [hash, name] : subquery_set_names)
            settings.pretty_names[PreparedSets::toString(hash, {})] = PrettyColumnName(name);
    }

    std::deque<ExplainPlan::Frame> stack;

    if (settings.pretty && parent_tree_prefix.empty())
    {
        QueryPlanFormat::formatOutputColumns(settings.pretty_names, settings.out, *root->step, settings.header_prefix);
        settings.out << '\n';
    }

    /// In compact mode, collect all indexes from the entire plan tree
    /// (including child plans like Merge table sub-plans), aggregate them,
    /// and print a flat summary after the reading step header.
    if (options.compact && options.indexes)
    {
        auto * reading_node = skipExpressions(root);
        auto header_options = options;
        header_options.indexes = false;
        explainStep(*reading_node->step, settings, header_options, max_description_length);

        std::vector<IndexesDescription> descriptions;
        collectIndexesFromPlan(*this, descriptions);

        if (!descriptions.empty())
        {
            auto aggregated = aggregateIndexes(descriptions, descriptions.size());
            formatIndexes(aggregated, settings);
        }
        return;
    }

    auto * first_node = options.compact ? skipExpressions(root) : root;
    stack.push_back(ExplainPlan::Frame{
        .node = first_node,
    });

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (!frame.is_description_printed)
        {
            if (options.pretty)
                buildTreeOffset(stack, frame, settings, parent_tree_prefix, is_last_child_plan);
            else
                buildIndentOffset(stack, settings, offset);

            explainStep(*frame.node->step, settings, options, max_description_length);
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            size_t child_idx = frame.next_child;

            bool has_child_plans_below = !frame.node->step->getChildPlans().empty();
            bool is_last = (frame.next_child + 1) == (frame.node->children.size()) && !has_child_plans_below;
            auto * next_node = options.compact ? skipExpressions(frame.node->children[child_idx]) : frame.node->children[child_idx];

            stack.push_back(ExplainPlan::Frame{
                .node = next_node,
                .is_last_child = is_last,
            });
            ++frame.next_child;
        }
        else
        {
            auto child_plans = frame.node->step->getChildPlans();

            std::string base_prefix;
            if (options.pretty && !child_plans.empty())
            {
                if (!parent_tree_prefix.empty())
                    base_prefix += is_last_child_plan ? "   " : "│  ";
                for (size_t i = 0; i < stack.size() - 1; ++i)
                    base_prefix += stack[i + 1].is_last_child ? "   " : "│  ";
                base_prefix = parent_tree_prefix + base_prefix;
            }

            size_t plan_idx = 0;

            for (const auto & child_plan : child_plans)
            {
                bool is_last_plan = (plan_idx + 1 == child_plans.size());
                child_plan->explainPlan(buffer, options, offset + stack.size(),
                                        max_description_length, base_prefix, is_last_plan);
                ++plan_idx;
            }

            stack.pop_back();
        }
    }
}

static void explainPipelineStep(IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings, bool distributed)
{
    settings.out << String(settings.offset, settings.indent_char) << "(" << step.getName() << ")\n";

    size_t current_offset = settings.offset;
    step.describePipeline(settings);
    if (current_offset == settings.offset)
        settings.offset += settings.base_indent;

    if (distributed)
        step.describeDistributedPipeline(settings, distributed);
}

void QueryPlan::explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options) const
{
    checkInitialized();

    IQueryPlanStep::FormatSettings settings{
        .out = buffer,
        .header_prefix = "",
        .detail_prefix = "",
        .write_header = options.header,
        .compact_repeated_processor_chains = options.compact_repeated_processor_chains,
        .pretty_names = {},
        .runtime_filter_names = {}
    };

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
            explainPipelineStep(*frame.node->step, settings, options.distributed);
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
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::QueryPlanOptimizeMicroseconds);

    /// optimization need to be applied before "mergeExpressions" optimization
    /// it removes redundant sorting steps, but keep underlying expressions,
    /// so "mergeExpressions" optimization handles them afterwards
    if (optimization_settings.remove_redundant_sorting)
        QueryPlanOptimizations::tryRemoveRedundantSorting(root);

    QueryPlanOptimizations::optimizeTreeFirstPass(optimization_settings, *root, nodes);
    QueryPlanOptimizations::optimizeTreeSecondPass(optimization_settings, *root, nodes, *this);
    /// `addStepsToBuildSets` is invoked before `resolveMaterializingCTEs` so
    /// that `DelayedCreatingSetsStep::makePlansForSets` (and any synchronous
    /// `buildSetInplace` / `buildOrderedSetInplace` it triggers via the
    /// recursive `plan->optimize`) can materialize a referenced CTE through
    /// the safety-net `DelayedMaterializingCTEsStep` planted by
    /// `forceMaterializeCTE` before the outer `DelayedMaterializingCTEsStep`
    /// in this plan is claimed. `resolveMaterializingCTEs` then only
    /// materializes the CTEs that were not already materialized inplace.
    if (optimization_settings.build_sets)
        QueryPlanOptimizations::addStepsToBuildSets(optimization_settings, *this, *root, nodes);
    if (optimization_settings.materialize_ctes)
        QueryPlanOptimizations::resolveMaterializingCTEs(optimization_settings, *this, *root, nodes);
}

namespace QueryPlanOptimizations
{

DistributedQueryPlan makeDistributedPlan(QueryPlan::Nodes nodes, QueryPlan::Node * root, const QueryPlanOptimizationSettings & optimization_settings);

}

void QueryPlan::convertToDistributed(const QueryPlanOptimizationSettings & optimization_settings)
{
    SharedHeader result_header = root->step->getOutputHeader();

    QueryPlan::Nodes old_nodes = std::move(nodes);
    QueryPlan::Node * old_root = root;
    root = nullptr;
    auto distributed_plan = QueryPlanOptimizations::makeDistributedPlan(std::move(old_nodes), old_root, optimization_settings);

    for (const auto & stage : distributed_plan.stages)
    {
        auto it = distributed_plan.stage_depends_on.find(stage.first);
        const auto & dependencies = it != distributed_plan.stage_depends_on.end() ? it->second : std::unordered_map<String, String>{};
        LOG_TRACE(getLogger("optimize"), "Distributed stage: '{}' depends on: [{}] plan:\n{}",
            stage.first, fmt::join(dependencies, ", "), dumpQueryPlan(stage.second.query_plan_fragment));
    }

    if (distributed_plan.stages.size() == 1)
    {
        /// For now just replace the plan with the first and only fragment, but preserve
        /// table locks and storage holders accumulated during planning.
        QueryPlanResourceHolder preserved_resources = std::move(resources);
        *this = std::move(distributed_plan.stages.begin()->second.query_plan_fragment);
        /// QueryPlanResourceHolder's move-assignment appends rhs into lhs without dropping existing entries.
        resources = std::move(preserved_resources);

        QueryPlanOptimizationSettings local_settings = optimization_settings;
        local_settings.make_distributed_plan = false;
        QueryPlanOptimizations::optimizeTreeSecondPass(local_settings, *root, nodes, *this);
    }
    else
    {
        ExchangeDescription final_result_exchange
        {
            .name = "final_result",
            .kind = optimization_settings.distributed_plan_force_exchange_kind == "Persisted" ? ExchangeDescription::Kind::Persisted : ExchangeDescription::Kind::Streaming,
            .source_bucket_count = 1,
            .destination_bucket_count = 1
        };
        auto result_stream_id = ExchangeStreamId(final_result_exchange.name, 0, 0);

        /// Add a step that writes the result of the main stage to the file
        auto & main_stage = distributed_plan.stages["main"];
        if (!main_stage.query_plan_fragment.isCompleted())
        {
            main_stage.query_plan_fragment.addStep(std::make_unique<GatherSendStep>(result_header, final_result_exchange.name));
            main_stage.tasks.front().output_exchange_streams.emplace_back(result_stream_id);
            distributed_plan.exchange_descriptions[final_result_exchange.name] = final_result_exchange;
            distributed_plan.final_result_stream_name = result_stream_id.toString();
        }

        /// Fail early (before execution) if any fragment contains a step that cannot be serialized
        /// for remote execution, instead of throwing late from serializeQueryPlan.
        for (const auto & [stage_name, stage] : distributed_plan.stages)
            assertFragmentSerializable(stage.query_plan_fragment, stage_name);

        /// Collect the list of all temporary files
        Strings all_temporary_files_for_cleanup;
        for (const auto & stage : distributed_plan.stages)
        {
            for (const auto & task : stage.second.tasks)
            {
                for (const auto & stream_id : task.output_exchange_streams)
                {
                    if (distributed_plan.exchange_descriptions.at(stream_id.exchange_id).kind == ExchangeDescription::Kind::Persisted)
                        all_temporary_files_for_cleanup.push_back(stream_id.toString());
                }
            }
        }

        auto context = CurrentThread::tryGetQueryContext();
        chassert(context);
        /// Local execution runs every task in-process and needs no worker hosts; constructing
        /// TaskToHostMap would require a configured worker cluster and fail on a plain single server.
        TaskToHostMapPtr task_to_host_map = optimization_settings.distributed_plan_execute_locally
            ? nullptr
            : std::make_shared<TaskToHostMap>(distributed_plan, context);

        /// Generate random unique id for the query
        /// We cannot use query_id from the context because user can put any string there and it might be not unique
        UUID unique_query_id = UUIDHelpers::generateV4();

        /// Make plan stub that reads from the executor that executes the distributed plan
        Pipe run_distributed_plan(std::make_shared<ReadFromDistributedPlanSource>(result_header, unique_query_id, std::move(distributed_plan), task_to_host_map));
        Pipes pipes;
        pipes.emplace_back(std::move(run_distributed_plan));

        auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(toString(unique_query_id), context);

        /// TODO: do this only if final_result_exchange is persisted
        auto temporary_files = createTemporaryFilesLookup(
            object_storage, object_storage_path, {result_stream_id.toString()}, {});

        ExchangeDescriptions exchange_descriptions;
        exchange_descriptions[final_result_exchange.name] = final_result_exchange;
        auto exchange_lookup = createExchangeLookup(
            toString(unique_query_id),
            exchange_descriptions,
            task_to_host_map ? ExchangeStreamSources{task_to_host_map->getExchangeStreamSourceHosts()} : ExchangeStreamSources{},
            temporary_files,
            context);

        auto lazily_create_result_reader = [result_header, exchange_lookup, result_stream_id]() -> QueryPipelineBuilder
        {
            Pipe read_result_from(exchange_lookup->createSource(result_header, result_stream_id));
            QueryPipelineBuilder builder;
            builder.init(std::move(read_result_from));
            return builder;
        };
        pipes.emplace_back(createDelayedPipe(result_header, lazily_create_result_reader, false, false));

        Pipe inputs = Pipe::unitePipes(std::move(pipes));
        /// For streaming exchange we start both inputs in parallel to let the main task send back the result to the initiator.
        /// In case of persisted exchange use ConcatProcessor to first execute the whole distributed plan and after that read the result from the file.
        if (final_result_exchange.kind == ExchangeDescription::Kind::Persisted)
            inputs.addTransform(std::make_shared<ConcatProcessor>(inputs.getSharedHeader(), inputs.numOutputPorts()));

        /// Plan stub that will be used if distributed plan is enabled
        QueryPlan read_from_distributed;

        read_from_distributed.addStep(std::make_unique<ReadFromPreparedSource>(std::move(inputs)));

        /// Preserve original table locks and storage holders across the move-assign
        /// so the final pipeline keeps the tables referenced by serialized fragments alive.
        QueryPlanResourceHolder preserved_resources = std::move(resources);
        *this = std::move(read_from_distributed);
        resources = std::move(preserved_resources);

        /// In-memory exchanges (execute_locally) must outlive the executor: the result reader drains
        /// final_result after the driver has finished. Remove them when the pipeline resources go away.
        resources.custom_resources.emplace_back(makeInMemoryExchangesCleaner(toString(unique_query_id)));

        /// Add temporary files cleaner to the resources so that all temporary files are removed after the pipeline is executed
        if (final_result_exchange.kind == ExchangeDescription::Kind::Persisted)
            all_temporary_files_for_cleanup.push_back(result_stream_id.toString());

        if (object_storage)
        {
            auto temporary_files_cleaner = makeTemporaryFilesCleaner(object_storage, object_storage_path, all_temporary_files_for_cleanup);
            resources.custom_resources.emplace_back(std::move(temporary_files_cleaner));
        }
    }
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

void QueryPlan::cloneInplace(Node * node_to_replace, Node * subplan_root)
{
    if (!subplan_root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot clone subplan in place because subplan root is null");

    struct Frame
    {
        Node * node;
        Node * clone;
        std::vector<Node *> children = {};
    };

    std::vector<Frame> nodes_to_process{ Frame{ .node = subplan_root, .clone = node_to_replace } };

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

            nodes.emplace_back(Node{ .step = {} });
            nodes.back().children.reserve(child->children.size());
            auto * child_clone = &nodes.back();

            frame.children.push_back(child_clone);

            nodes_to_process.push_back(Frame{ .node = child, .clone = child_clone });
        }
    }
}

QueryPlan QueryPlan::clone() const
{
    QueryPlan result;
    result.nodes.emplace_back(Node{ .step = {}, .children = {} });
    auto * current_subplan_copy_root = &result.nodes.back();

    result.cloneInplace(current_subplan_copy_root, root);
    result.root = current_subplan_copy_root;

    return result;
}

void QueryPlan::cloneSubplanAndReplace(Node * node_to_replace, Node * subplan_root, Nodes & nodes)
{
    if (!subplan_root)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot clone subplan in place because subplan root is null");

    struct Frame
    {
        Node * node;
        Node * clone;
        std::vector<Node *> children = {};
    };

    std::vector<Frame> nodes_to_process{ Frame{ .node = subplan_root, .clone = node_to_replace } };

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

            nodes.emplace_back(Node{ .step = {} });
            nodes.back().children.reserve(child->children.size());
            auto * child_clone = &nodes.back();

            frame.children.push_back(child_clone);

            nodes_to_process.push_back(Frame{ .node = child, .clone = child_clone });
        }
    }
}


void QueryPlan::replaceNodeWithPlan(Node * node, QueryPlan plan)
{
    chassert(nodes.end() != std::find_if(cbegin(nodes), cend(nodes), [node](const Node & n) { return n.step == node->step; }));

    SharedHeader expected_header;
    if (node->step)
        expected_header = node->step->getOutputHeader();

    replaceNodeWithPlan(node, std::move(plan), std::move(expected_header));
}

void QueryPlan::replaceNodeWithPlan(Node * node, QueryPlan plan, SharedHeader expected_header)
{
    if (expected_header)
    {
        const auto & plan_header = plan.getCurrentHeader();

        if (!blocksHaveEqualStructure(*expected_header, *plan_header))
        {
            auto converting_dag = ActionsDAG::makeConvertingActions(
                plan_header->getColumnsWithTypeAndName(),
                expected_header->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                nullptr);

            auto expression = std::make_unique<ExpressionStep>(plan_header, std::move(converting_dag));
            plan.addStep(std::move(expression));
        }
    }

    nodes.splice(nodes.end(), std::move(plan.nodes));

    node->step = std::move(plan.getRootNode()->step);
    node->children = std::move(plan.getRootNode()->children);

    max_threads = std::max(max_threads, plan.max_threads);
    resources = std::move(plan.resources);
}

}
