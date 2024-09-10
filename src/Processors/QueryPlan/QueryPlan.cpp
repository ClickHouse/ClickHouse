#include <stack>

#include <Common/JSONBuilder.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Storages/StorageSet.h>
#include <Columns/ColumnSet.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>

#include <IO/Operators.h>
#include <IO/WriteBuffer.h>

#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlanVisitor.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Sources/NullSource.h>

#include <Analyzer/Resolve/IdentifierResolver.h>
#include <Analyzer/TableNode.h>

#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
    return isInitialized() && !root->step->hasOutputStream();
}

const DataStream & QueryPlan::getCurrentDataStream() const
{
    checkInitialized();
    checkNotCompleted();
    return root->step->getOutputStream();
}

void QueryPlan::unitePlans(QueryPlanStepPtr step, std::vector<std::unique_ptr<QueryPlan>> plans)
{
    if (isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite plans because current QueryPlan is already initialized");

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

    for (auto & plan : plans)
    {
        max_threads = std::max(max_threads, plan->max_threads);
        resources = std::move(plan->resources);
    }
}

void QueryPlan::addStep(QueryPlanStepPtr step)
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

    return last_pipeline;
}

static void explainStep(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const QueryPlan::ExplainPlanOptions & options)
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

JSONBuilder::ItemPtr QueryPlan::explainPlan(const ExplainPlanOptions & options)
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
    const IQueryPlanStep & step,
    IQueryPlanStep::FormatSettings & settings,
    const QueryPlan::ExplainPlanOptions & options)
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

std::string debugExplainStep(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    IQueryPlanStep::FormatSettings settings{.out = out};
    QueryPlan::ExplainPlanOptions options{.actions = true};
    explainStep(step, settings, options);
    return out.str();
}

void QueryPlan::explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options, size_t indent)
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

void QueryPlan::explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options)
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

static void updateDataStreams(QueryPlan::Node & root)
{
    class UpdateDataStreams : public QueryPlanVisitor<UpdateDataStreams, false>
    {
    public:
        explicit UpdateDataStreams(QueryPlan::Node * root_) : QueryPlanVisitor<UpdateDataStreams, false>(root_) { }

        static bool visitTopDownImpl(QueryPlan::Node * /*current_node*/, QueryPlan::Node * /*parent_node*/) { return true; }

        static void visitBottomUpImpl(QueryPlan::Node * current_node, QueryPlan::Node * /*parent_node*/)
        {
            auto & current_step = *current_node->step;
            if (!current_step.canUpdateInputStream() || current_node->children.empty())
                return;

            for (const auto * child : current_node->children)
            {
                if (!child->step->hasOutputStream())
                    return;
            }

            DataStreams streams;
            streams.reserve(current_node->children.size());
            for (const auto * child : current_node->children)
                streams.emplace_back(child->step->getOutputStream());

            current_step.updateInputStreams(std::move(streams));
        }
    };

    UpdateDataStreams(&root).visit();
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
    QueryPlanOptimizations::optimizeTreeThirdPass(*this, *root, nodes);

    updateDataStreams(*root);
}

void QueryPlan::explainEstimate(MutableColumns & columns)
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

std::pair<QueryPlan::Nodes, QueryPlanResourceHolder> QueryPlan::detachNodesAndResources(QueryPlan && plan)
{
    return {std::move(plan.nodes), std::move(plan.resources)};
}

static void serializeHeader(const Block & header, WriteBuffer & out)
{
    /// Write only names and types.
    /// Constants should be filled by step.

    writeVarUInt(header.columns(), out);
    for (const auto & column : header)
    {
        writeStringBinary(column.name, out);
        encodeDataType(column.type, out);
    }
}

static Block deserializeHeader(ReadBuffer & in)
{
    UInt64 num_columns;
    readVarUInt(num_columns, in);

    ColumnsWithTypeAndName columns(num_columns);

    for (auto & column : columns)
    {
        readStringBinary(column.name, in);
        column.type = decodeDataType(in);
    }

    /// Fill columns in header. Some steps expect them to be not empty.
    for (auto & column : columns)
        column.column = column.type->createColumn();

    return Block(std::move(columns));
}

enum class SetSerializationKind : UInt8
{
    StorageSet = 1,
    TupleValues = 2,
    SubqueryPlan = 3,
};

static void serializeSets(SerializedSetsRegistry & registry, WriteBuffer & out)
{
    writeVarUInt(registry.sets.size(), out);
    for (const auto & [hash, set] : registry.sets)
    {
        writeBinary(hash, out);

        if (auto * from_storage = typeid_cast<FutureSetFromStorage *>(set.get()))
        {
            writeIntBinary(SetSerializationKind::StorageSet, out);
            const auto & storage_id = from_storage->getStorageID();
            if (!storage_id)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "FutureSetFromStorage without storage id");

            auto storage_name = storage_id->getFullTableName();
            writeStringBinary(storage_name, out);
        }
        else if (auto * from_tuple = typeid_cast<FutureSetFromTuple *>(set.get()))
        {
            writeIntBinary(SetSerializationKind::TupleValues, out);

            UInt8 flags = 0;
            if (from_tuple->get()->transform_null_in)
                flags |= 1;

            writeIntBinary(flags, out);

            auto types = from_tuple->getTypes();
            auto columns = from_tuple->getKeyColumns();

            if (columns.size() != types.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Invalid number of columns for Set. Expected {} got {}",
                    columns.size(), types.size());

            UInt64 num_columns = columns.size();
            UInt64 num_rows = num_columns > 0 ? columns.front()->size() : 0;

            writeVarUInt(num_columns, out);
            writeVarUInt(num_rows, out);

            for (size_t col = 0; col < num_columns; ++col)
            {
                if (columns[col]->size() != num_rows)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Invalid number of rows in column of Set. Expected {} got {}",
                        num_rows, columns[col]->size());

                encodeDataType(types[col], out);
                auto serialization = types[col]->getSerialization(ISerialization::Kind::DEFAULT);
                serialization->serializeBinaryBulk(*columns[col], out, 0, num_rows);
            }
        }
        else if (auto * from_subquery = typeid_cast<FutureSetFromSubquery *>(set.get()))
        {
            writeIntBinary(SetSerializationKind::SubqueryPlan, out);
            const auto * plan = from_subquery->getQueryPlan();
            if (!plan)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot serialize FutureSetFromSubquery with no query plan");

            auto not_filled = from_subquery->getNotFilled();

            UInt8 flags = 0;
            if (not_filled->transform_null_in)
                flags |= 1;

            writeIntBinary(flags, out);

            const auto & limits = not_filled->limits;
            SettingFieldOverflowMode mode(limits.overflow_mode);

            writeVarUInt(limits.max_rows, out);
            writeVarUInt(limits.max_bytes, out);
            writeIntBinary(limits.overflow_mode, out);

            writeVarUInt(not_filled->max_elements_to_fill, out);

            plan->serialize(out);
        }
        else
        {
            const auto & set_ref = *set;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown FutureSet type {}", typeid(set_ref).name());
        }
    }
}

QueryPlanAndSets deserializeSets(QueryPlan plan, DeserializedSetsRegistry & registry, ReadBuffer & in)
{
    UInt64 num_sets;
    readVarUInt(num_sets, in);

    QueryPlanAndSets res;
    res.plan = std::move(plan);

    for (size_t i = 0; i < num_sets; ++i)
    {
        PreparedSets::Hash hash;
        readBinary(hash, in);

        auto it = registry.sets.find(hash);
        if (it == registry.sets.end())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} is not registered", hash.low64, hash.high64);

        auto & columns = it->second;
        if (columns.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} is serialized twice", hash.low64, hash.high64);

        UInt8 kind;
        readVarUInt(kind, in);
        if (kind == UInt8(SetSerializationKind::StorageSet))
        {
            String storage_id;
            readStringBinary(storage_id, in);
            res.sets_from_storage.emplace_back(hash, std::move(storage_id), std::move(columns));
        }
        else if (kind == UInt8(SetSerializationKind::TupleValues))
        {
            UInt8 flags;
            readIntBinary(flags, in);
            bool transform_null_in = bool(flags & 1);

            UInt64 num_columns;
            UInt64 num_rows;
            readVarUInt(num_columns, in);
            readVarUInt(num_rows, in);

            ColumnsWithTypeAndName set_columns;
            set_columns.reserve(num_columns);

            for (size_t col = 0; col < num_columns; ++col)
            {
                auto type = decodeDataType(in);
                auto serialization = type->getSerialization(ISerialization::Kind::DEFAULT);
                auto column = type->createColumn();
                serialization->deserializeBinaryBulk(*column, in, num_rows, 0);

                set_columns.emplace_back(std::move(column), std::move(type), String{});
            }

            SizeLimits size_limits;
            auto set = std::make_shared<FutureSetFromTuple>(hash, std::move(set_columns), transform_null_in, size_limits);

            for (auto * column : columns)
                column->setData(set);
        }
        else if (kind == UInt8(SetSerializationKind::SubqueryPlan))
        {
            UInt8 flags;
            readIntBinary(flags, in);
            bool transform_null_in = bool(flags & 1);

            SizeLimits limits;
            readVarUInt(limits.max_rows, in);
            readVarUInt(limits.max_bytes, in);
            readIntBinary(limits.overflow_mode, in);

            if (limits.overflow_mode != OverflowMode::BREAK && limits.overflow_mode != OverflowMode::THROW)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect overflow mode {}", limits.overflow_mode);

            UInt64 max_size_for_index;
            readVarUInt(max_size_for_index, in);

            auto plan_for_set = QueryPlan::deserialize(in);
            auto set = std::make_shared<FutureSetFromSubquery>(
                hash, std::make_unique<QueryPlan>(std::move(plan_for_set.plan)), nullptr, nullptr,
                transform_null_in, limits, max_size_for_index);

            for (auto * column : columns)
                column->setData(set);

            res.subqueries.emplace_back(std::move(set), std::move(plan_for_set.subqueries));
            res.sets_from_storage.splice(res.sets_from_storage.end(), std::move(plan_for_set.sets_from_storage));
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA, "Serialized set {}_{} has unknown kind {}",
                hash.low64, hash.high64, int(kind));
    }

    return res;
}

void QueryPlan::serialize(WriteBuffer & out) const
{
    checkInitialized();

    SerializedSetsRegistry registry;

    struct Frame
    {
        Node * node = {};
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});
    while (!stack.empty())
    {
        auto & frame = stack.top();
        auto * node = frame.node;

        if (typeid_cast<DelayedCreatingSetsStep *>(node->step.get()))
        {
            frame.node = node->children.front();
            continue;
        }

        if (frame.next_child == 0)
        {
            writeVarUInt(node->children.size(), out);
        }

        if (frame.next_child < node->children.size())
        {
            stack.push(Frame{.node = node->children[frame.next_child]});
            ++frame.next_child;
            continue;
        }

        stack.pop();

        writeStringBinary(node->step->getSerializationName(), out);
        writeStringBinary(node->step->getStepDescription(), out);

        if (node->step->hasOutputStream())
            serializeHeader(node->step->getOutputStream().header, out);
        else
            serializeHeader({}, out);

        QueryPlanSerializationSettings settings;
        node->step->serializeSettings(settings);

        settings.writeChangedBinary(out);

        IQueryPlanStep::Serialization ctx{out, registry};
        node->step->serialize(ctx);
    }

    serializeSets(registry, out);
}

QueryPlanAndSets QueryPlan::deserialize(ReadBuffer & in)
{
    QueryPlanStepRegistry & step_registry = QueryPlanStepRegistry::instance();

    DeserializedSetsRegistry sets_registry;

    using NodePtr = Node *;
    struct Frame
    {
        NodePtr & to_fill;
        size_t next_child = 0;
        std::vector<Node *> children = {};
    };

    std::stack<Frame> stack;

    QueryPlan plan;
    stack.push(Frame{.to_fill = plan.root});

    while (!stack.empty())
    {
        auto & frame = stack.top();
        if (frame.next_child == 0)
        {
            UInt64 num_children;
            readVarUInt(num_children, in);
            frame.children.resize(num_children);
        }

        if (frame.next_child < frame.children.size())
        {
            stack.push(Frame{.to_fill = frame.children[frame.next_child]});
            ++frame.next_child;
            continue;
        }

        std::string step_name;
        std::string step_description;
        readStringBinary(step_name, in);
        readStringBinary(step_description, in);

        DataStream output_stream;
        output_stream.header = deserializeHeader(in);

        QueryPlanSerializationSettings settings;
        settings.readBinary(in);

        DataStreams input_streams;
        input_streams.reserve(frame.children.size());
        for (const auto & child : frame.children)
            input_streams.push_back(child->step->getOutputStream());

        IQueryPlanStep::Deserialization ctx{in, sets_registry, input_streams, &output_stream, settings};
        auto step = step_registry.createStep(step_name, ctx);

        if (step->hasOutputStream())
        {
            assertCompatibleHeader(step->getOutputStream().header, output_stream.header,
                 fmt::format("deserialization of query plan {} step", step_name));
        }
        else if (output_stream.header.columns())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Deserialized step {} has no output stream, but deserialized header is not empty : {}",
                step_name, output_stream.header.dumpStructure());

        auto & node = plan.nodes.emplace_back(std::move(step), std::move(frame.children));
        frame.to_fill = &node;

        stack.pop();
    }

    return deserializeSets(std::move(plan), sets_registry, in);
}

static std::shared_ptr<TableNode> resolveTable(const Identifier & identifier, const ContextPtr & context)
{
    auto table_node_ptr = IdentifierResolver::tryResolveTableIdentifierFromDatabaseCatalog(identifier, context);
    if (!table_node_ptr)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Unknown table {}", identifier.getFullName());

    return table_node_ptr;
}

static void makeSetsFromStorageSet(QueryPlanAndSets::SetsFromStorage sets, const ContextPtr & context)
{
    for (auto & set : sets)
    {
        Identifier identifier(set.storage_name);
        auto table_node = resolveTable(identifier, context);
        const auto * storage_set = typeid_cast<const StorageSet *>(table_node->getStorage().get());
        if (!storage_set)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Table {} is not a StorageSet", set.storage_name);

        auto future_set = std::make_shared<FutureSetFromStorage>(set.hash, storage_set->getSet(), table_node->getStorageID());
        for (auto * column : set.columns)
            column->setData(future_set);
    }
}

static QueryPlanResourceHolder replaceReadingFromTable(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const ContextPtr & context)
{
    const auto * reading_from_table = typeid_cast<const ReadFromTableStep *>(node.step.get());
    if (!reading_from_table)
        return {};

    Identifier identifier(reading_from_table->getTable());
    auto table_node = resolveTable(identifier, context);

    SelectQueryInfo select_query_info;
    select_query_info.table_expression_modifiers = reading_from_table->getTableExpressionModifiers();

    auto column_names = reading_from_table->getOutputStream().header.getNames();

    QueryPlan reading_plan;
    table_node->getStorage()->read(
        reading_plan,
        column_names,
        table_node->getStorageSnapshot(),
        select_query_info,
        context,
        QueryProcessingStage::FetchColumns,
        context->getSettingsRef().max_block_size,
        context->getSettingsRef().max_threads
    );

    if (!reading_plan.isInitialized())
    {
        /// Create step which reads from empty source if storage has no data.
        auto source_header = table_node->getStorageSnapshot()->getSampleBlockForColumns(column_names);
        Pipe pipe(std::make_shared<NullSource>(source_header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource");
        reading_plan.addStep(std::move(read_from_pipe));
    }

    auto converting_actions = ActionsDAG::makeConvertingActions(
        reading_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
        reading_from_table->getOutputStream().header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    node.step = std::make_unique<ExpressionStep>(reading_plan.getCurrentDataStream(), std::move(converting_actions));
    node.children = {reading_plan.getRootNode()};

    auto nodes_and_resource = QueryPlan::detachNodesAndResources(std::move(reading_plan));

    nodes.splice(nodes.end(), std::move(nodes_and_resource.first));
    return std::move(nodes_and_resource.second);
}

void QueryPlan::resolveReadFromTable(QueryPlan & plan, const ContextPtr & context)
{
    std::stack<QueryPlan::Node *> stack;
    stack.push(plan.getRootNode());
    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        for (auto * child : node->children)
            stack.push(child);

        if (node->children.empty())
            plan.addResources(replaceReadingFromTable(*node, plan.nodes, context));
    }
}

static void addSetsFromSubqueries(QueryPlan & plan, std::vector<QueryPlanAndSets::SubqueryAndSets> subqueries_and_sets, const ContextPtr & context)
{
    if (subqueries_and_sets.empty())
        return;

    PreparedSets::Subqueries subqueries;
    subqueries.reserve(subqueries_and_sets.size());
    for (auto & item : subqueries_and_sets)
    {
        auto & subquery_plan = *item.subquery->getQueryPlan();
        QueryPlan::resolveReadFromTable(subquery_plan, context);
        addSetsFromSubqueries(subquery_plan, std::move(item.sets), context);
        subqueries.push_back(std::move(item.subquery));
    }

    addCreatingSetsStep(plan, std::move(subqueries), context);
}

QueryPlan QueryPlan::resolveStorages(QueryPlanAndSets plan_and_sets, const ContextPtr & context)
{
    auto & plan = plan_and_sets.plan;

    resolveReadFromTable(plan, context);

    makeSetsFromStorageSet(plan_and_sets.sets_from_storage, context);

    if (!plan_and_sets.subqueries.empty())
        addSetsFromSubqueries(plan, std::move(plan_and_sets.subqueries), context);

    return std::move(plan);
}

}
