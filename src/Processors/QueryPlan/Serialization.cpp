#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>

#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Sources/NullSource.h>

#include <Analyzer/Resolve/IdentifierResolver.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>

#include <Columns/ColumnSet.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Planner/Utils.h>
#include <Storages/StorageSet.h>

#include <stack>

namespace DB
{

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
            String storage_name;
            readStringBinary(storage_name, in);
            res.sets_from_storage.emplace_back(QueryPlanAndSets::SetFromStorage{{hash, std::move(columns)}, std::move(storage_name)});
        }
        else if (kind == UInt8(SetSerializationKind::TupleValues))
        {
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

            res.sets_from_tuple.emplace_back(QueryPlanAndSets::SetFromTuple{{hash, std::move(columns)}, std::move(set_columns)});
        }
        else if (kind == UInt8(SetSerializationKind::SubqueryPlan))
        {
            auto plan_for_set = QueryPlan::deserialize(in);

            res.sets_from_subquery.emplace_back(QueryPlanAndSets::SetFromSubquery{
                {hash, std::move(columns)},
                std::make_unique<QueryPlan>(std::move(plan_for_set.plan)),
                std::move(plan_for_set.sets_from_subquery)});

            res.sets_from_storage.splice(res.sets_from_storage.end(), std::move(plan_for_set.sets_from_storage));
            res.sets_from_tuple.splice(res.sets_from_tuple.end(), std::move(plan_for_set.sets_from_tuple));
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

static QueryTreeNodePtr resolveTableFunction(const ASTPtr & table_function, const ContextPtr & context)
{
    QueryTreeNodePtr query_tree_node = buildTableFunctionQueryTree(table_function, context);

    bool only_analyze = false;
    QueryAnalyzer analyzer(only_analyze);
    analyzer.resolve(query_tree_node, nullptr, context);

    return query_tree_node;
}

static void makeSetsFromStorage(std::list<QueryPlanAndSets::SetFromStorage> sets, const ContextPtr & context)
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

static void makeSetsFromTuple(std::list<QueryPlanAndSets::SetFromTuple> sets, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    for (auto & set : sets)
    {
        SizeLimits size_limits = PreparedSets::getSizeLimitsForSet(settings);
        bool transform_null_in = settings.transform_null_in;

        auto future_set = std::make_shared<FutureSetFromTuple>(set.hash, std::move(set.set_columns), transform_null_in, size_limits);
        for (auto * column : set.columns)
            column->setData(future_set);
    }
}

static void makeSetsFromSubqueries(QueryPlan & plan, std::list<QueryPlanAndSets::SetFromSubquery> sets_from_subqueries, const ContextPtr & context)
{
    if (sets_from_subqueries.empty())
        return;

    const auto & settings = context->getSettingsRef();

    PreparedSets::Subqueries subqueries;
    subqueries.reserve(sets_from_subqueries.size());
    for (auto & set : sets_from_subqueries)
    {
        QueryPlan::resolveReadFromTable(*set.plan, context, nullptr);
        makeSetsFromSubqueries(*set.plan, std::move(set.sets), context);

        SizeLimits size_limits = PreparedSets::getSizeLimitsForSet(settings);
        bool transform_null_in = settings.transform_null_in;
        size_t max_size_for_index = settings.use_index_for_in_with_subqueries_max_values;

        auto future_set = std::make_shared<FutureSetFromSubquery>(
            set.hash, std::move(set.plan), nullptr, nullptr,
            transform_null_in, size_limits, max_size_for_index);

        for (auto * column : set.columns)
            column->setData(future_set);

        subqueries.push_back(std::move(future_set));
    }

    auto step = std::make_unique<DelayedCreatingSetsStep>(
        plan.getCurrentDataStream(),
        std::move(subqueries),
        context);

    plan.addStep(std::move(step));
}


static QueryPlanResourceHolder replaceReadingFromTable(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const ContextPtr & context, const ASTPtr & query)
{
    const auto * reading_from_table = typeid_cast<const ReadFromTableStep *>(node.step.get());
    const auto * reading_from_table_function = typeid_cast<const ReadFromTableFunctionStep *>(node.step.get());
    if (!reading_from_table && !reading_from_table_function)
        return {};

    StoragePtr storage;
    StorageSnapshotPtr snapshot;
    SelectQueryInfo select_query_info;

    if (reading_from_table)
    {
        Identifier identifier(reading_from_table->getTable());
        auto table_node = resolveTable(identifier, context);

        storage = table_node->getStorage();
        snapshot = table_node->getStorageSnapshot();
        select_query_info.table_expression_modifiers = reading_from_table->getTableExpressionModifiers();
    }
    else
    {
        auto serialized_ast = reading_from_table_function->getSerializedAST();
        ParserFunction parser(false, true);
        const auto & settings = context->getSettingsRef();
        auto ast = parseQuery(
            parser,
            serialized_ast,
            settings.max_query_size,
            settings.max_parser_depth,
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        auto query_tree_node = resolveTableFunction(ast, context);
        if (auto * table_function_node = query_tree_node->as<TableFunctionNode>())
        {
            storage = table_function_node->getStorage();
            snapshot = table_function_node->getStorageSnapshot();
        }
        else if (auto * table_node = query_tree_node->as<TableNode>())
        {
            storage = table_node->getStorage();
            snapshot = table_node->getStorageSnapshot();
        }
        else
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Unexpected query tree node type {}\n{}",
                query_tree_node->getNodeTypeName(),
                query_tree_node->dumpTree());

        select_query_info.table_expression_modifiers = reading_from_table_function->getTableExpressionModifiers();
    }

    const auto & header = node.step->getOutputStream().header;
    auto column_names = header.getNames();

    SelectQueryOptions options(QueryProcessingStage::QueryPlan);
    auto storage_limits = std::make_shared<StorageLimitsList>();
    storage_limits->emplace_back(buildStorageLimits(*context, options));
    select_query_info.storage_limits = std::move(storage_limits);
    /// Query is needed for StorageDistributed now.
    /// Distributed over Distributed is not supported properly now, but at least we can FetchColumns.
    select_query_info.query = query;

    QueryPlan reading_plan;
    storage->read(
        reading_plan,
        column_names,
        snapshot,
        select_query_info,
        context,
        QueryProcessingStage::FetchColumns,
        context->getSettingsRef().max_block_size,
        context->getSettingsRef().max_threads
    );

    if (!reading_plan.isInitialized())
    {
        /// Create step which reads from empty source if storage has no data.
        auto source_header = snapshot->getSampleBlockForColumns(column_names);
        Pipe pipe(std::make_shared<NullSource>(source_header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource");
        reading_plan.addStep(std::move(read_from_pipe));
    }

    auto converting_actions = ActionsDAG::makeConvertingActions(
        reading_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    node.step = std::make_unique<ExpressionStep>(reading_plan.getCurrentDataStream(), std::move(converting_actions));
    node.children = {reading_plan.getRootNode()};

    auto nodes_and_resource = QueryPlan::detachNodesAndResources(std::move(reading_plan));

    nodes.splice(nodes.end(), std::move(nodes_and_resource.first));
    return std::move(nodes_and_resource.second);
}

void QueryPlan::resolveReadFromTable(QueryPlan & plan, const ContextPtr & context, const ASTPtr & query)
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
            plan.addResources(replaceReadingFromTable(*node, plan.nodes, context, query));
    }
}

QueryPlan QueryPlan::resolveStorages(QueryPlanAndSets plan_and_sets, const ContextPtr & context, const ASTPtr & query)
{
    auto & plan = plan_and_sets.plan;

    resolveReadFromTable(plan, context, query);

    makeSetsFromStorage(std::move(plan_and_sets.sets_from_storage), context);
    makeSetsFromTuple(std::move(plan_and_sets.sets_from_tuple), context);
    makeSetsFromSubqueries(plan, std::move(plan_and_sets.sets_from_subquery), context);

    return std::move(plan);
}

}
