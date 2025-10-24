#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/Sources/NullSource.h>

#include <Analyzer/Resolve/IdentifierResolver.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/Context.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <Storages/StorageMerge.h>
#include <Planner/Utils.h>
#include <Core/Settings.h>

#include <stack>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsSetOperationMode union_default_mode;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_PARSE_TEXT;
}

Identifier parseTableIdentifier(const std::string & str, const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();

    Tokens tokens(str.data(), str.data() + str.size(), settings[Setting::max_query_size]);
    IParser::Pos pos(tokens, static_cast<unsigned>(settings[Setting::max_parser_depth]), static_cast<unsigned>(settings[Setting::max_parser_backtracks]));
    Expected expected;

    ParserCompoundIdentifier parser(false, false);
    ASTPtr res;
    if (!parser.parse(pos, res, expected))
        throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Cannot parse itable identifier ({})", str);

    return Identifier(std::move(res->as<ASTIdentifier>()->name_parts));
}

std::shared_ptr<TableNode> resolveTable(const Identifier & identifier, const ContextPtr & context)
{
    auto resolve_result = IdentifierResolver::tryResolveTableIdentifier(identifier, context);
    if (!resolve_result)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Unknown table {}", identifier.getFullName());

    return resolve_result;
}

static QueryTreeNodePtr resolveTableFunction(const ASTPtr & table_function, const ContextPtr & context)
{
    QueryTreeNodePtr query_tree_node = buildTableFunctionQueryTree(table_function, context);

    bool only_analyze = false;
    QueryAnalyzer analyzer(only_analyze);
    analyzer.resolve(query_tree_node, nullptr, context);

    return query_tree_node;
}

static ASTPtr makeASTForReadingColumns(const Names & names, ASTPtr table_expression)
{
    auto select = std::make_shared<ASTSelectQuery>();
    auto columns = std::make_shared<ASTExpressionList>();
    for (const auto & name : names)
        columns->children.push_back(std::make_shared<ASTIdentifier>(name));

    auto tables = std::make_shared<ASTTablesInSelectQuery>();
    auto table_element = std::make_shared<ASTTablesInSelectQueryElement>();
    table_element->children.push_back(table_expression);
    table_element->table_expression = std::move(table_expression);
    tables->children.push_back(std::move(table_element));

    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(columns));
    select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    return select;
}

static ASTPtr wrapWithUnion(ASTPtr select)
{
    auto select_with_union = std::make_shared<ASTSelectWithUnionQuery>();
    auto selects = std::make_shared<ASTExpressionList>();
    selects->children.push_back(select);
    select_with_union->list_of_selects = selects;
    select_with_union->children.push_back(select_with_union->list_of_selects);

    return select_with_union;
}

static QueryPlanResourceHolder replaceReadingFromTable(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const ContextPtr & context)
{
    const auto * reading_from_table = typeid_cast<const ReadFromTableStep *>(node.step.get());
    const auto * reading_from_table_function = typeid_cast<const ReadFromTableFunctionStep *>(node.step.get());
    if (!reading_from_table && !reading_from_table_function)
        return {};

    const auto & header = node.step->getOutputHeader();
    auto column_names = header->getNames();

    StoragePtr storage;
    StorageSnapshotPtr snapshot;
    SelectQueryInfo select_query_info;
    ASTPtr table_function_ast;

    if (reading_from_table)
    {
        Identifier identifier = parseTableIdentifier(reading_from_table->getTable(), context);
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
        table_function_ast = parseQuery(
            parser,
            serialized_ast,
            settings[Setting::max_query_size],
            settings[Setting::max_parser_depth],
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        {
            SelectIntersectExceptQueryVisitor::Data data{settings[Setting::intersect_default_mode], settings[Setting::except_default_mode]};
            SelectIntersectExceptQueryVisitor{data}.visit(table_function_ast);
        }

        {
            /// Normalize SelectWithUnionQuery
            NormalizeSelectWithUnionQueryVisitor::Data data{settings[Setting::union_default_mode]};
            NormalizeSelectWithUnionQueryVisitor{data}.visit(table_function_ast);
        }

        auto query_tree_node = resolveTableFunction(table_function_ast, context);
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

    ASTPtr query;
    bool is_storage_merge = typeid_cast<const StorageMerge *>(storage.get());
    if (storage->isRemote() || is_storage_merge)
    {
        auto table_expression = std::make_shared<ASTTableExpression>();
        if (table_function_ast)
        {
            table_expression->children.push_back(table_function_ast);
            table_expression->table_function = std::move(table_function_ast);
        }
        else
        {
            const auto & table_id = storage->getStorageID();
            auto table_identifier = std::make_shared<ASTTableIdentifier>(table_id.database_name, table_id.table_name);
            table_expression->children.push_back(table_identifier);
            table_identifier->uuid = table_id.uuid;
            table_expression->database_and_table_name = std::move(table_identifier);
        }

        query = makeASTForReadingColumns(column_names, std::move(table_expression));
        // std::cerr << query->dumpTree() << std::endl;
    }

    QueryPlan reading_plan;
    if (storage->isRemote() || is_storage_merge)
    {
        SelectQueryOptions options(QueryProcessingStage::FetchColumns);
        options.ignore_rename_columns = true;
        InterpreterSelectQueryAnalyzer interpreter(wrapWithUnion(std::move(query)), context, options);
        reading_plan = std::move(interpreter).extractQueryPlan();
    }
    else
    {
        SelectQueryOptions options(QueryProcessingStage::FetchColumns);
        auto storage_limits = std::make_shared<StorageLimitsList>();
        storage_limits->emplace_back(buildStorageLimits(*context, options));
        select_query_info.storage_limits = std::move(storage_limits);
        select_query_info.query = std::move(query);

        storage->read(
            reading_plan,
            column_names,
            snapshot,
            select_query_info,
            context,
            QueryProcessingStage::FetchColumns,
            context->getSettingsRef()[Setting::max_block_size],
            context->getSettingsRef()[Setting::max_threads]
        );
    }

    if (!reading_plan.isInitialized())
    {
        /// Create step which reads from empty source if storage has no data.
        auto source_header = std::make_shared<const Block>(snapshot->getSampleBlockForColumns(column_names));
        Pipe pipe(std::make_shared<NullSource>(source_header));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource");
        reading_plan.addStep(std::move(read_from_pipe));
    }

    auto converting_actions = ActionsDAG::makeConvertingActions(
        reading_plan.getCurrentHeader()->getColumnsWithTypeAndName(),
        header->getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);

    node.step = std::make_unique<ExpressionStep>(reading_plan.getCurrentHeader(), std::move(converting_actions));
    node.children = {reading_plan.getRootNode()};

    auto nodes_and_resource = QueryPlan::detachNodesAndResources(std::move(reading_plan));

    nodes.splice(nodes.end(), std::move(nodes_and_resource.first));
    return std::move(nodes_and_resource.second);
}

void QueryPlan::resolveStorages(const ContextPtr & context)
{
    std::stack<QueryPlan::Node *> stack;
    stack.push(getRootNode());
    while (!stack.empty())
    {
        auto * node = stack.top();
        stack.pop();

        if (const auto * delayed_creating_sets = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()))
        {
            for (const auto & set : delayed_creating_sets->getSets())
                set->getQueryPlan()->resolveStorages(context);
        }

        for (auto * child : node->children)
            stack.push(child);

        if (node->children.empty())
            addResources(replaceReadingFromTable(*node, nodes, context));
    }
}

}
