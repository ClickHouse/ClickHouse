#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>

#include <Core/QueryProcessingStage.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/FieldToDataType.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Functions/FunctionsMiscellaneous.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

QueryPipeline buildDummyPipeline()
{
    ColumnsWithTypeAndName columns;
    auto string_data_type = std::make_shared<DataTypeString>();

    auto string_column = string_data_type->createColumn();
    string_column->insert("TestValue");
    columns.emplace_back(ColumnWithTypeAndName{std::move(string_column), string_data_type, "test_column"});

    Block block(columns);
    auto source = std::make_shared<SourceFromSingleChunk>(block);
    auto shell_input_pipe = Pipe(std::move(source));

    QueryPipeline pipeline(std::move(shell_input_pipe));
    return pipeline;
}

String dumpQueryPlan(QueryPlan & query_plan)
{
    WriteBufferFromOwnString query_plan_buffer;
    query_plan.explainPlan(query_plan_buffer, QueryPlan::ExplainPlanOptions{});
    return query_plan_buffer.str();
}

String dumpQueryPipeline(QueryPlan & query_plan)
{
    QueryPlan::ExplainPipelineOptions explain_pipeline;
    WriteBufferFromOwnString query_pipeline_buffer;
    query_plan.explainPipeline(query_pipeline_buffer, explain_pipeline);
    return query_pipeline_buffer.str();
}


struct QueryTreeActionsScopeNode
{
    explicit QueryTreeActionsScopeNode(ActionsDAGPtr actions_dag_, QueryTreeNodePtr scope_node_)
        : actions_dag(std::move(actions_dag_))
        , scope_node(std::move(scope_node_))
    {
        for (const auto & node : actions_dag->getNodes())
            node_name_to_node[node.result_name] = &node;
    }

    bool containsNode(const std::string & node_name)
    {
        return node_name_to_node.find(node_name) != node_name_to_node.end();
    }

    const ActionsDAG::Node * tryGetNode(const std::string & node_name)
    {
        auto it = node_name_to_node.find(node_name);
        if (it == node_name_to_node.end())
            return {};

        return it->second;
    }

    const ActionsDAG::Node * getNodeOrThrow(const std::string & node_name)
    {
        auto it = node_name_to_node.find(node_name);
        if (it == node_name_to_node.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "No node with name {}. There are only nodes {}",
                node_name,
                actions_dag->dumpNames());

        return it->second;
    }

    const ActionsDAG::Node * addInputColumnIfNecessary(const std::string & node_name, const DataTypePtr & column_type)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addInput(node_name, column_type);

        // std::cout << "QueryTreeActionsScopeNode::addInputColumnIfNecessary dag " << actions_dag << " node name " << node_name;
        // std::cout << " result node ptr " << node << std::endl;

        node_name_to_node[node->result_name] = node;

        return node;
    }

    const ActionsDAG::Node * addConstantIfNecessary(const std::string & node_name, const ColumnWithTypeAndName & column)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addColumn(column);

        // std::cout << "QueryTreeActionsScopeNode::addConstantIfNecessary dag " << actions_dag << " node name " << node_name;
        // std::cout << " result node ptr " << node << std::endl;

        node_name_to_node[node->result_name] = node;

        return node;
    }

    const ActionsDAG::Node * addFunctionIfNecessary(const std::string & node_name, ActionsDAG::NodeRawConstPtrs children, FunctionOverloadResolverPtr function)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addFunction(function, children, node_name);

        // std::cout << "QueryTreeActionsScopeNode::addFunctionIfNecessary dag " << actions_dag << " node name " << node_name;
        // std::cout << " result node ptr " << node << std::endl;

        node_name_to_node[node->result_name] = node;

        return node;
    }

    std::unordered_map<std::string_view, const ActionsDAG::Node *> node_name_to_node;
    ActionsDAGPtr actions_dag;
    QueryTreeNodePtr scope_node;
};

class QueryTreeActionsVisitor : public WithContext
{
public:
    explicit QueryTreeActionsVisitor(
        ActionsDAGPtr actions_dag,
        ContextPtr context_)
        : WithContext(context_)
    {
        actions_stack.emplace_back(std::move(actions_dag), nullptr);
    }

    ActionsDAG::NodeRawConstPtrs visit(QueryTreeNodePtr expression_node)
    {
        ActionsDAG::NodeRawConstPtrs result;

        if (auto * expression_list_node = expression_node->as<ListNode>())
        {
            for (auto & node : expression_list_node->getNodes())
            {
                auto [node_name, _] = visitImpl(node);
                result.push_back(actions_stack.front().getNodeOrThrow(node_name));
            }
        }
        else
        {
            auto [node_name, _] = visitImpl(expression_node);
            result.push_back(actions_stack.front().getNodeOrThrow(node_name));
        }

        return result;
    }

private:

    std::pair<std::string, size_t> visitImpl(QueryTreeNodePtr node)
    {
        if (auto * column_node = node->as<ColumnNode>())
            return visitColumn(*column_node);
        else if (auto * constant_node = node->as<ConstantNode>())
            return visitConstant(*constant_node);
        else if (auto * function_node = node->as<FunctionNode>())
            return visitFunction(*function_node);

        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected only column, constant or function node. Actual {}", node->formatASTForErrorMessage());
    }

    std::pair<std::string, size_t> visitColumn(ColumnNode & column)
    {
        const auto & column_name = column.getColumnName();

        Int64 actions_stack_size = static_cast<Int64>(actions_stack.size() - 1);
        for (Int64 i = actions_stack_size; i >= 0; --i)
        {
            actions_stack[i].addInputColumnIfNecessary(column_name, column.getColumnType());

            if (column.getColumnSource()->getNodeType() == QueryTreeNodeType::LAMBDA &&
                actions_stack[i].scope_node.get() == column.getColumnSource().get())
            {
                return {column_name, i};
            }
        }

        return {column_name, 0};
    }

    std::pair<std::string, size_t> visitConstant(ConstantNode & constant_node)
    {
        const auto & literal = constant_node.getConstantValue();

        auto constant_name = constant_node.getName();

        ColumnWithTypeAndName column;
        column.name = constant_name;
        column.type = constant_node.getResultType();
        column.column = column.type->createColumnConst(1, literal);

        actions_stack[0].addConstantIfNecessary(constant_name, column);

        size_t actions_stack_size = actions_stack.size();
        for (size_t i = 1; i < actions_stack_size; ++i)
        {
            auto & actions_stack_node = actions_stack[i];
            actions_stack_node.addInputColumnIfNecessary(constant_name, column.type);
        }

        return {constant_name, 0};
    }

    std::pair<std::string, size_t> visitLambda(QueryTreeNodePtr lambda_node_untyped)
    {
        auto & lambda_node = lambda_node_untyped->as<LambdaNode &>();
        auto result_type = lambda_node.getResultType();
        if (!result_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Lambda {} is not resolved during query analysis",
                lambda_node.formatASTForErrorMessage());

        // std::cout << "QueryTreeActionsVisitor::visitLambda " << lambda_node.formatASTForErrorMessage() << std::endl;
        // std::cout << "Lambda arguments nodes size " << lambda_node.getArguments().getNodes().size() << std::endl;

        NamesAndTypesList lambda_arguments_names_and_types;

        for (auto & lambda_node_argument : lambda_node.getArguments().getNodes())
        {
            auto lambda_argument_name = lambda_node_argument->getName();
            auto lambda_argument_type = lambda_node_argument->getResultType();
            // std::cout << "Lambda argument name " << lambda_argument_name;
            // std::cout << " type " << lambda_argument_type->getName() << std::endl;
            lambda_arguments_names_and_types.emplace_back(lambda_argument_name, lambda_argument_type);
        }

        size_t previous_scope_node_actions_stack_index = actions_stack.size() - 1;

        auto lambda_actions_dag = std::make_shared<ActionsDAG>();
        actions_stack.emplace_back(lambda_actions_dag, lambda_node_untyped);

        auto [node_name, level] = visitImpl(lambda_node.getExpression());
        auto lambda_result_node_name = node_name;
        lambda_actions_dag->getIndex().push_back(actions_stack.back().getNodeOrThrow(node_name));

        // std::cout << "Previous DAG nodes " << actions_stack[previous_scope_node_actions_stack_index].actions_dag.get() << std::endl;
        // for (const auto & previous_actions_node : actions_stack[previous_scope_node_actions_stack_index].actions_dag->getNodes())
        // {
        //     std::cout << "Node " << &previous_actions_node << " result name " << previous_actions_node.result_name << std::endl;
        //     std::cout << "Children " << previous_actions_node.children.size() << std::endl;
        //     for (const auto * child_node : previous_actions_node.children)
        //     {
        //         std::cout << "Child node " << child_node << " result name " << child_node->result_name << std::endl;
        //     }
        // }

        lambda_actions_dag->removeUnusedActions(Names(1, lambda_result_node_name));

        // std::cout << "Lambda actions DAG Node " << node_name << " level " << level << std::endl;
        // std::cout << "Lambda actions DAG " << lambda_actions_dag.get() << std::endl;
        // std::cout << lambda_actions_dag->dumpDAG() << std::endl;

        auto lambda_actions = std::make_shared<ExpressionActions>(
            lambda_actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));

        Names captured_column_names;
        ActionsDAG::NodeRawConstPtrs lambda_children;
        Names required_column_names = lambda_actions->getRequiredColumns();

        const auto & lambda_argument_names = lambda_node.getArgumentNames();

        for (const auto & required_column_name : required_column_names)
        {
            auto it = std::find_if(
                lambda_argument_names.begin(), lambda_argument_names.end(), [&](auto & value) { return value == required_column_name; });

            if (it == lambda_argument_names.end())
            {
                lambda_children.push_back(actions_stack[previous_scope_node_actions_stack_index].getNodeOrThrow(required_column_name));
                captured_column_names.push_back(required_column_name);
            }
        }

        auto lambda_hash = lambda_node.getTreeHash();
        std::string lambda_name = "__lambda_" + toString(lambda_hash.first) + '_' + toString(lambda_hash.second);

        auto function_capture = std::make_shared<FunctionCaptureOverloadResolver>(
            lambda_actions, captured_column_names, lambda_arguments_names_and_types, result_type, lambda_result_node_name);
        actions_stack.pop_back();

        if (level == actions_stack.size())
            --level;

        actions_stack[level].addFunctionIfNecessary(lambda_name, lambda_children, function_capture);

        size_t actions_stack_size = actions_stack.size();
        for (size_t i = level + 1; i < actions_stack_size; ++i)
        {
            auto & actions_stack_node = actions_stack[i];
            actions_stack_node.addInputColumnIfNecessary(lambda_name, result_type);
        }

        return {lambda_name, level};
    }

    std::pair<std::string, size_t> visitFunction(FunctionNode & function_node)
    {
        auto function_node_name = function_node.getName();

        if (function_node.getFunctionName() == "grouping")
        {
            size_t arguments_size = function_node.getArguments().getNodes().size();

            if (arguments_size == 0)
                throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function GROUPING expects at least one argument");
            else if (arguments_size > 64)
                throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Function GROUPING can have up to 64 arguments, but {} provided", arguments_size);

            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Function GROUPING is not supported");
        }

        if (function_node.isAggregateFunction())
        {
            size_t actions_stack_size = actions_stack.size();

            for (size_t i = 0; i < actions_stack_size; ++i)
            {
                auto & actions_stack_node = actions_stack[i];
                actions_stack_node.addInputColumnIfNecessary(function_node_name, function_node.getResultType());
            }

            return {function_node_name, 0};
        }

        auto & function_arguments = function_node.getArguments().getNodes();
        size_t function_arguments_size = function_arguments.size();

        Names function_arguments_node_names;
        function_arguments_node_names.reserve(function_arguments_size);

        size_t level = 0;
        for (auto & argument : function_arguments)
        {
            if (argument->getNodeType() == QueryTreeNodeType::LAMBDA)
            {
                auto [node_name, node_min_level] = visitLambda(argument);
                function_arguments_node_names.push_back(std::move(node_name));
                level = std::max(level, node_min_level);
                continue;
            }

            auto [node_name, node_min_level] = visitImpl(argument);
            function_arguments_node_names.push_back(std::move(node_name));
            level = std::max(level, node_min_level);
        }

        ActionsDAG::NodeRawConstPtrs children;
        children.reserve(function_arguments_size);

        for (auto & function_argument_node_name : function_arguments_node_names)
            children.push_back(actions_stack[level].getNodeOrThrow(function_argument_node_name));

        actions_stack[level].addFunctionIfNecessary(function_node_name, children, function_node.getFunction());

        size_t actions_stack_size = actions_stack.size();
        for (size_t i = level + 1; i < actions_stack_size; ++i)
        {
            auto & actions_stack_node = actions_stack[i];
            actions_stack_node.addInputColumnIfNecessary(function_node_name, function_node.getResultType());
        }

        return {function_node_name, level};
    }

    std::vector<QueryTreeActionsScopeNode> actions_stack;
};

class CollectSourceColumnsMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<CollectSourceColumnsMatcher, true, false>;

    struct Data
    {
        NameSet source_columns_set;
    };

    static void visit(QueryTreeNodePtr & node, Data & data)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        if (column_node->getColumnSource()->getNodeType() == QueryTreeNodeType::LAMBDA)
            return;

        /// Replace ALIAS column with expression

        if (column_node->hasAliasExpression())
        {
            node = column_node->getAliasExpression();
            visit(node, data);
            return;
        }

        data.source_columns_set.insert(column_node->getColumnName());
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &)
    {
        return true;
    }
};

using CollectSourceColumnsVisitor = CollectSourceColumnsMatcher::Visitor;

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const ASTPtr & query_,
    const SelectQueryOptions & select_query_options_,
    ContextPtr context_)
    : WithContext(context_)
    , query(query_)
    , select_query_options(select_query_options_)
{
    if (auto * select_with_union_query_typed = query->as<ASTSelectWithUnionQuery>())
    {
        auto & select_lists = select_with_union_query_typed->list_of_selects->as<ASTExpressionList &>();

        if (select_lists.children.size() == 1)
        {
            query = select_lists.children[0];
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "UNION is not supported");
        }
    }
    else if (auto * subquery = query->as<ASTSubquery>())
    {
        query = subquery->children[0];
    }
    else if (auto * select_query_typed = query_->as<ASTSelectQuery>())
    {
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected ASTSelectWithUnionQuery or ASTSelectQuery. Actual {}",
            query->formatForErrorMessage());
    }

    query_tree = buildQueryTree(query, context_);

    QueryTreePassManager query_tree_pass_manager(context_);
    addQueryTreePasses(query_tree_pass_manager);
    query_tree_pass_manager.run(query_tree);
}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const QueryTreeNodePtr & query_tree_,
    const SelectQueryOptions & select_query_options_,
    ContextPtr context_)
    : WithContext(context_)
    , query(query_tree_->toAST())
    , query_tree(query_tree_)
    , select_query_options(select_query_options_)
{
    if (query_tree_->getNodeType() != QueryTreeNodeType::QUERY)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected query node. Actual {}",
            query_tree_->formatASTForErrorMessage());

}

Block InterpreterSelectQueryAnalyzer::getSampleBlock()
{
    initializeQueryPlanIfNeeded();
    return query_plan.getCurrentDataStream().header;
}

BlockIO InterpreterSelectQueryAnalyzer::execute()
{
    initializeQueryPlanIfNeeded();

    QueryPlanOptimizationSettings optimization_settings;
    BuildQueryPipelineSettings build_pipeline_settings;
    auto pipeline_builder = query_plan.buildQueryPipeline(optimization_settings, build_pipeline_settings);

    BlockIO res;
    res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));

    return res;
}

void InterpreterSelectQueryAnalyzer::initializeQueryPlanIfNeeded()
{
    if (query_plan.isInitialized())
        return;

    auto & query_tree_typed = query_tree->as<QueryNode &>();

    ActionsDAGPtr action_dag = std::make_shared<ActionsDAG>();
    ColumnsWithTypeAndName inputs;

    CollectSourceColumnsVisitor::Data data;
    CollectSourceColumnsVisitor collect_source_columns_visitor(data);
    collect_source_columns_visitor.visit(query_tree);

    NameSet source_columns_set = std::move(data.source_columns_set);

    // std::cout << "DAG before " << action_dag.get() << " nodes " << action_dag->getNodes().size() << std::endl;
    // std::cout << action_dag->dumpDAG() << std::endl;

    QueryTreeActionsVisitor visitor(action_dag, getContext());
    auto projection_action_dag_nodes = visitor.visit(query_tree_typed.getProjectionNode());
    size_t projection_action_dag_nodes_size = projection_action_dag_nodes.size();

    // std::cout << "Projection action dag nodes size " << projection_action_dag_nodes_size << std::endl;
    // for (size_t i = 0; i < projection_action_dag_nodes_size; ++i)
    // {
    //     std::cout << "DAG node " << projection_action_dag_nodes[i] << std::endl;
    // }

    // std::cout << "DAG after " << action_dag.get() << " nodes " << action_dag->getNodes().size() << std::endl;
    // std::cout << action_dag->dumpDAG() << std::endl;

    auto & projection_nodes = query_tree_typed.getProjection().getNodes();
    size_t projection_nodes_size = projection_nodes.size();

    if (projection_nodes_size != projection_action_dag_nodes_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "QueryTree projection nodes size mismatch. Expected {}. Actual {}",
            projection_action_dag_nodes_size,
            projection_nodes_size);

    NamesWithAliases projection_names;
    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        auto & node = projection_nodes[i];
        const auto * action_dag_node = projection_action_dag_nodes[i];
        auto action_dag_node_name = action_dag_node->result_name;

        action_dag->getIndex().push_back(action_dag_node);

        if (node->hasAlias())
            projection_names.push_back({action_dag_node_name, node->getAlias()});
        else
            projection_names.push_back({action_dag_node_name, action_dag_node_name});
    }

    action_dag->project(projection_names);

    // std::cout << "Final DAG " << action_dag.get() << " nodes " << action_dag->getNodes().size() << std::endl;
    // std::cout << action_dag->dumpDAG() << std::endl;
    // std::cout << "Names " << action_dag->dumpNames() << std::endl;
    // std::cout << "Final DAG nodes " << std::endl;
    // for (const auto & node : action_dag->getNodes())
    // {
    //     std::cout << "Node " << &node << " result name " << node.result_name << std::endl;
    // }

    // std::cout << "Source columns " << source_columns_set.size() << std::endl;
    // for (const auto & source_column : source_columns_set)
    //     std::cout << source_column << std::endl;

    auto current_context = getContext();
    size_t max_block_size = current_context->getSettingsRef().max_block_size;
    size_t max_streams = current_context->getSettingsRef().max_threads;

    SelectQueryInfo query_info;
    query_info.original_query = query;
    query_info.query = query;

    auto * table_node = query_tree_typed.getFrom()->as<TableNode>();
    auto * table_function_node = query_tree_typed.getFrom()->as<TableFunctionNode>();
    auto * query_node = query_tree_typed.getFrom()->as<QueryNode>();

    if (table_node || table_function_node)
    {
        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto from_stage = storage->getQueryProcessingStage(current_context, select_query_options.to_stage, storage_snapshot, query_info);

        Names column_names(source_columns_set.begin(), source_columns_set.end());

        if (column_names.empty() && storage->getName() == "SystemOne")
            column_names.push_back("dummy");

        if (!column_names.empty())
            storage->read(query_plan, column_names, storage_snapshot, query_info, getContext(), from_stage, max_block_size, max_streams);

        /// Create step which reads from empty source if storage has no data.
        if (!query_plan.isInitialized())
        {
            auto source_header = storage_snapshot->getSampleBlockForColumns(column_names);
            Pipe pipe(std::make_shared<NullSource>(source_header));
            auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
            read_from_pipe->setStepDescription("Read from NullSource");
            query_plan.addStep(std::move(read_from_pipe));
        }
    }
    else if (query_node)
    {
        InterpreterSelectQueryAnalyzer interpeter(query_tree_typed.getFrom(), select_query_options, getContext());
        interpeter.initializeQueryPlanIfNeeded();
        query_plan = std::move(interpeter.query_plan);
    }
    else
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Only single table or query in FROM section are supported");
    }

    auto projection_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), action_dag);
    projection_step->setStepDescription("Projection");
    query_plan.addStep(std::move(projection_step));
}

}
