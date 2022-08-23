#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>

#include <Core/QueryProcessingStage.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>

#include <Columns/getLeastSuperColumn.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>

#include <Analyzer/Utils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/CastOverloadResolver.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/ArrayJoinAction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int INVALID_JOIN_ON_EXPRESSION;
}

/** ClickHouse query planner.
  *
  * TODO: JOIN support ASOF. JOIN support strictness. JOIN support constants. JOIN support ON t1.id = t1.id
  * TODO: JOIN drop unnecessary columns after ON, USING section
  * TODO: Support display names
  * TODO: Support RBAC. Support RBAC for ALIAS columns.
  * TODO: Support distributed query processing
  * TODO: Support PREWHERE
  * TODO: Support GROUP BY, HAVING
  * TODO: Support ORDER BY, LIMIT
  * TODO: Support WINDOW FUNCTIONS
  * TODO: Support DISTINCT
  * TODO: Support building sets for IN functions
  * TODO: Support trivial count optimization
  * TODO: Support totals, extremes
  * TODO: Support projections
  * TODO: Support read in order optimization
  * TODO: Simplify actions chain
  * TODO: UNION storage limits
  * TODO: Interpreter resources
  */

namespace
{

[[maybe_unused]] String dumpQueryPlan(QueryPlan & query_plan)
{
    WriteBufferFromOwnString query_plan_buffer;
    query_plan.explainPlan(query_plan_buffer, QueryPlan::ExplainPlanOptions{true, true, true, true});
    return query_plan_buffer.str();
}

[[maybe_unused]] String dumpQueryPipeline(QueryPlan & query_plan)
{
    QueryPlan::ExplainPipelineOptions explain_pipeline;
    WriteBufferFromOwnString query_pipeline_buffer;
    query_plan.explainPipeline(query_pipeline_buffer, explain_pipeline);
    return query_pipeline_buffer.str();
}

Block getCommonHeaderForUnion(const Blocks & headers)
{
    size_t num_selects = headers.size();
    Block common_header = headers.front();
    size_t num_columns = common_header.columns();

    for (size_t query_num = 1; query_num < num_selects; ++query_num)
    {
        if (headers[query_num].columns() != num_columns)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                            "Different number of columns in UNION elements: {} and {}",
                            common_header.dumpNames(),
                            headers[query_num].dumpNames());
    }

    std::vector<const ColumnWithTypeAndName *> columns(num_selects);

    for (size_t column_num = 0; column_num < num_columns; ++column_num)
    {
        for (size_t i = 0; i < num_selects; ++i)
            columns[i] = &headers[i].getByPosition(column_num);

        ColumnWithTypeAndName & result_elem = common_header.getByPosition(column_num);
        result_elem = getLeastSuperColumn(columns);
    }

    return common_header;
}

class ActionsChainNode;
using ActionsChainNodePtr = std::unique_ptr<ActionsChainNode>;
using ActionsChainNodes = std::vector<ActionsChainNodePtr>;

class ActionsChainNode
{
public:
    explicit ActionsChainNode(ActionsDAGPtr actions_, bool available_output_columns_only_aliases_ = false)
        : actions(std::move(actions_))
        , available_output_columns_only_aliases(available_output_columns_only_aliases_)
    {
        initialize();
    }

    [[maybe_unused]] ActionsDAGPtr & getActions()
    {
        return actions;
    }

    [[maybe_unused]] const ActionsDAGPtr & getActions() const
    {
        return actions;
    }

    const ColumnsWithTypeAndName & getAvailableOutputColumns() const
    {
        return available_output_columns;
    }

    const NameSet & getInputColumnNames() const
    {
        return input_columns_names;
    }

    const NameSet & getChildRequiredOutputColumnsNames() const
    {
        return child_required_output_columns_names;
    }

    void finalizeInputAndOutputColumns(NameSet & child_input_columns)
    {
        child_required_output_columns_names.clear();
        std::vector<const ActionsDAG::Node *> required_output_nodes;

        for (const auto & node : actions->getNodes())
        {
            auto it = child_input_columns.find(node.result_name);

            if (it == child_input_columns.end())
                continue;

            child_required_output_columns_names.insert(node.result_name);
            required_output_nodes.push_back(&node);
            child_input_columns.erase(it);
        }

        for (auto & required_output_node : required_output_nodes)
            actions->addOrReplaceInOutputs(*required_output_node);

        actions->removeUnusedActions();
        initialize();
    }

    void dump(WriteBuffer & buffer) const
    {
        buffer << "DAG" << '\n';
        buffer << actions->dumpDAG();
        if (!child_required_output_columns_names.empty())
        {
            buffer << "Child required output columns " << boost::join(child_required_output_columns_names, ", ");
            buffer << '\n';
        }
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }

    [[maybe_unused]] void addParentIndex(size_t parent_node_index)
    {
        parent_nodes_indices.push_back(parent_node_index);
    }

    void addParentIndices(const std::vector<size_t> & parent_nodes_indices_value)
    {
        parent_nodes_indices.insert(parent_nodes_indices.end(), parent_nodes_indices_value.begin(), parent_nodes_indices_value.end());
    }

    const std::vector<size_t> & getParentNodesIndices() const
    {
        return parent_nodes_indices;
    }

private:
    void initialize()
    {
        auto required_columns_names = actions->getRequiredColumnsNames();
        input_columns_names = NameSet(required_columns_names.begin(), required_columns_names.end());

        available_output_columns.clear();

        for (const auto & node : actions->getNodes())
        {
            if (available_output_columns_only_aliases)
            {
                if (node.type == ActionsDAG::ActionType::ALIAS)
                    available_output_columns.emplace_back(node.column, node.result_type, node.result_name);

                continue;
            }

            if (node.type == ActionsDAG::ActionType::INPUT ||
                node.type == ActionsDAG::ActionType::FUNCTION ||
                node.type == ActionsDAG::ActionType::ARRAY_JOIN)
                available_output_columns.emplace_back(node.column, node.result_type, node.result_name);
        }
    }

    ActionsDAGPtr actions;

    bool available_output_columns_only_aliases;

    NameSet input_columns_names;

    NameSet child_required_output_columns_names;

    ColumnsWithTypeAndName available_output_columns;

    std::vector<size_t> parent_nodes_indices;

};

class ActionsChain
{
public:
    void addNode(ActionsChainNodePtr node)
    {
        nodes.emplace_back(std::move(node));
    }

    [[maybe_unused]] const ActionsChainNodes & getNodes() const
    {
        return nodes;
    }

    ColumnsWithTypeAndName getAvailableOutputColumns(const std::vector<size_t> & nodes_indices)
    {
        ColumnsWithTypeAndName result;

        for (const auto & node_index : nodes_indices)
        {
            assert(node_index < nodes.size());
            const auto & node_available_output_columns = nodes[node_index]->getAvailableOutputColumns();
            result.insert(result.end(), node_available_output_columns.begin(), node_available_output_columns.end());
        }

        return result;
    }

    // ColumnsWithTypeAndName getOutputColumns(const std::vector<size_t> & nodes_indices)
    // {
    //     ColumnsWithTypeAndName result;

    //     for (const auto & node_index : nodes_indices)
    //     {
    //         assert(node_index < nodes.size());
    //         const auto & node_output_columns = nodes[node_index]->getActions()->getResultColumns();
    //         result.insert(result.end(), node_output_columns.begin(), node_output_columns.end());
    //     }

    //     return result;
    // }

    [[maybe_unused]] NameSet getInputColumnNames(const std::vector<size_t> & nodes_indices)
    {
        NameSet result;

        for (const auto & node_index : nodes_indices)
        {
            assert(node_index < nodes.size());
            const auto & node_input_column_names = nodes[node_index]->getInputColumnNames();
            result.insert(node_input_column_names.begin(), node_input_column_names.end());
        }

        return result;
    }

    [[maybe_unused]] size_t size() const
    {
        return nodes.size();
    }

    [[maybe_unused]] const ActionsChainNodePtr & at(size_t index) const
    {
        if (index >= nodes.size())
            throw std::out_of_range("actions chain access is out of range");

        return nodes[index];
    }

    [[maybe_unused]] ActionsChainNodePtr & at(size_t index)
    {
        if (index >= nodes.size())
            throw std::out_of_range("actions chain access is out of range");

        return nodes[index];
    }

    [[maybe_unused]] ActionsChainNodePtr & operator[](size_t index)
    {
        return nodes[index];
    }

    [[maybe_unused]] const ActionsChainNodePtr & operator[](size_t index) const
    {
        return nodes[index];
    }

    [[maybe_unused]] ActionsChainNode * getLastNode()
    {
        return nodes.back().get();
    }

    [[maybe_unused]] ActionsChainNode * getLastNodeOrThrow()
    {
        if (nodes.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ActionsChain is empty");

        return nodes.back().get();
    }

    size_t getLastNodeIndex()
    {
        return nodes.size() - 1;
    }

    [[maybe_unused]] size_t getLastNodeIndexOrThrow()
    {
        if (nodes.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ActionsChain is empty");

        return nodes.size() - 1;
    }

    void finalize()
    {
        if (nodes.empty())
            return;

        std::deque<size_t> nodes_indices_to_process;
        nodes_indices_to_process.push_front(nodes.size() - 1);

        /// For root node there are no columns required in child nodes
        NameSet empty_child_input_columns;
        nodes.back().get()->finalizeInputAndOutputColumns(empty_child_input_columns);

        while (!nodes_indices_to_process.empty())
        {
            auto node_index_to_process = nodes_indices_to_process.front();
            nodes_indices_to_process.pop_front();

            auto & node_to_process = nodes[node_index_to_process];

            const auto & parent_nodes_indices = node_to_process->getParentNodesIndices();
            auto input_columns_names_copy = node_to_process->getInputColumnNames();

            for (const auto & parent_node_index : parent_nodes_indices)
            {
                assert(parent_node_index < nodes.size());

                auto & parent_node = nodes[parent_node_index];
                parent_node->finalizeInputAndOutputColumns(input_columns_names_copy);
                nodes_indices_to_process.push_back(parent_node_index);
            }
        }
    }

    void dump(WriteBuffer & buffer) const
    {
        size_t nodes_size = nodes.size();

        for (size_t i = 0; i < nodes_size; ++i)
        {
            const auto & node = nodes[i];
            buffer << "Node " << i;

            const auto & parent_nodes_indices = node->getParentNodesIndices();
            if (!parent_nodes_indices.empty())
            {
                buffer << " parent nodes indices ";
                for (const auto & parent_node_index : parent_nodes_indices)
                    buffer << parent_node_index << ' ';
            }

            buffer << '\n';
            node->dump(buffer);

            buffer << '\n';
        }
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);
        return buffer.str();
    }

private:
    ActionsChainNodes nodes;
};

using ColumnIdentifier = std::string;

struct TableExpressionColumns
{
    /// Valid for table, table function, query table expression nodes
    NamesAndTypesList source_columns;

    /// Valid for table, table function, query table expression nodes
    NameSet source_columns_names;

    /// Valid only for table table expression node
    NameSet alias_columns;

    /// Valid for table, table function, query table expression nodes
    std::unordered_map<std::string, ColumnIdentifier> column_name_to_column_identifier;
};

using TableExpressionNodeToColumns = std::unordered_map<const IQueryTreeNode *, TableExpressionColumns>;

struct PlannerContext
{
    std::unordered_map<const IQueryTreeNode *, ColumnIdentifier> column_node_to_column_identifier;
    std::unordered_map<const IQueryTreeNode *, std::string> table_expression_node_to_identifier;

    TableExpressionNodeToColumns table_expression_node_to_columns;
    size_t column_identifier_counter = 0;

    ActionsChain actions_chain;

    ActionsDAGPtr where_actions;
    size_t where_actions_chain_node_index = 0;
    std::string where_action_node_name;
    ActionsDAGPtr projection_actions;

    ContextPtr query_context;

    ColumnIdentifier getColumnUniqueIdentifier(const IQueryTreeNode * column_source_node, std::string column_name = {})
    {
        auto column_unique_prefix = "__column_" + std::to_string(column_identifier_counter);
        ++column_identifier_counter;

        std::string table_expression_identifier;
        auto table_expression_identifier_it = table_expression_node_to_identifier.find(column_source_node);
        if (table_expression_identifier_it != table_expression_node_to_identifier.end())
            table_expression_identifier = table_expression_identifier_it->second;

        std::string debug_identifier_suffix;

        if (column_source_node->hasAlias())
        {
            debug_identifier_suffix += column_source_node->getAlias();
        }
        else if (const auto * table_source_node = column_source_node->as<TableNode>())
        {
            debug_identifier_suffix += table_source_node->getStorageID().getFullNameNotQuoted();
        }
        else
        {
            auto column_source_node_type = column_source_node->getNodeType();
            if (column_source_node_type == QueryTreeNodeType::JOIN)
                debug_identifier_suffix += "join";
            else if (column_source_node_type == QueryTreeNodeType::ARRAY_JOIN)
                debug_identifier_suffix += "array_join";
            else if (column_source_node_type == QueryTreeNodeType::TABLE_FUNCTION)
                debug_identifier_suffix += "table_function";
            else if (column_source_node_type == QueryTreeNodeType::QUERY)
                debug_identifier_suffix += "subquery";

            if (!table_expression_identifier.empty())
                debug_identifier_suffix += '_' + table_expression_identifier;
        }

        if (!column_name.empty())
            debug_identifier_suffix += '.' + column_name;

        if (!debug_identifier_suffix.empty())
            column_unique_prefix += '_' + debug_identifier_suffix;

        return column_unique_prefix;
    }

    ColumnIdentifier getColumnIdentifierOrThrow(const IQueryTreeNode * column_source_node)
    {
        assert(column_source_node->getNodeType() == QueryTreeNodeType::COLUMN);
        auto it = column_node_to_column_identifier.find(column_source_node);
        if (it == column_node_to_column_identifier.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column identifier is not initialized for column {}",
                column_source_node->formatASTForErrorMessage());

        return it->second;
    }
};

struct QueryTreeActionsScopeNode
{
    explicit QueryTreeActionsScopeNode(ActionsDAGPtr actions_dag_, QueryTreeNodePtr scope_node_)
        : actions_dag(std::move(actions_dag_))
        , scope_node(std::move(scope_node_))
    {
        for (const auto & node : actions_dag->getNodes())
            node_name_to_node[node.result_name] = &node;
    }

    [[maybe_unused]] bool containsNode(const std::string & node_name)
    {
        return node_name_to_node.find(node_name) != node_name_to_node.end();
    }

    [[maybe_unused]] const ActionsDAG::Node * tryGetNode(const std::string & node_name)
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

    const ActionsDAG::Node * addInputConstantColumnIfNecessary(const std::string & node_name, const ColumnWithTypeAndName & column)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addInput(column);
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

    const ActionsDAG::Node * addArrayJoinIfNecessary(const std::string & node_name, const ActionsDAG::Node * child)
    {
        auto it = node_name_to_node.find(node_name);
        if (it != node_name_to_node.end())
            return it->second;

        const auto * node = &actions_dag->addArrayJoin(*child, node_name);
        node_name_to_node[node->result_name] = node;

        return node;
    }

    std::unordered_map<std::string_view, const ActionsDAG::Node *> node_name_to_node;
    ActionsDAGPtr actions_dag;
    QueryTreeNodePtr scope_node;
};

class QueryTreeActionsVisitor
{
public:
    explicit QueryTreeActionsVisitor(ActionsDAGPtr actions_dag, const PlannerContext & planner_context_)
        : planner_context(planner_context_)
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

    using NodeNameAndNodeMinLevel = std::pair<std::string, size_t>;

    NodeNameAndNodeMinLevel visitImpl(QueryTreeNodePtr node)
    {
        if (auto * column_node = node->as<ColumnNode>())
            return visitColumn(node);
        else if (auto * constant_node = node->as<ConstantNode>())
            return visitConstant(node);
        else if (auto * function_node = node->as<FunctionNode>())
            return visitFunction(node);

        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected only column, constant or function node. Actual {}", node->formatASTForErrorMessage());
    }

    NodeNameAndNodeMinLevel visitColumn(const QueryTreeNodePtr & node)
    {
        auto column_node_name = getActionsDAGNodeName(node.get());
        const auto & column_node = node->as<ColumnNode &>();

        Int64 actions_stack_size = static_cast<Int64>(actions_stack.size() - 1);
        for (Int64 i = actions_stack_size; i >= 0; --i)
        {
            actions_stack[i].addInputColumnIfNecessary(column_node_name, column_node.getColumnType());

            if (column_node.getColumnSource()->getNodeType() == QueryTreeNodeType::LAMBDA &&
                actions_stack[i].scope_node.get() == column_node.getColumnSource().get())
            {
                return {column_node_name, i};
            }
        }

        return {column_node_name, 0};
    }

    NodeNameAndNodeMinLevel visitConstant(const QueryTreeNodePtr & node)
    {
        auto constant_node_name = getActionsDAGNodeName(node.get());
        const auto & constant_node = node->as<ConstantNode &>();
        const auto & literal = constant_node.getConstantValue();

        ColumnWithTypeAndName column;
        column.name = constant_node_name;
        column.type = constant_node.getResultType();
        column.column = column.type->createColumnConst(1, literal);

        actions_stack[0].addConstantIfNecessary(constant_node_name, column);

        size_t actions_stack_size = actions_stack.size();
        for (size_t i = 1; i < actions_stack_size; ++i)
        {
            auto & actions_stack_node = actions_stack[i];
            actions_stack_node.addInputConstantColumnIfNecessary(constant_node_name, column);
        }

        return {constant_node_name, 0};
    }

    NodeNameAndNodeMinLevel visitLambda(const QueryTreeNodePtr & node)
    {
        auto & lambda_node = node->as<LambdaNode &>();
        auto result_type = lambda_node.getResultType();
        if (!result_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Lambda {} is not resolved during query analysis",
                lambda_node.formatASTForErrorMessage());

        NamesAndTypesList lambda_arguments_names_and_types;

        for (auto & lambda_node_argument : lambda_node.getArguments().getNodes())
        {
            auto lambda_argument_name = lambda_node_argument->getName();
            auto lambda_argument_type = lambda_node_argument->getResultType();
            lambda_arguments_names_and_types.emplace_back(lambda_argument_name, lambda_argument_type);
        }

        size_t previous_scope_node_actions_stack_index = actions_stack.size() - 1;

        auto lambda_actions_dag = std::make_shared<ActionsDAG>();
        actions_stack.emplace_back(lambda_actions_dag, node);

        auto [lambda_expression_node_name, level] = visitImpl(lambda_node.getExpression());
        lambda_actions_dag->getOutputs().push_back(actions_stack.back().getNodeOrThrow(lambda_expression_node_name));
        lambda_actions_dag->removeUnusedActions(Names(1, lambda_expression_node_name));

        auto lambda_actions = std::make_shared<ExpressionActions>(
            lambda_actions_dag, ExpressionActionsSettings::fromContext(planner_context.query_context, CompileExpressions::yes));

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

        auto lambda_node_name = getActionsDAGNodeName(node.get());
        auto function_capture = std::make_shared<FunctionCaptureOverloadResolver>(
            lambda_actions, captured_column_names, lambda_arguments_names_and_types, result_type, lambda_expression_node_name);
        actions_stack.pop_back();

        if (level == actions_stack.size())
            --level;

        actions_stack[level].addFunctionIfNecessary(lambda_node_name, lambda_children, function_capture);

        size_t actions_stack_size = actions_stack.size();
        for (size_t i = level + 1; i < actions_stack_size; ++i)
        {
            auto & actions_stack_node = actions_stack[i];
            actions_stack_node.addInputColumnIfNecessary(lambda_node_name, result_type);
        }

        return {lambda_node_name, level};
    }

    NodeNameAndNodeMinLevel visitFunction(const QueryTreeNodePtr & node)
    {
        auto function_node_name = getActionsDAGNodeName(node.get());
        const auto & function_node = node->as<FunctionNode &>();

        if (function_node.getFunctionName() == "grouping")
        {
            size_t arguments_size = function_node.getArguments().getNodes().size();

            if (arguments_size == 0)
                throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function GROUPING expects at least one argument");
            else if (arguments_size > 64)
                throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Function GROUPING can have up to 64 arguments, but {} provided", arguments_size);

            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Function GROUPING is not supported");
        }
        else if (isNameOfInFunction(function_node.getFunctionName()))
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Function IN is not supported");
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

        const auto & function_arguments = function_node.getArguments().getNodes();
        size_t function_arguments_size = function_arguments.size();

        Names function_arguments_node_names;
        function_arguments_node_names.reserve(function_arguments_size);

        size_t level = 0;
        for (const auto & argument : function_arguments)
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

        if (function_node.getFunctionName() == "arrayJoin")
        {
            if (level != 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Expression in arrayJoin cannot depend on lambda argument: {} ",
                    function_arguments_node_names.at(0));

            actions_stack[level].addArrayJoinIfNecessary(function_node_name, children.at(0));
        }
        else
        {
            actions_stack[level].addFunctionIfNecessary(function_node_name, children, function_node.getFunction());
        }

        size_t actions_stack_size = actions_stack.size();
        for (size_t i = level + 1; i < actions_stack_size; ++i)
        {
            auto & actions_stack_node = actions_stack[i];
            actions_stack_node.addInputColumnIfNecessary(function_node_name, function_node.getResultType());
        }

        return {function_node_name, level};
    }

    String getActionsDAGNodeName(const IQueryTreeNode * node) const
    {
        String result;
        auto node_type = node->getNodeType();

        switch (node_type)
        {
            case QueryTreeNodeType::COLUMN:
            {
                auto it = planner_context.column_node_to_column_identifier.find(node);
                if (it == planner_context.column_node_to_column_identifier.end())
                    result = node->getName();
                else
                    result = it->second;

                break;
            }
            case QueryTreeNodeType::CONSTANT:
            {
                result = "__constant_" + node->getName();
                break;
            }
            case QueryTreeNodeType::FUNCTION:
            {
                const auto & function_node = node->as<FunctionNode &>();

                WriteBufferFromOwnString buffer;
                buffer << "__function_" + function_node.getFunctionName();

                const auto & function_parameters_nodes = function_node.getParameters().getNodes();

                if (!function_parameters_nodes.empty())
                {
                    buffer << '(';

                    size_t function_parameters_nodes_size = function_parameters_nodes.size();
                    for (size_t i = 0; i < function_parameters_nodes_size; ++i)
                    {
                        const auto & function_parameter_node = function_parameters_nodes[i];
                        getActionsDAGNodeName(function_parameter_node.get());

                        if (i + 1 != function_parameters_nodes_size)
                            buffer << ", ";
                    }

                    buffer << ')';
                }

                const auto & function_arguments_nodes = function_node.getArguments().getNodes();

                buffer << '(';

                size_t function_arguments_nodes_size = function_arguments_nodes.size();
                for (size_t i = 0; i < function_arguments_nodes_size; ++i)
                {
                    const auto & function_argument_node = function_arguments_nodes[i];
                    buffer << getActionsDAGNodeName(function_argument_node.get());

                    if (i + 1 != function_arguments_nodes_size)
                        buffer << ", ";
                }

                buffer << ')';

                result = buffer.str();
                break;
            }
            case QueryTreeNodeType::QUERY:
            {
                auto query_hash = node->getTreeHash();

                result = "__subquery_" + std::to_string(query_hash.first) + '_' + std::to_string(query_hash.second);
                break;
            }
            case QueryTreeNodeType::LAMBDA:
            {
                auto lambda_hash = node->getTreeHash();

                result = "__lambda_" + toString(lambda_hash.first) + '_' + toString(lambda_hash.second);
                break;
            }
            default:
            {
                result = node->getName();
                break;
            }
        }

        return result;
    }

    std::vector<QueryTreeActionsScopeNode> actions_stack;
    const PlannerContext & planner_context;
};

class CollectTableExpressionIdentifiersVisitor
{
public:
    void visit(const QueryTreeNodePtr & join_tree_node, PlannerContext & planner_context)
    {
        auto join_tree_node_type = join_tree_node->getNodeType();

        switch (join_tree_node_type)
        {
            case QueryTreeNodeType::QUERY:
                [[fallthrough]];
            case QueryTreeNodeType::UNION:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE:
                [[fallthrough]];
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                std::string table_expression_identifier = std::to_string(planner_context.table_expression_node_to_identifier.size());
                planner_context.table_expression_node_to_identifier.emplace(join_tree_node.get(), table_expression_identifier);
                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join_node = join_tree_node->as<JoinNode &>();
                visit(join_node.getLeftTableExpression(), planner_context);

                std::string table_expression_identifier = std::to_string(planner_context.table_expression_node_to_identifier.size());
                planner_context.table_expression_node_to_identifier.emplace(join_tree_node.get(), table_expression_identifier);

                visit(join_node.getRightTableExpression(), planner_context);
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & array_join_node = join_tree_node->as<ArrayJoinNode &>();
                visit(array_join_node.getTableExpression(), planner_context);

                std::string table_expression_identifier = std::to_string(planner_context.table_expression_node_to_identifier.size());
                planner_context.table_expression_node_to_identifier.emplace(join_tree_node.get(), table_expression_identifier);
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected query, table, table function, join or array join query node. Actual {}",
                    join_tree_node->formatASTForErrorMessage());
            }
        }
    }
};

class CollectSourceColumnsMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<CollectSourceColumnsMatcher, true, false>;

    struct Data
    {
        PlannerContext & planner_context;
    };

    static void visit(QueryTreeNodePtr & node, Data & data)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source_node = column_node->getColumnSource();
        auto column_source_node_type = column_source_node->getNodeType();

        if (column_source_node_type == QueryTreeNodeType::ARRAY_JOIN ||
            column_source_node_type == QueryTreeNodeType::LAMBDA)
            return;

        /// JOIN using expression
        if (column_node->hasExpression() && column_source_node->getNodeType() == QueryTreeNodeType::JOIN)
            return;

        auto & table_expression_node_to_columns = data.planner_context.table_expression_node_to_columns;
        auto & table_expression_column_node_to_column_identifier = data.planner_context.column_node_to_column_identifier;

        auto [it, _] = table_expression_node_to_columns.emplace(column_source_node.get(), TableExpressionColumns());
        auto & table_expression_columns = it->second;

        if (column_node->hasExpression())
        {
            /// Replace ALIAS column with expression
            table_expression_columns.alias_columns.insert(column_node->getColumnName());
            node = column_node->getExpression();
            visit(node, data);
            return;
        }

        if (column_source_node_type != QueryTreeNodeType::TABLE &&
            column_source_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
            column_source_node_type != QueryTreeNodeType::QUERY &&
            column_source_node_type != QueryTreeNodeType::UNION)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected table, table function, query or union column source. Actual {}",
                column_source_node->formatASTForErrorMessage());

        auto [source_columns_set_it, inserted] = it->second.source_columns_names.insert(column_node->getColumnName());

        if (inserted)
        {
            auto column_identifier = data.planner_context.getColumnUniqueIdentifier(column_source_node.get(), column_node->getColumnName());
            table_expression_column_node_to_column_identifier.emplace(column_node, column_identifier);
            it->second.column_name_to_column_identifier.emplace(column_node->getColumnName(), column_identifier);
            it->second.source_columns.emplace_back(column_node->getColumn());
        }
        else
        {
            auto column_identifier_it = it->second.column_name_to_column_identifier.find(column_node->getColumnName());
            if (column_identifier_it == it->second.column_name_to_column_identifier.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Column node {} column identifier is not initialized",
                    column_node->formatASTForErrorMessage());

            table_expression_column_node_to_column_identifier.emplace(column_node, column_identifier_it->second);
        }
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return child_node->getNodeType() != QueryTreeNodeType::QUERY;
    }
};

using CollectSourceColumnsVisitor = CollectSourceColumnsMatcher::Visitor;

ActionsDAGPtr convertExpressionNodeIntoDAG(const QueryTreeNodePtr & expression_node, const ColumnsWithTypeAndName & inputs, const PlannerContext & planner_context)
{
    ActionsDAGPtr action_dag = std::make_shared<ActionsDAG>(inputs);
    QueryTreeActionsVisitor actions_visitor(action_dag, planner_context);
    auto expression_dag_index_nodes = actions_visitor.visit(expression_node);
    action_dag->getOutputs().clear();

    for (auto & expression_dag_index_node : expression_dag_index_nodes)
        action_dag->getOutputs().push_back(expression_dag_index_node);

    return action_dag;
}

QueryPlan buildQueryPlanForJoinTreeNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context);

QueryPlan buildQueryPlanForTableExpression(QueryTreeNodePtr table_expression,
    SelectQueryInfo & table_expression_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context)
{
    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();
    auto * union_node = table_expression->as<UnionNode>();

    QueryPlan query_plan;

    /** Use default columns to support case when there are no columns in query.
      * Example: SELECT 1;
      */
    const auto & [it, _] = planner_context.table_expression_node_to_columns.emplace(table_expression.get(), TableExpressionColumns());
    auto & table_expression_columns = it->second;

    if (table_node || table_function_node)
    {
        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto from_stage = storage->getQueryProcessingStage(planner_context.query_context, select_query_options.to_stage, storage_snapshot, table_expression_query_info);

        Names column_names(table_expression_columns.source_columns_names.begin(), table_expression_columns.source_columns_names.end());

        std::optional<NameAndTypePair> read_additional_column;

        bool plan_has_multiple_table_expressions = planner_context.table_expression_node_to_columns.size() > 1;
        if (column_names.empty() && (plan_has_multiple_table_expressions || storage->getName() == "SystemOne"))
        {
            auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());
            read_additional_column = column_names_and_types.front();
        }

        if (read_additional_column)
        {
            auto column_identifier = planner_context.getColumnUniqueIdentifier(table_expression.get(), read_additional_column->name);
            column_names.push_back(read_additional_column->name);
            table_expression_columns.source_columns_names.emplace(read_additional_column->name);
            table_expression_columns.source_columns.emplace_back(*read_additional_column);
            table_expression_columns.column_name_to_column_identifier.emplace(read_additional_column->name, column_identifier);
        }

        if (!column_names.empty())
        {
            size_t max_block_size = planner_context.query_context->getSettingsRef().max_block_size;
            size_t max_streams = planner_context.query_context->getSettingsRef().max_threads;
            storage->read(query_plan, column_names, storage_snapshot, table_expression_query_info, planner_context.query_context, from_stage, max_block_size, max_streams);
        }

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
    else if (query_node || union_node)
    {
        InterpreterSelectQueryAnalyzer interpeter(table_expression, select_query_options, planner_context.query_context);
        interpeter.initializeQueryPlanIfNeeded();
        query_plan = std::move(interpeter).extractQueryPlan();
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function, query or union. Actual {}", table_expression->formatASTForErrorMessage());
    }

    auto rename_actions_dag = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());

    for (const auto & [column_name, column_identifier] : table_expression_columns.column_name_to_column_identifier)
    {
        auto position = query_plan.getCurrentDataStream().header.getPositionByName(column_name);
        const auto * node_to_rename = rename_actions_dag->getOutputs()[position];
        rename_actions_dag->getOutputs()[position] = &rename_actions_dag->addAlias(*node_to_rename, column_identifier);
    }

    auto rename_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), rename_actions_dag);
    rename_step->setStepDescription("Change column names to column identifiers");
    query_plan.addStep(std::move(rename_step));

    return query_plan;
}

class JoinClause
{
public:
    void addKey(const ActionsDAG::Node * left_key_node, const ActionsDAG::Node * right_key_node)
    {
        left_key_nodes.emplace_back(left_key_node);
        right_key_nodes.emplace_back(right_key_node);
    }

    void addCondition(JoinTableSide table_side, const ActionsDAG::Node * condition_node)
    {
        auto & filter_condition_nodes = table_side == JoinTableSide::Left ? left_filter_condition_nodes : right_filter_condition_nodes;
        filter_condition_nodes.push_back(condition_node);
    }

    const ActionsDAG::NodeRawConstPtrs & getLeftKeyNodes() const
    {
        return left_key_nodes;
    }

    const ActionsDAG::NodeRawConstPtrs & getRightKeyNodes() const
    {
        return right_key_nodes;
    }

    ActionsDAG::NodeRawConstPtrs & getLeftKeyNodes()
    {
        return left_key_nodes;
    }

    ActionsDAG::NodeRawConstPtrs & getRightKeyNodes()
    {
        return right_key_nodes;
    }

    const ActionsDAG::NodeRawConstPtrs & getLeftFilterConditionNodes() const
    {
        return left_filter_condition_nodes;
    }

    const ActionsDAG::NodeRawConstPtrs & getRightFilterConditionNodes() const
    {
        return right_filter_condition_nodes;
    }

    void clearConditionNodes(JoinTableSide table_side)
    {
        auto & filter_condition_nodes = table_side == JoinTableSide::Left ? left_filter_condition_nodes : right_filter_condition_nodes;
        filter_condition_nodes.clear();
    }

    void dump(WriteBuffer & buffer) const
    {
        auto dump_dag_nodes = [&](const ActionsDAG::NodeRawConstPtrs & dag_nodes)
        {
            String dag_nodes_dump;

            if (!dag_nodes.empty())
            {
                for (const auto & dag_node : dag_nodes)
                {
                    dag_nodes_dump += dag_node->result_name;
                    dag_nodes_dump += ", ";
                }

                dag_nodes_dump.pop_back();
                dag_nodes_dump.pop_back();
            }

            return dag_nodes_dump;
        };

        buffer << "left_key_nodes: " << dump_dag_nodes(left_key_nodes);
        buffer << " right_key_nodes: " << dump_dag_nodes(right_key_nodes);

        if (!left_filter_condition_nodes.empty())
            buffer << " left_condition_nodes: " + dump_dag_nodes(left_filter_condition_nodes);

        if (!right_filter_condition_nodes.empty())
            buffer << " left_condition_nodes: " + dump_dag_nodes(right_filter_condition_nodes);
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }
private:
    ActionsDAG::NodeRawConstPtrs left_key_nodes;
    ActionsDAG::NodeRawConstPtrs right_key_nodes;

    ActionsDAG::NodeRawConstPtrs left_filter_condition_nodes;
    ActionsDAG::NodeRawConstPtrs right_filter_condition_nodes;
};

using JoinClauses = std::vector<JoinClause>;

std::optional<JoinTableSide> extractJoinTableSideFromExpression(const ActionsDAG::Node * expression_root_node,
    const NameSet & left_table_expression_columns_names,
    const NameSet & right_table_expression_columns_names,
    const JoinNode & join_node)
{
    std::optional<JoinTableSide> table_side;
    std::vector<const ActionsDAG::Node *> nodes_to_process;
    nodes_to_process.push_back(expression_root_node);

    while (!nodes_to_process.empty())
    {
        const auto * node_to_process = nodes_to_process.back();
        nodes_to_process.pop_back();

        for (const auto & child : node_to_process->children)
            nodes_to_process.push_back(child);

        if (node_to_process->type != ActionsDAG::ActionType::INPUT)
            continue;

        const auto & input_name = node_to_process->result_name;

        bool left_table_expression_contains_input = left_table_expression_columns_names.contains(input_name);
        bool right_table_expression_contains_input = right_table_expression_columns_names.contains(input_name);

        if (!left_table_expression_contains_input && !right_table_expression_contains_input)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} actions has column {} that do not exist in left {} or right {} table expression columns",
                join_node.formatASTForErrorMessage(),
                input_name,
                boost::join(left_table_expression_columns_names, ", "),
                boost::join(right_table_expression_columns_names, ", "));

        auto input_table_side = left_table_expression_contains_input ? JoinTableSide::Left : JoinTableSide::Right;
        if (table_side && (*table_side) != input_table_side)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} join expression contains column from left and right table",
                join_node.formatASTForErrorMessage());

        table_side = input_table_side;
    }

    return table_side;
}

void buildJoinClause(ActionsDAGPtr join_expression_dag,
    const ActionsDAG::Node * join_expressions_actions_node,
    const NameSet & left_table_expression_columns_names,
    const NameSet & right_table_expression_columns_names,
    const JoinNode & join_node,
    JoinClause & join_clause)
{
    /// For and function go into children
    if (join_expressions_actions_node->function && join_expressions_actions_node->function->getName() == "and")
    {
        for (const auto & child : join_expressions_actions_node->children)
        {
            buildJoinClause(join_expression_dag,
                child,
                left_table_expression_columns_names,
                right_table_expression_columns_names,
                join_node,
                join_clause);
        }

        return;
    }

    if (join_expressions_actions_node->function && join_expressions_actions_node->function->getName() == "equals")
    {
        const auto * equals_left_child = join_expressions_actions_node->children.at(0);
        const auto * equals_right_child = join_expressions_actions_node->children.at(1);

        auto left_equals_expression_side_optional = extractJoinTableSideFromExpression(equals_left_child,
            left_table_expression_columns_names,
            right_table_expression_columns_names,
            join_node);

        auto right_equals_expression_side_optional = extractJoinTableSideFromExpression(equals_right_child,
            left_table_expression_columns_names,
            right_table_expression_columns_names,
            join_node);

        if (!left_equals_expression_side_optional && !right_equals_expression_side_optional)
        {
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} ON expression {} with constants is not supported",
                join_node.formatASTForErrorMessage(),
                join_expressions_actions_node->function->getName());
        }
        else if (left_equals_expression_side_optional && !right_equals_expression_side_optional)
        {
            join_clause.addCondition(*left_equals_expression_side_optional, join_expressions_actions_node);
        }
        else if (!left_equals_expression_side_optional && right_equals_expression_side_optional)
        {
            join_clause.addCondition(*right_equals_expression_side_optional, join_expressions_actions_node);
        }
        else
        {
            auto left_equals_expression_side = *left_equals_expression_side_optional;
            auto right_equals_expression_side = *right_equals_expression_side_optional;

            if (left_equals_expression_side != right_equals_expression_side)
            {
                const ActionsDAG::Node * left_key = equals_left_child;
                const ActionsDAG::Node * right_key = equals_right_child;

                if (left_equals_expression_side == JoinTableSide::Right)
                {
                    left_key = equals_right_child;
                    right_key = equals_left_child;
                }

                join_clause.addKey(left_key, right_key);
            }
            else
            {
                join_clause.addCondition(left_equals_expression_side, join_expressions_actions_node);
            }
        }

        return;
    }

    auto expression_side_optional = extractJoinTableSideFromExpression(join_expressions_actions_node,
        left_table_expression_columns_names,
        right_table_expression_columns_names,
        join_node);

    if (!expression_side_optional)
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} with constants is not supported",
                join_node.formatASTForErrorMessage());

    auto expression_side = *expression_side_optional;

    join_clause.addCondition(expression_side, join_expressions_actions_node);
}

struct JoinClausesAndActions
{
    JoinClauses join_clauses;
    ActionsDAGPtr join_expression_actions;
    ActionsDAGPtr left_join_expressions_actions;
    ActionsDAGPtr right_join_expressions_actions;
};

JoinClausesAndActions buildJoinClausesAndActions(const ColumnsWithTypeAndName & join_expression_input_columns,
    const ColumnsWithTypeAndName & left_table_expression_columns,
    const ColumnsWithTypeAndName & right_table_expression_columns,
    const JoinNode & join_node,
    const PlannerContext & planner_context)
{
    ActionsDAGPtr join_expression_actions = std::make_shared<ActionsDAG>(join_expression_input_columns);

    QueryTreeActionsVisitor join_expression_visitor(join_expression_actions, planner_context);
    auto join_expression_dag_node_raw_pointers = join_expression_visitor.visit(join_node.getJoinExpression());
    if (join_expression_dag_node_raw_pointers.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JOIN {} ON clause contains multiple expressions",
            join_node.formatASTForErrorMessage());

    const auto * join_expressions_actions_root_node = join_expression_dag_node_raw_pointers[0];
    if (!join_expressions_actions_root_node->function)
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "JOIN {} join expression expected function",
            join_node.formatASTForErrorMessage());

    size_t left_table_expression_columns_size = left_table_expression_columns.size();

    Names join_left_actions_names;
    join_left_actions_names.reserve(left_table_expression_columns_size);

    NameSet join_left_actions_names_set;
    join_left_actions_names_set.reserve(left_table_expression_columns_size);

    for (const auto & left_table_expression_column : left_table_expression_columns)
    {
        join_left_actions_names.push_back(left_table_expression_column.name);
        join_left_actions_names_set.insert(left_table_expression_column.name);
    }

    size_t right_table_expression_columns_size = right_table_expression_columns.size();

    Names join_right_actions_names;
    join_right_actions_names.reserve(right_table_expression_columns_size);

    NameSet join_right_actions_names_set;
    join_right_actions_names_set.reserve(right_table_expression_columns_size);

    for (const auto & right_table_expression_column : right_table_expression_columns)
    {
        join_right_actions_names.push_back(right_table_expression_column.name);
        join_right_actions_names_set.insert(right_table_expression_column.name);
    }

    JoinClausesAndActions result;
    result.join_expression_actions = join_expression_actions;

    const auto & function_name = join_expressions_actions_root_node->function->getName();
    if (function_name == "or")
    {
        for (const auto & child : join_expressions_actions_root_node->children)
        {
            result.join_clauses.emplace_back();

            buildJoinClause(join_expression_actions,
                child,
                join_left_actions_names_set,
                join_right_actions_names_set,
                join_node,
                result.join_clauses.back());
        }
    }
    else
    {
        result.join_clauses.emplace_back();

        buildJoinClause(join_expression_actions,
                join_expressions_actions_root_node,
                join_left_actions_names_set,
                join_right_actions_names_set,
                join_node,
                result.join_clauses.back());
    }

    auto and_function = FunctionFactory::instance().get("and", planner_context.query_context);

    auto add_necessary_name_if_needed = [&](JoinTableSide join_table_side, const String & name)
    {
        auto & necessary_names = join_table_side == JoinTableSide::Left ? join_left_actions_names : join_right_actions_names;
        auto & necessary_names_set = join_table_side == JoinTableSide::Left ? join_left_actions_names_set : join_right_actions_names_set;

        auto [_, inserted] = necessary_names_set.emplace(name);
        if (inserted)
            necessary_names.push_back(name);
    };

    for (auto & join_clause : result.join_clauses)
    {
        const auto & left_filter_condition_nodes = join_clause.getLeftFilterConditionNodes();
        if (!left_filter_condition_nodes.empty())
        {
            const ActionsDAG::Node * dag_filter_condition_node = nullptr;

            if (left_filter_condition_nodes.size() > 1)
                dag_filter_condition_node = &join_expression_actions->addFunction(and_function, left_filter_condition_nodes, {});
            else
                dag_filter_condition_node = left_filter_condition_nodes[0];

            join_clause.clearConditionNodes(JoinTableSide::Left);
            join_clause.addCondition(JoinTableSide::Left, dag_filter_condition_node);

            join_expression_actions->addOrReplaceInOutputs(*dag_filter_condition_node);

            add_necessary_name_if_needed(JoinTableSide::Left, dag_filter_condition_node->result_name);
        }

        const auto & right_filter_condition_nodes = join_clause.getRightFilterConditionNodes();
        if (!right_filter_condition_nodes.empty())
        {
            const ActionsDAG::Node * dag_filter_condition_node = nullptr;

            if (right_filter_condition_nodes.size() > 1)
                dag_filter_condition_node = &join_expression_actions->addFunction(and_function, right_filter_condition_nodes, {});
            else
                dag_filter_condition_node = right_filter_condition_nodes[0];

            join_clause.clearConditionNodes(JoinTableSide::Right);
            join_clause.addCondition(JoinTableSide::Right, dag_filter_condition_node);

            join_expression_actions->addOrReplaceInOutputs(*dag_filter_condition_node);

            add_necessary_name_if_needed(JoinTableSide::Right, dag_filter_condition_node->result_name);
        }

        assert(join_clause.getLeftKeyNodes().size() == join_clause.getRightKeyNodes().size());
        size_t join_clause_left_key_nodes_size = join_clause.getLeftKeyNodes().size();

        for (size_t i = 0; i < join_clause_left_key_nodes_size; ++i)
        {
            auto & left_key_node = join_clause.getLeftKeyNodes()[i];
            auto & right_key_node = join_clause.getRightKeyNodes()[i];

            if (!left_key_node->result_type->equals(*right_key_node->result_type))
            {
                DataTypePtr common_type;

                try
                {
                    common_type = getLeastSupertype(DataTypes{left_key_node->result_type, right_key_node->result_type});
                }
                catch (Exception & ex)
                {
                    ex.addMessage("JOIN {} cannot infer common type in ON section for keys. Left key {} type {}. Right key {} type {}",
                        join_node.formatASTForErrorMessage(),
                        left_key_node->result_name,
                        left_key_node->result_type->getName(),
                        right_key_node->result_name,
                        right_key_node->result_type->getName());
                }

                ColumnWithTypeAndName cast_column;
                cast_column.name = "__constant_" + common_type->getName();
                cast_column.column = DataTypeString().createColumnConst(0, common_type->getName());
                cast_column.type = std::make_shared<DataTypeString>();

                const ActionsDAG::Node * cast_type_constant_node = nullptr;

                if (!left_key_node->result_type->equals(*common_type))
                {
                    cast_type_constant_node = &join_expression_actions->addColumn(cast_column);

                    FunctionCastBase::Diagnostic diagnostic = {left_key_node->result_name, left_key_node->result_name};
                    FunctionOverloadResolverPtr func_builder_cast
                        = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(diagnostic);

                    ActionsDAG::NodeRawConstPtrs children = {left_key_node, cast_type_constant_node};
                    left_key_node = &join_expression_actions->addFunction(func_builder_cast, std::move(children), {});
                }

                if (!right_key_node->result_type->equals(*common_type))
                {
                    if (!cast_type_constant_node)
                        cast_type_constant_node = &join_expression_actions->addColumn(cast_column);

                    FunctionCastBase::Diagnostic diagnostic = {right_key_node->result_name, right_key_node->result_name};
                    FunctionOverloadResolverPtr func_builder_cast
                        = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(std::move(diagnostic));

                    ActionsDAG::NodeRawConstPtrs children = {right_key_node, cast_type_constant_node};
                    right_key_node = &join_expression_actions->addFunction(func_builder_cast, std::move(children), {});
                }
            }

            join_expression_actions->addOrReplaceInOutputs(*left_key_node);
            join_expression_actions->addOrReplaceInOutputs(*right_key_node);

            add_necessary_name_if_needed(JoinTableSide::Left, left_key_node->result_name);
            add_necessary_name_if_needed(JoinTableSide::Right, right_key_node->result_name);
        }
    }

    result.left_join_expressions_actions = join_expression_actions->clone();
    result.left_join_expressions_actions->removeUnusedActions(join_left_actions_names);

    result.right_join_expressions_actions = join_expression_actions->clone();
    result.right_join_expressions_actions->removeUnusedActions(join_right_actions_names);

    return result;
}

QueryPlan buildQueryPlanForJoinNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context)
{
    auto & join_node = join_tree_node->as<JoinNode &>();

    auto left_plan = buildQueryPlanForJoinTreeNode(join_node.getLeftTableExpression(),
        select_query_info,
        select_query_options,
        planner_context);
    auto left_plan_output_columns = left_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    auto right_plan = buildQueryPlanForJoinTreeNode(join_node.getRightTableExpression(),
        select_query_info,
        select_query_options,
        planner_context);
    auto right_plan_output_columns = right_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    if (join_node.getStrictness() == JoinStrictness::Asof)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "JOIN {} ASOF is not supported",
            join_node.formatASTForErrorMessage());

    JoinClausesAndActions join_clauses_and_actions;

    if (join_node.isOnJoinExpression())
    {
        auto join_expression_input_columns = left_plan_output_columns;
        join_expression_input_columns.insert(join_expression_input_columns.end(), right_plan_output_columns.begin(), right_plan_output_columns.end());

        join_clauses_and_actions = buildJoinClausesAndActions(join_expression_input_columns,
            left_plan_output_columns,
            right_plan_output_columns,
            join_node,
            planner_context);

        auto left_join_expressions_actions_step = std::make_unique<ExpressionStep>(left_plan.getCurrentDataStream(), join_clauses_and_actions.left_join_expressions_actions);
        left_join_expressions_actions_step->setStepDescription("JOIN actions");
        left_plan.addStep(std::move(left_join_expressions_actions_step));

        auto right_join_expressions_actions_step = std::make_unique<ExpressionStep>(right_plan.getCurrentDataStream(), join_clauses_and_actions.right_join_expressions_actions);
        right_join_expressions_actions_step->setStepDescription("JOIN actions");
        right_plan.addStep(std::move(right_join_expressions_actions_step));
    }

    std::unordered_map<ColumnIdentifier, DataTypePtr> left_plan_column_name_to_cast_type;
    std::unordered_map<ColumnIdentifier, DataTypePtr> right_plan_column_name_to_cast_type;

    if (join_node.isUsingJoinExpression())
    {
        auto & join_node_using_columns_list = join_node.getJoinExpression()->as<ListNode &>();
        for (auto & join_node_using_node : join_node_using_columns_list.getNodes())
        {
            auto & join_node_using_column_node = join_node_using_node->as<ColumnNode &>();
            auto & inner_columns_list = join_node_using_column_node.getExpressionOrThrow()->as<ListNode &>();

            auto & left_inner_column_node = inner_columns_list.getNodes().at(0);
            auto & left_inner_column = left_inner_column_node->as<ColumnNode &>();

            auto & right_inner_column_node = inner_columns_list.getNodes().at(1);
            auto & right_inner_column = right_inner_column_node->as<ColumnNode &>();

            const auto & join_node_using_column_node_type = join_node_using_column_node.getColumnType();
            if (!left_inner_column.getColumnType()->equals(*join_node_using_column_node_type))
            {
                auto left_inner_column_identifier = planner_context.getColumnIdentifierOrThrow(left_inner_column_node.get());
                left_plan_column_name_to_cast_type.emplace(left_inner_column_identifier, join_node_using_column_node_type);
            }

            if (!right_inner_column.getColumnType()->equals(*join_node_using_column_node_type))
            {
                auto right_inner_column_identifier = planner_context.getColumnIdentifierOrThrow(right_inner_column_node.get());
                right_plan_column_name_to_cast_type.emplace(right_inner_column_identifier, join_node_using_column_node_type);
            }
        }
    }

    auto join_cast_plan_output_nodes = [&](QueryPlan & plan_to_add_cast, std::unordered_map<std::string, DataTypePtr> & plan_column_name_to_cast_type)
    {
        auto cast_actions_dag = std::make_shared<ActionsDAG>(plan_to_add_cast.getCurrentDataStream().header.getColumnsWithTypeAndName());

        for (auto & output_node : cast_actions_dag->getOutputs())
        {
            auto it = plan_column_name_to_cast_type.find(output_node->result_name);
            if (it == plan_column_name_to_cast_type.end())
                continue;

            const auto & cast_type = it->second;
            auto cast_type_name = cast_type->getName();

            ColumnWithTypeAndName column;
            column.name = "__constant_" + cast_type_name;
            column.column = DataTypeString().createColumnConst(0, cast_type_name);
            column.type = std::make_shared<DataTypeString>();

            const auto * cast_type_constant_node = &cast_actions_dag->addColumn(std::move(column));

            FunctionCastBase::Diagnostic diagnostic = {output_node->result_name, output_node->result_name};
            FunctionOverloadResolverPtr func_builder_cast
                = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl(std::move(diagnostic));

            ActionsDAG::NodeRawConstPtrs children = {output_node, cast_type_constant_node};
            output_node = &cast_actions_dag->addFunction(func_builder_cast, std::move(children), output_node->result_name);
        }

        auto cast_join_columns_step
            = std::make_unique<ExpressionStep>(plan_to_add_cast.getCurrentDataStream(), std::move(cast_actions_dag));
        cast_join_columns_step->setStepDescription("Cast JOIN USING columns");
        plan_to_add_cast.addStep(std::move(cast_join_columns_step));
    };

    if (!left_plan_column_name_to_cast_type.empty())
        join_cast_plan_output_nodes(left_plan, left_plan_column_name_to_cast_type);

    if (!right_plan_column_name_to_cast_type.empty())
        join_cast_plan_output_nodes(right_plan, right_plan_column_name_to_cast_type);

    JoinKind join_kind = join_node.getKind();
    bool join_use_nulls = planner_context.query_context->getSettingsRef().join_use_nulls;
    auto to_nullable_function = FunctionFactory::instance().get("toNullable", planner_context.query_context);

    auto join_cast_plan_columns_to_nullable = [&](QueryPlan & plan_to_add_cast)
    {
        auto cast_actions_dag = std::make_shared<ActionsDAG>(plan_to_add_cast.getCurrentDataStream().header.getColumnsWithTypeAndName());

        for (auto & output_node : cast_actions_dag->getOutputs())
        {
            if (output_node->type == ActionsDAG::ActionType::INPUT && output_node->result_name.starts_with("__column"))
                output_node = &cast_actions_dag->addFunction(to_nullable_function, {output_node}, output_node->result_name);
        }

        auto cast_join_columns_step = std::make_unique<ExpressionStep>(plan_to_add_cast.getCurrentDataStream(), std::move(cast_actions_dag));
        cast_join_columns_step->setStepDescription("Cast JOIN columns to Nullable");
        plan_to_add_cast.addStep(std::move(cast_join_columns_step));
    };

    if (join_use_nulls)
    {
        if (isFull(join_kind))
        {
            join_cast_plan_columns_to_nullable(left_plan);
            join_cast_plan_columns_to_nullable(right_plan);
        }
        else if (isLeft(join_kind))
        {
            join_cast_plan_columns_to_nullable(right_plan);
        }
        else if (isRight(join_kind))
        {
            join_cast_plan_columns_to_nullable(left_plan);
        }
    }

    auto table_join = std::make_shared<TableJoin>();
    table_join->getTableJoin() = join_node.toASTTableJoin()->as<ASTTableJoin &>();
    if (join_node.getKind() == JoinKind::Comma)
        table_join->getTableJoin().kind = JoinKind::Cross;
    table_join->getTableJoin().strictness = JoinStrictness::All;

    if (join_node.isOnJoinExpression())
    {
        const auto & join_clauses = join_clauses_and_actions.join_clauses;
        auto & table_join_clauses = table_join->getClauses();

        for (const auto & join_clause : join_clauses)
        {
            table_join_clauses.emplace_back();
            auto & table_join_clause = table_join_clauses.back();

            const auto & join_clause_left_key_nodes = join_clause.getLeftKeyNodes();
            const auto & join_clause_right_key_nodes = join_clause.getRightKeyNodes();

            size_t join_clause_key_nodes_size = join_clause_left_key_nodes.size();
            assert(join_clause_key_nodes_size == join_clause_right_key_nodes.size());

            for (size_t i = 0; i < join_clause_key_nodes_size; ++i)
            {
                table_join_clause.key_names_left.push_back(join_clause_left_key_nodes[i]->result_name);
                table_join_clause.key_names_right.push_back(join_clause_right_key_nodes[i]->result_name);
            }

            const auto & join_clause_get_left_filter_condition_nodes = join_clause.getLeftFilterConditionNodes();
            if (!join_clause_get_left_filter_condition_nodes.empty())
            {
                if (join_clause_get_left_filter_condition_nodes.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "JOIN {} left filter conditions size must be 1. Actual {}",
                        join_node.formatASTForErrorMessage(),
                        join_clause_get_left_filter_condition_nodes.size());

                const auto & join_clause_left_filter_condition_name = join_clause_get_left_filter_condition_nodes[0]->result_name;
                table_join_clause.analyzer_left_filter_condition_column_name = join_clause_left_filter_condition_name;
            }

            const auto & join_clause_get_right_filter_condition_nodes = join_clause.getRightFilterConditionNodes();
            if (!join_clause_get_right_filter_condition_nodes.empty())
            {
                if (join_clause_get_right_filter_condition_nodes.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "JOIN {} right filter conditions size must be 1. Actual {}",
                        join_node.formatASTForErrorMessage(),
                        join_clause_get_right_filter_condition_nodes.size());

                const auto & join_clause_right_filter_condition_name = join_clause_get_right_filter_condition_nodes[0]->result_name;
                table_join_clause.analyzer_right_filter_condition_column_name = join_clause_right_filter_condition_name;
            }
        }
    }
    else if (join_node.isUsingJoinExpression())
    {
        auto & table_join_clauses = table_join->getClauses();
        table_join_clauses.emplace_back();
        auto & table_join_clause = table_join_clauses.back();

        auto & using_list = join_node.getJoinExpression()->as<ListNode &>();

        for (auto & join_using_node : using_list.getNodes())
        {
            auto & join_using_column_node = join_using_node->as<ColumnNode &>();
            if (!join_using_column_node.getExpression() ||
                join_using_column_node.getExpression()->getNodeType() != QueryTreeNodeType::LIST)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "JOIN {} column in USING does not have inner columns",
                    join_node.formatASTForErrorMessage());

            auto & using_join_columns_list = join_using_column_node.getExpression()->as<ListNode &>();
            auto & using_join_left_join_column_node = using_join_columns_list.getNodes().at(0);
            auto & using_join_right_join_column_node = using_join_columns_list.getNodes().at(1);

            auto left_column_identifier_it = planner_context.column_node_to_column_identifier.find(using_join_left_join_column_node.get());
            auto right_column_identifier_it = planner_context.column_node_to_column_identifier.find(using_join_right_join_column_node.get());

            table_join_clause.key_names_left.push_back(left_column_identifier_it->second);
            table_join_clause.key_names_right.push_back(right_column_identifier_it->second);
        }
    }

    auto left_table_names = left_plan.getCurrentDataStream().header.getNames();
    NameSet left_table_names_set(left_table_names.begin(), left_table_names.end());

    auto columns_from_joined_table = right_plan.getCurrentDataStream().header.getNamesAndTypesList();
    table_join->setColumnsFromJoinedTable(columns_from_joined_table, left_table_names_set, "");

    for (auto & column_from_joined_table : columns_from_joined_table)
    {
        if (column_from_joined_table.name.starts_with("__column"))
            table_join->addJoinedColumn(column_from_joined_table);
    }

    size_t max_block_size = planner_context.query_context->getSettingsRef().max_block_size;
    size_t max_streams = planner_context.query_context->getSettingsRef().max_threads;

    JoinPtr join_ptr = std::make_shared<HashJoin>(table_join, right_plan.getCurrentDataStream().header, false /*any_take_last_row*/);
    QueryPlanStepPtr join_step = std::make_unique<JoinStep>(
        left_plan.getCurrentDataStream(),
        right_plan.getCurrentDataStream(),
        join_ptr,
        max_block_size,
        max_streams,
        false /*optimize_read_in_order*/);

    join_step->setStepDescription(fmt::format("JOIN {}", JoinPipelineType::FillRightFirst));

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

    auto result_plan = QueryPlan();
    result_plan.unitePlans(std::move(join_step), {std::move(plans)});

    return result_plan;
}

QueryPlan buildQueryPlanForArrayJoinNode(QueryTreeNodePtr table_expression,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context)
{
    auto & array_join_node = table_expression->as<ArrayJoinNode &>();

    auto left_plan = buildQueryPlanForJoinTreeNode(array_join_node.getTableExpression(),
        select_query_info,
        select_query_options,
        planner_context);
    auto left_plan_output_columns = left_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    ActionsDAGPtr array_join_action_dag = std::make_shared<ActionsDAG>(left_plan_output_columns);
    QueryTreeActionsVisitor actions_visitor(array_join_action_dag, planner_context);

    NameSet array_join_columns;
    for (auto & array_join_expression : array_join_node.getJoinExpressions().getNodes())
    {
        auto & array_join_expression_column = array_join_expression->as<ColumnNode &>();
        const auto & array_join_column_name = array_join_expression_column.getColumnName();
        array_join_columns.insert(array_join_column_name);

        auto expression_dag_index_nodes = actions_visitor.visit(array_join_expression_column.getExpressionOrThrow());
        for (auto & expression_dag_index_node : expression_dag_index_nodes)
        {
            const auto * array_join_column_node = &array_join_action_dag->addAlias(*expression_dag_index_node, array_join_column_name);
            array_join_action_dag->getOutputs().push_back(array_join_column_node);
        }
    }

    auto array_join_actions = std::make_unique<ExpressionStep>(left_plan.getCurrentDataStream(), array_join_action_dag);
    array_join_actions->setStepDescription("ARRAY JOIN actions");
    left_plan.addStep(std::move(array_join_actions));

    auto array_join_action = std::make_shared<ArrayJoinAction>(array_join_columns, array_join_node.isLeft(), planner_context.query_context);
    auto array_join_step = std::make_unique<ArrayJoinStep>(left_plan.getCurrentDataStream(), std::move(array_join_action));
    array_join_step->setStepDescription("ARRAY JOIN");
    left_plan.addStep(std::move(array_join_step));

    return left_plan;
}

QueryPlan buildQueryPlanForJoinTreeNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context)
{
    auto join_tree_node_type = join_tree_node->getNodeType();

    switch (join_tree_node_type)
    {
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            SelectQueryInfo table_expression_query_info = select_query_info;
            return buildQueryPlanForTableExpression(join_tree_node, table_expression_query_info, select_query_options, planner_context);
        }
        case QueryTreeNodeType::JOIN:
        {
            return buildQueryPlanForJoinNode(join_tree_node, select_query_info, select_query_options, planner_context);
        }
        case QueryTreeNodeType::ARRAY_JOIN:
        {
            return buildQueryPlanForArrayJoinNode(join_tree_node, select_query_info, select_query_options, planner_context);
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected query, table, table function, join or array join query node. Actual {}",
                join_tree_node->formatASTForErrorMessage());
        }
    }
}

}

InterpreterSelectQueryAnalyzer::InterpreterSelectQueryAnalyzer(
    const ASTPtr & query_,
    const SelectQueryOptions & select_query_options_,
    ContextPtr context_)
    : WithContext(context_)
    , query(query_)
    , select_query_options(select_query_options_)
{
    if (query->as<ASTSelectWithUnionQuery>() || query->as<ASTSelectQuery>())
    {
    }
    else if (auto * subquery = query->as<ASTSubquery>())
    {
        query = subquery->children[0];
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
    if (query_tree->getNodeType() != QueryTreeNodeType::QUERY &&
        query_tree->getNodeType() != QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Expected QUERY or UNION node. Actual {}",
            query_tree->formatASTForErrorMessage());

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

    auto current_context = getContext();

    if (auto * union_query_tree = query_tree->as<UnionNode>())
    {
        auto union_mode = union_query_tree->getUnionMode();
        if (union_mode == SelectUnionMode::Unspecified)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION mode must be initialized");

        std::vector<std::unique_ptr<QueryPlan>> query_plans;
        Blocks query_plans_headers;

        for (auto & query_node : union_query_tree->getQueries().getNodes())
        {
            InterpreterSelectQueryAnalyzer interpeter(query_node, select_query_options, current_context);
            interpeter.initializeQueryPlanIfNeeded();
            auto query_node_plan = std::make_unique<QueryPlan>(std::move(interpeter).extractQueryPlan());
            query_plans_headers.push_back(query_node_plan->getCurrentDataStream().header);
            query_plans.push_back(std::move(query_node_plan));
        }

        Block union_common_header = getCommonHeaderForUnion(query_plans_headers);
        DataStreams query_plans_streams;
        query_plans_streams.reserve(query_plans.size());

        for (auto & query_node_plan : query_plans)
        {
            if (blocksHaveEqualStructure(query_node_plan->getCurrentDataStream().header, union_common_header))
                continue;

            auto actions_dag = ActionsDAG::makeConvertingActions(
                    query_node_plan->getCurrentDataStream().header.getColumnsWithTypeAndName(),
                    union_common_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Position);
            auto converting_step = std::make_unique<ExpressionStep>(query_node_plan->getCurrentDataStream(), std::move(actions_dag));
            converting_step->setStepDescription("Conversion before UNION");
            query_node_plan->addStep(std::move(converting_step));

            query_plans_streams.push_back(query_node_plan->getCurrentDataStream());
        }

        const auto & settings = current_context->getSettingsRef();
        auto max_threads = settings.max_threads;

        if (union_mode == SelectUnionMode::ALL || union_mode == SelectUnionMode::DISTINCT)
        {
            auto union_step = std::make_unique<UnionStep>(std::move(query_plans_streams), max_threads);
            query_plan.unitePlans(std::move(union_step), std::move(query_plans));

            if (union_query_tree->getUnionMode() == SelectUnionMode::DISTINCT)
            {
                /// Add distinct transform
                SizeLimits limits(settings.max_rows_in_distinct, settings.max_bytes_in_distinct, settings.distinct_overflow_mode);

                auto distinct_step = std::make_unique<DistinctStep>(
                    query_plan.getCurrentDataStream(),
                    limits,
                    0 /*limit hint*/,
                    query_plan.getCurrentDataStream().header.getNames(),
                    false /*pre distinct*/,
                    settings.optimize_distinct_in_order);

                query_plan.addStep(std::move(distinct_step));
            }
        }
        else if (union_mode == SelectUnionMode::INTERSECT || union_mode == SelectUnionMode::EXCEPT)
        {
            IntersectOrExceptStep::Operator intersect_or_except_operator = IntersectOrExceptStep::Operator::INTERSECT;
            if (union_mode == SelectUnionMode::EXCEPT)
                intersect_or_except_operator = IntersectOrExceptStep::Operator::EXCEPT;

            auto union_step = std::make_unique<IntersectOrExceptStep>(std::move(query_plans_streams), intersect_or_except_operator, max_threads);
            query_plan.unitePlans(std::move(union_step), std::move(query_plans));
        }

        return;
    }

    auto & query_node = query_tree->as<QueryNode &>();

    SelectQueryInfo select_query_info;
    select_query_info.original_query = query;
    select_query_info.query = query;

    PlannerContext planner_context;
    planner_context.query_context = getContext();

    CollectTableExpressionIdentifiersVisitor collect_table_expression_identifiers_visitor;
    collect_table_expression_identifiers_visitor.visit(query_node.getJoinTree(), planner_context);

    CollectSourceColumnsVisitor::Data data {planner_context};
    CollectSourceColumnsVisitor collect_source_columns_visitor(data);
    collect_source_columns_visitor.visit(query_tree);

    query_plan = buildQueryPlanForJoinTreeNode(query_node.getJoinTree(), select_query_info, select_query_options, planner_context);
    std::optional<std::vector<size_t>> action_chain_node_parent_indices;

    if (query_node.hasWhere())
    {
        ColumnsWithTypeAndName where_input;
        if (action_chain_node_parent_indices)
            planner_context.actions_chain.getAvailableOutputColumns(*action_chain_node_parent_indices);
        else
            where_input = query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

        planner_context.where_actions = convertExpressionNodeIntoDAG(query_node.getWhere(), where_input, planner_context);
        planner_context.where_action_node_name = planner_context.where_actions->getOutputs().at(0)->result_name;

        auto where_actions_node = std::make_unique<ActionsChainNode>(planner_context.where_actions);
        if (action_chain_node_parent_indices)
            where_actions_node->addParentIndices(*action_chain_node_parent_indices);

        planner_context.actions_chain.addNode(std::move(where_actions_node));
        action_chain_node_parent_indices = {planner_context.actions_chain.getLastNodeIndex()};
        planner_context.where_actions_chain_node_index = planner_context.actions_chain.size();
    }

    ColumnsWithTypeAndName projection_input;
    if (action_chain_node_parent_indices)
        planner_context.actions_chain.getAvailableOutputColumns(*action_chain_node_parent_indices);
    else
        projection_input = query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    planner_context.projection_actions = convertExpressionNodeIntoDAG(query_node.getProjectionNode(), projection_input, planner_context);

    auto projection_actions_node = std::make_unique<ActionsChainNode>(planner_context.projection_actions);
    if (action_chain_node_parent_indices)
        projection_actions_node->addParentIndices(*action_chain_node_parent_indices);
    planner_context.actions_chain.addNode(std::move(projection_actions_node));

    const auto & projection_action_dag_nodes = planner_context.projection_actions->getOutputs();
    size_t projection_action_dag_nodes_size = projection_action_dag_nodes.size();

    auto & projection_nodes = query_node.getProjection().getNodes();
    size_t projection_nodes_size = projection_nodes.size();

    if (projection_nodes_size != projection_action_dag_nodes_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "QueryTree projection nodes size mismatch. Expected {}. Actual {}",
            projection_action_dag_nodes_size,
            projection_nodes_size);

    NamesWithAliases projection_names;

    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        auto & node = projection_nodes[i];
        auto node_name = node->getName();
        const auto * action_dag_node = projection_action_dag_nodes[i];
        const auto & actions_dag_node_name = action_dag_node->result_name;

        if (node->hasAlias())
            projection_names.push_back({actions_dag_node_name, node->getAlias()});
        else
            projection_names.push_back({actions_dag_node_name, node_name});
    }

    planner_context.projection_actions->project(projection_names);

    // std::cout << "Chain dump before finalize" << std::endl;
    // std::cout << planner_context.actions_chain.dump() << std::endl;

    planner_context.actions_chain.finalize();

    // std::cout << "Chain dump after finalize" << std::endl;
    // std::cout << planner_context.actions_chain.dump() << std::endl;

    if (query_node.hasWhere())
    {
        auto & where_actions_chain_node = planner_context.actions_chain.at(planner_context.where_actions_chain_node_index);
        bool remove_filter = !where_actions_chain_node->getChildRequiredOutputColumnsNames().contains(planner_context.where_action_node_name);
        auto where_step = std::make_unique<FilterStep>(query_plan.getCurrentDataStream(),
            planner_context.where_actions,
            planner_context.where_action_node_name,
            remove_filter);
        where_step->setStepDescription("WHERE");
        query_plan.addStep(std::move(where_step));
    }

    // std::cout << "Query plan dump" << std::endl;
    // std::cout << dumpQueryPlan(query_plan) << std::endl;

    auto projection_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), planner_context.projection_actions);
    projection_step->setStepDescription("Projection");
    query_plan.addStep(std::move(projection_step));
}

}
