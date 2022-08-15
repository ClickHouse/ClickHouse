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
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionFactory.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin.h>


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
  * TODO: JOIN support columns cast. JOIN support ASOF. JOIN support strictness.
  * TODO: Support RBAC. Support RBAC for ALIAS columns.
  * TODO: Support distributed query processing
  * TODO: Support PREWHERE
  * TODO: Support GROUP BY, HAVING
  * TODO: Support ORDER BY, LIMIT
  * TODO: Support WINDOW FUNCTIONS
  * TODO: Support DISTINCT
  * TODO: Support ArrayJoin
  * TODO: Support building sets for IN functions
  * TODO: Support trivial count optimization
  * TODO: Support totals, extremes
  * TODO: Support projections
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

struct TableExpressionColumns
{
    NamesAndTypesList all_columns;
    NameSet all_columns_names_set;

    NamesAndTypesList source_input_columns;
    NameSet source_columns_set;
    std::unordered_map<std::string, std::string> column_name_to_column_identifier;
};

using TableExpressionNodeToColumns = std::unordered_map<const IQueryTreeNode *, TableExpressionColumns>;
using TableExpressionColumnNodeToColumnIdentifier = std::unordered_map<const IQueryTreeNode *, std::string>;
using ActionsNodeNameToCount = std::unordered_map<std::string, size_t>;

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

    // NamesAndTypes getAvailableOutputNamesAndTypes() const
    // {
    //     NamesAndTypes result;
    //     result.reserve(available_output_columns.size());

    //     for (const auto & available_output_column : available_output_columns)
    //         result.emplace_back(available_output_column.name, available_output_column.type);

    //     return result;
    // }

    // [[maybe_unused]] Names getAvailableOutputNames() const
    // {
    //     Names result;
    //     result.reserve(available_output_columns.size());

    //     for (const auto & available_output_column : available_output_columns)
    //         result.emplace_back(available_output_column.name);

    //     return result;
    // }

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

class QueryPlanBuilder
{
public:
    using BuildRootStep = std::function<QueryPlan (void)>;
    using UniteStep = std::function<QueryPlan (std::vector<QueryPlan>)>;
    using BuildStep = std::function<void (QueryPlan &)>;

    explicit QueryPlanBuilder(QueryPlan plan_root_)
    {
        auto plan_root_ptr = std::make_shared<QueryPlan>(std::move(plan_root_));
        build_root_step = [plan_root_ptr]()
        {
            return std::move(*plan_root_ptr);
        };
    }

    [[maybe_unused]] explicit QueryPlanBuilder(std::vector<QueryPlanBuilder> plan_builders_, UniteStep unit_step_)
    {
        auto plan_builders_ptr = std::make_shared<std::vector<QueryPlanBuilder>>(std::move(plan_builders_));
        build_root_step = [plan_builders_ptr, unite_step = std::move(unit_step_)]()
        {
            auto plan_builders = std::move(*plan_builders_ptr);
            std::vector<QueryPlan> plans;
            plans.reserve(plan_builders.size());

            for (auto && plan_builder : plan_builders)
                plans.push_back(std::move(plan_builder).buildPlan());

            return unite_step(std::move(plans));
        };
    }

    QueryPlanBuilder(QueryPlanBuilder &&) noexcept = default;
    [[maybe_unused]] QueryPlanBuilder & operator=(QueryPlanBuilder &&) noexcept = default;

    void addBuildStep(BuildStep step)
    {
        build_steps.push_back(std::move(step));
    }

    QueryPlan buildPlan() &&
    {
        auto plan = build_root_step();

        for (auto & build_step : build_steps)
            build_step(plan);

        return plan;
    }
private:
    BuildRootStep build_root_step;
    std::vector<BuildStep> build_steps;
};

struct PlannerContext
{
    TableExpressionColumnNodeToColumnIdentifier table_expression_column_node_to_column_identifier;
    TableExpressionNodeToColumns table_expression_node_to_columns;
    size_t column_identifier_counter = 0;

    ActionsChain actions_chain;

    ActionsDAGPtr where_actions;
    std::string where_action_node_name;
    ActionsDAGPtr projection_actions;

    ContextPtr query_context;

    std::string getColumnUniqueIdentifier()
    {
        auto result = "__column_" + std::to_string(column_identifier_counter);
        ++column_identifier_counter;

        return result;
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

    // bool containsNode(const std::string & node_name)
    // {
    //     return node_name_to_node.find(node_name) != node_name_to_node.end();
    // }

    // const ActionsDAG::Node * tryGetNode(const std::string & node_name)
    // {
    //     auto it = node_name_to_node.find(node_name);
    //     if (it == node_name_to_node.end())
    //         return {};

    //     return it->second;
    // }

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
                auto it = planner_context.table_expression_column_node_to_column_identifier.find(node);
                if (it == planner_context.table_expression_column_node_to_column_identifier.end())
                    return node->getName();

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
                buffer << function_node.getFunctionName();

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

        if (column_source_node_type == QueryTreeNodeType::ARRAY_JOIN)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "ARRAY JOIN is not supported");

        if (column_source_node_type == QueryTreeNodeType::LAMBDA)
            return;

        if (column_node->hasExpression())
        {
            /// Replace ALIAS column with expression
            node = column_node->getExpression();
            visit(node, data);
            return;
        }

        if (column_source_node_type != QueryTreeNodeType::TABLE &&
            column_source_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
            column_source_node_type != QueryTreeNodeType::QUERY)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected table, table function or query column source. Actual {}",
                column_source_node->formatASTForErrorMessage());

        auto & table_expression_node_to_columns = data.planner_context.table_expression_node_to_columns;
        auto & table_expression_column_node_to_column_identifier = data.planner_context.table_expression_column_node_to_column_identifier;

        auto [it, _] = table_expression_node_to_columns.emplace(column_source_node.get(), TableExpressionColumns());
        auto [source_columns_set_it, inserted] = it->second.source_columns_set.insert(column_node->getColumnName());

        if (inserted)
        {
            auto column_identifier = data.planner_context.getColumnUniqueIdentifier();
            table_expression_column_node_to_column_identifier.emplace(column_node, column_identifier);
            it->second.column_name_to_column_identifier.emplace(column_node->getColumnName(), column_identifier);
            it->second.source_input_columns.emplace_back(column_node->getColumn());
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

struct JoinTreeNodePlan
{
    QueryPlanBuilder plan_builder;
    std::vector<size_t> actions_chain_node_indices;
};

JoinTreeNodePlan buildQueryPlanForJoinTreeNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context);

JoinTreeNodePlan buildQueryPlanForTableExpression(QueryTreeNodePtr table_expression,
    SelectQueryInfo & table_expression_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context)
{
    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();

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

        Names column_names(table_expression_columns.source_columns_set.begin(), table_expression_columns.source_columns_set.end());

        std::optional<NameAndTypePair> read_additional_column;

        bool plan_has_multiple_table_expressions = planner_context.table_expression_node_to_columns.size() > 1;
        if (column_names.empty() && (plan_has_multiple_table_expressions || storage->getName() == "SystemOne"))
        {
            auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns());
            read_additional_column = column_names_and_types.front();
        }

        if (read_additional_column)
        {
            column_names.push_back(read_additional_column->name);
            table_expression_columns.source_columns_set.emplace(read_additional_column->name);
            table_expression_columns.source_input_columns.emplace_back(*read_additional_column);
            table_expression_columns.column_name_to_column_identifier.emplace(read_additional_column->name, planner_context.getColumnUniqueIdentifier());
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
    else if (query_node)
    {
        InterpreterSelectQueryAnalyzer interpeter(table_expression, select_query_options, planner_context.query_context);
        interpeter.initializeQueryPlanIfNeeded();
        query_plan = std::move(interpeter).extractQueryPlan();
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function or query. Actual {}", table_expression->formatASTForErrorMessage());
    }

    auto rename_actions_dag = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());

    for (const auto & [column_name, column_identifier] : table_expression_columns.column_name_to_column_identifier)
    {
        auto position = query_plan.getCurrentDataStream().header.getPositionByName(column_name);
        const auto * node_to_rename = rename_actions_dag->getOutputs()[position];
        rename_actions_dag->getOutputs()[position] = &rename_actions_dag->addAlias(*node_to_rename, column_identifier);
    }

    planner_context.actions_chain.addNode(std::make_unique<ActionsChainNode>(rename_actions_dag, true /*available_output_columns_only_aliases*/));
    size_t actions_chain_node_index = planner_context.actions_chain.getLastNodeIndex();

    QueryPlanBuilder builder(std::move(query_plan));

    builder.addBuildStep([rename_actions_dag](QueryPlan & build_plan) {
        auto rename_step = std::make_unique<ExpressionStep>(build_plan.getCurrentDataStream(), rename_actions_dag);
        rename_step->setStepDescription("Change column names to column identifiers");
        build_plan.addStep(std::move(rename_step));
    });

    return {std::move(builder), {actions_chain_node_index}};
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
                join_clause.addKey(equals_left_child, equals_right_child);
            else
                join_clause.addCondition(left_equals_expression_side, join_expressions_actions_node);
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
    std::cout << "buildJoinClausesAndActions " << join_node.formatASTForErrorMessage() << std::endl;

    ActionsDAGPtr join_expression_actions = std::make_shared<ActionsDAG>(join_expression_input_columns);
        // std::cout << "buildJoinClausesAndActions join expression actions dag before visitor " << std::endl;
    // std::cout << join_expression_actions->dumpDAG() << std::endl;

    QueryTreeActionsVisitor join_expression_visitor(join_expression_actions, planner_context);
    auto join_expression_dag_node_raw_pointers = join_expression_visitor.visit(join_node.getJoinExpression());
    if (join_expression_dag_node_raw_pointers.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JOIN {} ON clause contains multiple expressions",
            join_node.formatASTForErrorMessage());

    // std::cout << "buildJoinClausesAndActions join expression actions dag after visitor " << std::endl;
    // std::cout << join_expression_actions->dumpDAG() << std::endl;

    const auto * join_expressions_actions_root_node = join_expression_dag_node_raw_pointers[0];
    if (!join_expressions_actions_root_node->function)
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "JOIN {} join expression expected function",
            join_node.formatASTForErrorMessage());

    std::cout << "buildJoinClausesAndActions join expressions actions DAG dump " << std::endl;
    std::cout << join_expression_actions->dumpDAG() << std::endl;

    std::cout << "root node " << join_expressions_actions_root_node << std::endl;

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

        for (const auto & left_key_node : join_clause.getLeftKeyNodes())
        {
            join_expression_actions->addOrReplaceInOutputs(*left_key_node);
            add_necessary_name_if_needed(JoinTableSide::Left, left_key_node->result_name);
        }

        for (const auto & right_key_node : join_clause.getRightKeyNodes())
        {
            join_expression_actions->addOrReplaceInOutputs(*right_key_node);
            add_necessary_name_if_needed(JoinTableSide::Right, right_key_node->result_name);
        }
    }

    result.left_join_expressions_actions = join_expression_actions->clone();
    result.left_join_expressions_actions->removeUnusedActions(join_left_actions_names);

    result.right_join_expressions_actions = join_expression_actions->clone();
    result.right_join_expressions_actions->removeUnusedActions(join_right_actions_names);

    return result;
}

JoinTreeNodePlan buildQueryPlanForJoinNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context)
{
    auto & join_node = join_tree_node->as<JoinNode &>();

    auto left_plan_build_result = buildQueryPlanForJoinTreeNode(join_node.getLeftTableExpression(),
        select_query_info,
        select_query_options,
        planner_context);
    auto left_plan_builder = std::move(left_plan_build_result.plan_builder);
    ColumnsWithTypeAndName left_plan_output_columns = planner_context.actions_chain.getAvailableOutputColumns(left_plan_build_result.actions_chain_node_indices);

    auto right_plan_build_result = buildQueryPlanForJoinTreeNode(join_node.getRightTableExpression(),
        select_query_info,
        select_query_options,
        planner_context);
    auto right_plan_builder = std::move(right_plan_build_result.plan_builder);
    auto right_plan_output_columns = planner_context.actions_chain.getAvailableOutputColumns(right_plan_build_result.actions_chain_node_indices);

    if (join_node.getStrictness() == JoinStrictness::Asof)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "JOIN {} ASOF is not supported",
            join_node.formatASTForErrorMessage());

    JoinClausesAndActions join_clauses_and_actions;

    std::vector<size_t> actions_chain_node_indices;
    std::vector<size_t> actions_chain_right_plan_node_indexes;

    if (join_node.getJoinExpression())
    {
        if (join_node.isUsingJoinExpression())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "JOIN {} USING is unsupported",
                join_node.formatASTForErrorMessage());

        auto join_expression_input_columns = left_plan_output_columns;
        join_expression_input_columns.insert(join_expression_input_columns.end(), right_plan_output_columns.begin(), right_plan_output_columns.end());
        join_clauses_and_actions = buildJoinClausesAndActions(join_expression_input_columns,
            left_plan_output_columns,
            right_plan_output_columns,
            join_node,
            planner_context);

        auto left_join_actions_node = std::make_unique<ActionsChainNode>(join_clauses_and_actions.left_join_expressions_actions);
        left_join_actions_node->addParentIndices(left_plan_build_result.actions_chain_node_indices);
        planner_context.actions_chain.addNode(std::move(left_join_actions_node));
        actions_chain_node_indices.push_back(planner_context.actions_chain.getLastNodeIndex());

        auto right_join_actions_node = std::make_unique<ActionsChainNode>(join_clauses_and_actions.right_join_expressions_actions);
        right_join_actions_node->addParentIndices(right_plan_build_result.actions_chain_node_indices);
        planner_context.actions_chain.addNode(std::move(right_join_actions_node));
        actions_chain_node_indices.push_back(planner_context.actions_chain.getLastNodeIndex());
        actions_chain_right_plan_node_indexes.push_back(planner_context.actions_chain.getLastNodeIndex());

        left_plan_builder.addBuildStep([left_join_expressions_actions = join_clauses_and_actions.left_join_expressions_actions](QueryPlan & build_plan)
        {
            auto left_join_expressions_actions_step = std::make_unique<ExpressionStep>(build_plan.getCurrentDataStream(), left_join_expressions_actions);
            left_join_expressions_actions_step->setStepDescription("Join actions");
            build_plan.addStep(std::move(left_join_expressions_actions_step));
        });

        right_plan_builder.addBuildStep([right_join_expressions_actions = join_clauses_and_actions.right_join_expressions_actions](QueryPlan & build_plan)
        {
            auto right_join_expressions_actions_step = std::make_unique<ExpressionStep>(build_plan.getCurrentDataStream(), right_join_expressions_actions);
            right_join_expressions_actions_step->setStepDescription("Join actions");
            build_plan.addStep(std::move(right_join_expressions_actions_step));
        });
    }
    else
    {
        actions_chain_right_plan_node_indexes = right_plan_build_result.actions_chain_node_indices;
        actions_chain_node_indices.insert(actions_chain_node_indices.end(), actions_chain_right_plan_node_indexes.begin(), actions_chain_right_plan_node_indexes.end());
    }

    std::vector<QueryPlanBuilder> builders;
    builders.emplace_back(std::move(left_plan_builder));
    builders.emplace_back(std::move(right_plan_builder));

    QueryPlanBuilder builder(std::move(builders), [join_clauses_and_actions, actions_chain_right_plan_node_indexes, &join_node, &planner_context](std::vector<QueryPlan> build_query_plans)
    {
        if (build_query_plans.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join step expects 2 query plans. Actual {}", build_query_plans.size());

        auto left_plan = std::move(build_query_plans[0]);
        auto right_plan = std::move(build_query_plans[1]);

        auto table_join = std::make_shared<TableJoin>();
        table_join->getTableJoin() = join_node.toASTTableJoin()->as<ASTTableJoin &>();
        if (join_node.getKind() == JoinKind::Comma)
            table_join->getTableJoin().kind = JoinKind::Cross;
        table_join->getTableJoin().strictness = JoinStrictness::All;

        NameSet join_clauses_right_column_names;

        if (join_node.getJoinExpression())
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
                    join_clauses_right_column_names.insert(join_clause_right_key_nodes[i]->result_name);
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
                    join_clauses_right_column_names.insert(join_clause_right_filter_condition_name);
                }
            }
        }

        auto left_table_names = left_plan.getCurrentDataStream().header.getNames();
        NameSet left_table_names_set(left_table_names.begin(), left_table_names.end());

        auto columns_from_joined_table = right_plan.getCurrentDataStream().header.getNamesAndTypesList();
        table_join->setColumnsFromJoinedTable(columns_from_joined_table, left_table_names_set, "");

        NamesAndTypesList columns_added_by_join;
        for (auto & column_from_joined_table : columns_from_joined_table)
        {
            for (const auto & actions_chain_right_plan_node_index : actions_chain_right_plan_node_indexes)
            {
                const auto & child_required_ouput_columns_names = planner_context.actions_chain[actions_chain_right_plan_node_index]->getChildRequiredOutputColumnsNames();

                if (child_required_ouput_columns_names.contains(column_from_joined_table.name))
                {
                    columns_added_by_join.insert(columns_added_by_join.end(), column_from_joined_table);
                    break;
                }
            }
        }

        table_join->setColumnsAddedByJoin(columns_added_by_join);

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

        auto result = QueryPlan();
        result.unitePlans(std::move(join_step), {std::move(plans)});

        return result;
    });

    return {std::move(builder), actions_chain_node_indices};
}

JoinTreeNodePlan buildQueryPlanForJoinTreeNode(QueryTreeNodePtr join_tree_node,
    SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContext & planner_context)
{
    auto join_tree_node_type = join_tree_node->getNodeType();

    switch (join_tree_node_type)
    {
        case QueryTreeNodeType::QUERY:
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
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "ARRAY JOIN is not supported");
        }
        default:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected query, table, table function, join or array join query node. Actual {}", join_tree_node->formatASTForErrorMessage());
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

    auto & query_node = query_tree->as<QueryNode &>();

    auto current_context = getContext();

    SelectQueryInfo select_query_info;
    select_query_info.original_query = query;
    select_query_info.query = query;

    PlannerContext planner_context;
    planner_context.query_context = getContext();

    CollectSourceColumnsVisitor::Data data {planner_context};
    CollectSourceColumnsVisitor collect_source_columns_visitor(data);
    collect_source_columns_visitor.visit(query_tree);

    JoinTreeNodePlan join_tree_node_plan = buildQueryPlanForJoinTreeNode(query_node.getFrom(), select_query_info, select_query_options, planner_context);
    auto query_plan_builder = std::move(join_tree_node_plan.plan_builder);
    auto action_chain_node_parent_indices = join_tree_node_plan.actions_chain_node_indices;

    if (query_node.hasWhere())
    {
        ColumnsWithTypeAndName where_input = planner_context.actions_chain.getAvailableOutputColumns(action_chain_node_parent_indices);
        planner_context.where_actions = convertExpressionNodeIntoDAG(query_node.getWhere(), where_input, planner_context);
        planner_context.where_action_node_name = planner_context.where_actions->getOutputs().at(0)->result_name;

        auto where_actions_node = std::make_unique<ActionsChainNode>(planner_context.where_actions);
        where_actions_node->addParentIndices(action_chain_node_parent_indices);
        planner_context.actions_chain.addNode(std::move(where_actions_node));
        action_chain_node_parent_indices = {planner_context.actions_chain.getLastNodeIndex()};

        size_t where_node_index = planner_context.actions_chain.size();

        query_plan_builder.addBuildStep([&, where_node_index](QueryPlan & build_plan)
        {
            bool remove_filter = !planner_context.actions_chain.at(where_node_index)->getChildRequiredOutputColumnsNames().contains(planner_context.where_action_node_name);
            auto where_step = std::make_unique<FilterStep>(build_plan.getCurrentDataStream(),
                planner_context.where_actions,
                planner_context.where_action_node_name,
                remove_filter);
            where_step->setStepDescription("WHERE");
            build_plan.addStep(std::move(where_step));
        });
    }

    ColumnsWithTypeAndName projection_input = planner_context.actions_chain.getAvailableOutputColumns(action_chain_node_parent_indices);
    planner_context.projection_actions = convertExpressionNodeIntoDAG(query_node.getProjectionNode(), projection_input, planner_context);

    auto projection_actions_node = std::make_unique<ActionsChainNode>(planner_context.projection_actions);
    projection_actions_node->addParentIndices(action_chain_node_parent_indices);
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

    query_plan_builder.addBuildStep([&](QueryPlan & build_plan)
    {
        auto projection_step = std::make_unique<ExpressionStep>(build_plan.getCurrentDataStream(), planner_context.projection_actions);
        projection_step->setStepDescription("Projection");
        build_plan.addStep(std::move(projection_step));
    });

    // std::cout << "Chain dump before finalize" << std::endl;
    // std::cout << planner_context.actions_chain.dump() << std::endl;

    planner_context.actions_chain.finalize();

    // std::cout << "Chain dump after finalize" << std::endl;
    // std::cout << planner_context.actions_chain.dump() << std::endl;

    query_plan = std::move(query_plan_builder).buildPlan();
}

}
