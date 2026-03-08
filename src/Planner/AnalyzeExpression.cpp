#include <Planner/AnalyzeExpression.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/TableNode.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Common/FieldVisitorToString.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/indexHint.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/ASTExpressionList.h>
#include <Planner/CollectSets.h>
#include <Planner/CollectTableExpressionData.h>
#include <Planner/Planner.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageDummy.h>


namespace DB
{

ActionsDAG analyzeExpressionToActionsDAG(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases)
{
    /// Ensure the AST is an expression list, because QueryNode projection expects a ListNode.
    ASTPtr expr_list_ast = expression_ast;
    if (!expr_list_ast->as<ASTExpressionList>())
    {
        auto wrapper = make_intrusive<ASTExpressionList>();
        wrapper->children.push_back(expr_list_ast);
        expr_list_ast = wrapper;
    }

    const auto & ast_children = expr_list_ast->as<ASTExpressionList &>().children;

    /// Handle empty expression list (e.g., ORDER BY tuple() produces no key columns,
    /// or missing PARTITION BY produces an empty partition key).
    /// Return an empty DAG with no inputs — callers like getRequiredColumns()
    /// must see an empty list, not all table columns.
    if (ast_children.empty())
        return ActionsDAG();

    /// Collect AST column names to use for output renaming, so that callers
    /// that rely on ast->getColumnName() (e.g. findInOutputs) work correctly.
    /// When add_aliases is true, use getAliasOrColumnName() to match the old
    /// ExpressionAnalyzer behavior — the output column name should be the alias
    /// if one is set (e.g. for ALIAS columns resolved via addTypeConversionToAST).
    std::vector<String> ast_column_names;
    ast_column_names.reserve(ast_children.size());
    for (const auto & child : ast_children)
        ast_column_names.push_back(add_aliases ? child->getAliasOrColumnName() : child->getColumnName());

    auto execution_context = Context::createCopy(context);

    /// StorageDummy requires at least one column.  When the expression is constant
    /// (e.g. a constant TTL like '2000-10-10'::DateTime), available_columns may be empty.
    auto columns_for_dummy = available_columns;
    if (columns_for_dummy.empty())
        columns_for_dummy.emplace_back("_dummy", std::make_shared<DataTypeUInt8>());

    ColumnsDescription columns_description(columns_for_dummy);
    auto storage = std::make_shared<StorageDummy>(StorageID{"dummy", "dummy"}, columns_description);
    QueryTreeNodePtr fake_table_expression = std::make_shared<TableNode>(storage, execution_context);

    auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{});
    auto planner_context = std::make_shared<PlannerContext>(execution_context, global_planner_context, SelectQueryOptions{});

    QueryAnalyzer analyzer(/* only_analyze */ true);

    auto query_node = std::make_shared<QueryNode>(execution_context);

    auto expression_list = buildQueryTree(expr_list_ast, execution_context);

    query_node->getProjectionNode() = expression_list;
    query_node->getJoinTree() = fake_table_expression;

    QueryTreeNodePtr query_tree = query_node;
    analyzer.resolve(query_tree, nullptr, execution_context);

    query_node = std::static_pointer_cast<QueryNode>(query_tree);
    expression_list = query_node->getProjectionNode();

    collectSourceColumns(expression_list, planner_context, false);
    collectSets(expression_list, *planner_context);

    ColumnNodePtrWithHashSet empty_correlated_columns_set;
    auto [actions, _] = buildActionsDAGFromExpressionNode(
        expression_list,
        {},
        planner_context,
        empty_correlated_columns_set,
        false /* use_column_identifier_as_action_node_name */);

    /// Build subquery sets in place AFTER constructing the DAG.  Building before
    /// DAG construction would make the sets "ready", causing PlannerActionsVisitor
    /// to constant-fold expressions like exists((SELECT 1)) — which then fails the
    /// "key cannot contain constants" check for partition/sorting keys.
    /// By building after, the DAG sees unready sets and keeps them as function calls.
    /// The sets are still ready by the time the expression is actually evaluated
    /// (e.g. during TTL checks, constraint validation, or INSERT).
    for (auto & subquery_set : planner_context->getPreparedSets().getSubqueries())
    {
        auto subquery_tree = subquery_set->detachQueryTree();
        if (subquery_tree)
        {
            auto subquery_options = SelectQueryOptions{}.subquery();
            subquery_options.ignore_limits = false;
            Planner subquery_planner(
                subquery_tree,
                subquery_options,
                std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{}));
            subquery_planner.buildQueryPlanIfNeeded();
            auto subquery_plan = std::move(subquery_planner).extractQueryPlan();
            subquery_set->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_plan)));
        }
        subquery_set->buildSetInplace(execution_context);
    }

    /// After building subquery sets in place, mark the corresponding DAG column
    /// nodes as deterministic constants.  The sets have been evaluated and are now
    /// fixed values, so assertDeterministic() should not reject them (e.g. when
    /// used in partition key expressions like PARTITION BY exists((SELECT 1))).
    for (const auto & node : actions.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::COLUMN
            && node.result_name.starts_with("__set_")
            && !node.is_deterministic_constant)
        {
            const_cast<ActionsDAG::Node &>(node).is_deterministic_constant = true;
        }
    }

    /// FunctionIndexHint hides its column arguments in an internal ActionsDAG,
    /// making them invisible as inputs of the main DAG.  For key expression
    /// analysis (e.g. partition key minmax index), those columns must be visible
    /// so that getRequiredColumnsWithTypes() reports them.
    std::vector<std::pair<String, DataTypePtr>> index_hint_columns;
    for (const auto & node : actions.getNodes())
    {
        if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base)
        {
            if (const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(node.function_base.get()))
            {
                if (const auto * index_hint = typeid_cast<const FunctionIndexHint *>(adaptor->getFunction().get()))
                {
                    for (const auto * input : index_hint->getActions().getInputs())
                        index_hint_columns.emplace_back(input->result_name, input->result_type);
                }
            }
        }
    }
    for (const auto & [name, type] : index_hint_columns)
    {
        bool found = false;
        for (const auto * existing : actions.getInputs())
        {
            if (existing->result_name == name)
            {
                found = true;
                break;
            }
        }
        if (!found)
            actions.addInput(name, type);
    }

    if (add_aliases)
    {
        /// Project to only the expression columns, renamed to match AST column names.
        auto & outputs = actions.getOutputs();
        NamesWithAliases rename_pairs;
        rename_pairs.reserve(outputs.size());

        for (size_t i = 0; i != outputs.size(); ++i)
            rename_pairs.emplace_back(outputs[i]->result_name, ast_column_names[i]);

        actions.project(rename_pairs);
    }
    else
    {
        /// Rename constant and function nodes to match AST naming conventions.
        /// PlannerActionsVisitor names constants with type suffixes (e.g. "1_UInt8")
        /// while the old ExpressionAnalyzer used AST-based names (e.g. "1").
        /// KeyCondition matches key subexpression names against WHERE clause names
        /// derived from ASTs, so intermediate nodes must use AST-compatible naming.
        /// Process in topological order (getNodes() returns leaves first) so that
        /// function node names are rebuilt from already-renamed children.
        for (const auto & node : actions.getNodes())
        {
            if (node.type == ActionsDAG::ActionType::COLUMN
                && node.column && isColumnConst(*node.column)
                && !node.result_name.starts_with("__set_"))
            {
                const auto & constant = assert_cast<const ColumnConst &>(*node.column);
                const_cast<ActionsDAG::Node &>(node).result_name
                    = applyVisitor(FieldVisitorToString(), constant.getField());
            }
            else if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base)
            {
                WriteBufferFromOwnString buf;
                buf << node.function_base->getName() << '(';
                for (size_t i = 0; i < node.children.size(); ++i)
                {
                    if (i > 0)
                        buf << ", ";
                    buf << node.children[i]->result_name;
                }
                buf << ')';
                const_cast<ActionsDAG::Node &>(node).result_name = buf.str();
            }
        }

        /// Rename outputs to match AST column names (overrides the above for
        /// top-level expressions where AST formatting may differ from the
        /// recursive function-name rebuild).
        auto & outputs = actions.getOutputs();
        for (size_t i = 0; i < outputs.size(); ++i)
        {
            if (outputs[i]->result_name != ast_column_names[i])
                const_cast<ActionsDAG::Node *>(outputs[i])->result_name = ast_column_names[i];
        }

        /// Add source columns to outputs to match ExpressionAnalyzer::getActions(false) behavior.
        /// The old code included all source columns in the output; buildActionsDAGFromExpressionNode
        /// only outputs the expression results.
        std::vector<const ActionsDAG::Node *> inputs_to_add;
        for (const auto * input : actions.getInputs())
        {
            bool already_in_outputs = false;
            for (const auto * output : outputs)
            {
                if (output == input)
                {
                    already_in_outputs = true;
                    break;
                }
            }
            if (!already_in_outputs)
                inputs_to_add.push_back(input);
        }
        for (const auto * input : inputs_to_add)
            outputs.push_back(input);
    }

    return std::move(actions);
}

ExpressionActionsPtr analyzeExpressionToActions(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases)
{
    auto dag = analyzeExpressionToActionsDAG(expression_ast, available_columns, context, add_aliases);
    return std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings(context));
}

}
