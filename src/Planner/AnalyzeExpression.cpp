#include <Planner/AnalyzeExpression.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>

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


/// Build the analyzer DAG up to (and including) `buildSetInplace`, set-determinism
/// marking, and `indexHint` input promotion.  Output column names are NOT yet
/// rewritten to AST conventions (no projection, no aliasing, no source-column
/// appending).  This is the costly part — it runs `QueryAnalyzer::resolve` and
/// executes any `IN (subquery)` sets — and is shared by helpers that need the
/// resulting DAG in different post-processed shapes.
struct CoreAnalysisResult
{
    ActionsDAG actions;
    /// AST column names using `getColumnName()` (no aliases applied).  Length
    /// matches the original AST children before wildcard expansion.
    std::vector<String> ast_column_names_no_aliases;
    /// AST column names using `getAliasOrColumnName()`.  Length matches the
    /// original AST children before wildcard expansion.
    std::vector<String> ast_column_names_with_aliases;
};

static CoreAnalysisResult buildExpressionCoreDAG(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool build_subquery_sets)
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
        return {ActionsDAG(), {}, {}};

    /// Collect AST column names in both conventions so that any post-processing
    /// branch (alias-projecting or AST-name override) can pick the right one
    /// without re-running the analyzer.
    /// NOTE: these names are preliminary; if the Analyzer rewrites the projection
    /// (e.g. IN → has/equals), the count may change. The caller re-derives names
    /// from the resolved projection if needed.
    std::vector<String> ast_column_names_no_aliases;
    std::vector<String> ast_column_names_with_aliases;
    ast_column_names_no_aliases.reserve(ast_children.size());
    ast_column_names_with_aliases.reserve(ast_children.size());
    for (const auto & child : ast_children)
    {
        ast_column_names_no_aliases.push_back(child->getColumnName());
        ast_column_names_with_aliases.push_back(child->getAliasOrColumnName());
    }

    auto execution_context = Context::createCopy(context);

    /// StorageDummy requires at least one column.  When the expression is constant
    /// (e.g. a constant TTL like '2000-10-10'::DateTime), available_columns may be empty.
    auto columns_for_dummy = available_columns;
    if (columns_for_dummy.empty())
        columns_for_dummy.emplace_back("_dummy", std::make_shared<DataTypeUInt8>());

    ColumnsDescription columns_description(columns_for_dummy);
    auto storage = std::make_shared<StorageDummy>(StorageID{"_analyze_expression_db", "_analyze_expression_table"}, columns_description);
    QueryTreeNodePtr fake_table_expression = std::make_shared<TableNode>(storage, execution_context);

    auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{});
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

    /// Convert each subquery set's query tree into a query plan so the set becomes
    /// buildable (a `FutureSetFromSubquery` created by `collectSets` only carries a
    /// query tree; `buildSetInplace` needs a query plan as its `source`).  This must
    /// always run so the set can be built later, whether eagerly here or lazily by the
    /// caller.
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
                std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}));
            subquery_planner.buildQueryPlanIfNeeded();
            auto subquery_plan = std::move(subquery_planner).extractQueryPlan();
            subquery_set->setQueryPlan(std::make_unique<QueryPlan>(std::move(subquery_plan)));
        }
    }

    /// Build subquery sets in place AFTER constructing the DAG.  Building before
    /// DAG construction would make the sets "ready", causing PlannerActionsVisitor
    /// to constant-fold expressions like exists((SELECT 1)) — which then fails the
    /// "key cannot contain constants" check for partition/sorting keys.
    /// By building after, the DAG sees unready sets and keeps them as function calls.
    /// The sets are still ready by the time the expression is actually evaluated
    /// (e.g. during TTL checks, constraint validation, or INSERT).
    ///
    /// Skipped when `build_subquery_sets` is false: some callers build the sets
    /// themselves at the correct time (e.g. constraint checks at insert time via
    /// `VirtualColumnUtils::buildSetsForDAG`).  Executing the subquery here would run
    /// a nested pipeline that reports progress on the outer query's context, breaking
    /// the INSERT protocol handshake (a stray `Progress` packet before the sample block).
    if (build_subquery_sets)
    {
        for (auto & subquery_set : planner_context->getPreparedSets().getSubqueries())
            subquery_set->buildSetInplace(execution_context);

        /// After building subquery sets in place, mark the corresponding DAG column
        /// nodes as deterministic constants.  The sets have been evaluated and are now
        /// fixed values, so assertDeterministic() should not reject them (e.g. when
        /// used in partition key expressions like PARTITION BY exists((SELECT 1))).
        /// Identify set columns by their data type (`DataTypeSet`), not by name prefix
        /// alone — a user may legally have a column named `__set_x`, and we must not
        /// flip its `is_deterministic_constant` flag based on the name.
        for (const auto & node : actions.getNodes())
        {
            if (node.type == ActionsDAG::ActionType::COLUMN
                && node.result_type
                && node.result_type->getTypeId() == TypeIndex::Set
                && !node.is_deterministic_constant)
            {
                const_cast<ActionsDAG::Node &>(node).is_deterministic_constant = true;
            }
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

    return {std::move(actions), std::move(ast_column_names_no_aliases), std::move(ast_column_names_with_aliases)};
}

/// Apply the `add_aliases=false` post-processing branch in place: rebuild constant
/// and function `result_name`s in AST style (so KeyCondition can match subexpression
/// names against WHERE clause names), override top-level output names from
/// `ast_column_names`, and append source columns to outputs (matching the old
/// `ExpressionAnalyzer::getActions(false, false)` behavior).
static void applyAstNamingAndAppendSourceColumns(ActionsDAG & actions, const std::vector<String> & ast_column_names)
{
    auto & outputs = actions.getOutputs();

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
            && (!node.result_type || node.result_type->getTypeId() != TypeIndex::Set))
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

ActionsDAG analyzeExpressionToActionsDAG(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases,
    bool project_result,
    bool build_subquery_sets)
{
    auto core = buildExpressionCoreDAG(expression_ast, available_columns, context, build_subquery_sets);
    auto & actions = core.actions;
    auto & ast_column_names = add_aliases ? core.ast_column_names_with_aliases : core.ast_column_names_no_aliases;

    /// Empty AST → empty DAG (preserves the early-return behavior of the old code path).
    if (ast_column_names.empty() && actions.getOutputs().empty())
        return std::move(actions);

    auto & outputs = actions.getOutputs();

    /// The Analyzer may rewrite the projection (e.g. `IN column` → `has()`/`equals()`),
    /// changing the number of output columns.  When that happens, fall back to using
    /// the DAG's own output names — callers that need specific AST-based names will
    /// still work because the Analyzer preserves semantics.
    if (outputs.size() != ast_column_names.size())
    {
        ast_column_names.clear();
        ast_column_names.reserve(outputs.size());
        for (const auto * output : outputs)
            ast_column_names.push_back(output->result_name);
    }

    if (add_aliases && project_result)
    {
        /// Project to only the expression columns, renamed to match AST column names.
        NamesWithAliases rename_pairs;
        rename_pairs.reserve(outputs.size());

        for (size_t i = 0; i != outputs.size(); ++i)
            rename_pairs.emplace_back(outputs[i]->result_name, ast_column_names[i]);

        actions.project(rename_pairs);
    }
    else if (add_aliases && !project_result)
    {
        /// Mirror `ExpressionAnalyzer::getActionsDAG(true, false)`: add aliases
        /// for the projection outputs, append all source columns to outputs,
        /// then prune outputs to (alias names ∪ source-column names).  Without
        /// the pruning step the DAG keeps the original non-aliased expression
        /// output alongside the alias, which diverges from the legacy header
        /// shape (e.g. `expr AS x` would expose both `expr` and `x`).
        /// Source columns must be appended to outputs before `removeUnusedActions`
        /// because the `NameSet` overload looks only at outputs — inputs are not
        /// reachable by name there.
        NamesWithAliases rename_pairs;
        rename_pairs.reserve(outputs.size());
        NameSet required_names;
        required_names.reserve(outputs.size() + actions.getInputs().size());

        for (size_t i = 0; i != outputs.size(); ++i)
        {
            rename_pairs.emplace_back(outputs[i]->result_name, ast_column_names[i]);
            required_names.insert(ast_column_names[i]);
        }

        actions.addAliases(rename_pairs);

        /// Append source columns (inputs) to outputs if not already present,
        /// matching the legacy behavior where the input block columns remained
        /// visible alongside the aliased expression.
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
            required_names.insert(input->result_name);
        }
        for (const auto * input : inputs_to_add)
            outputs.push_back(input);

        actions.removeUnusedActions(required_names);
    }
    else
    {
        applyAstNamingAndAppendSourceColumns(actions, ast_column_names);
    }

    return std::move(actions);
}

ExpressionActionsPtr analyzeExpressionToActions(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    bool add_aliases,
    CompileExpressions compile_expressions,
    bool build_subquery_sets)
{
    auto dag = analyzeExpressionToActionsDAG(
        expression_ast, available_columns, context, add_aliases, /* project_result */ true, build_subquery_sets);
    /// Match ExpressionAnalyzer::getActions(add_aliases, remove_unused_result=true) which constructs
    /// ExpressionActions with project_inputs = add_aliases && remove_unused_result. With the default
    /// project_result=true in the DAG, remove_unused_result is effectively true here.
    return std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings(context, compile_expressions), add_aliases);
}

AnalyzedExpressionWithSampleBlock analyzeExpressionToActionsAndSampleBlock(
    const ASTPtr & expression_ast,
    const NamesAndTypesList & available_columns,
    const ContextPtr & context,
    CompileExpressions compile_expressions)
{
    auto core = buildExpressionCoreDAG(expression_ast, available_columns, context, /* build_subquery_sets */ true);
    auto & actions = core.actions;

    if (actions.getOutputs().empty() && core.ast_column_names_no_aliases.empty())
    {
        AnalyzedExpressionWithSampleBlock result;
        result.expression = std::make_shared<ExpressionActions>(std::move(actions), ExpressionActionsSettings(context, compile_expressions));
        return result;
    }

    auto & outputs = actions.getOutputs();

    auto & names_no_aliases = core.ast_column_names_no_aliases;
    auto & names_with_aliases = core.ast_column_names_with_aliases;

    /// Wildcard / matcher expansion changes the number of resolved outputs.
    /// Fall back to DAG-generated names in that case so that downstream
    /// consistency checks (e.g. `column_names.size() != sample_block.columns()`)
    /// fire on the projected sample block.
    if (outputs.size() != names_no_aliases.size())
    {
        names_no_aliases.clear();
        names_with_aliases.clear();
        names_no_aliases.reserve(outputs.size());
        names_with_aliases.reserve(outputs.size());
        for (const auto * output : outputs)
        {
            names_no_aliases.push_back(output->result_name);
            names_with_aliases.push_back(output->result_name);
        }
    }

    /// Build the projected sample block from a clone of the DAG, before any
    /// AST-style renaming or source-column appending is applied to the original.
    Block sample_block;
    {
        auto sample_dag = actions.clone();
        NamesWithAliases rename_pairs;
        rename_pairs.reserve(outputs.size());
        const auto & sample_outputs = sample_dag.getOutputs();
        for (size_t i = 0; i != sample_outputs.size(); ++i)
            rename_pairs.emplace_back(sample_outputs[i]->result_name, names_with_aliases[i]);
        sample_dag.project(rename_pairs);
        sample_block = ExpressionActions(std::move(sample_dag), ExpressionActionsSettings(context)).getSampleBlock();
    }

    /// Apply AST-style intermediate renaming and append source columns to outputs
    /// so the resulting `ExpressionActions` matches the legacy
    /// `ExpressionAnalyzer::getActions(false, false)` shape.
    applyAstNamingAndAppendSourceColumns(actions, names_no_aliases);

    AnalyzedExpressionWithSampleBlock result;
    result.expression = std::make_shared<ExpressionActions>(std::move(actions), ExpressionActionsSettings(context, compile_expressions));
    result.sample_block = std::move(sample_block);
    return result;
}

}
