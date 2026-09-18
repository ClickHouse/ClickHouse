#include <Core/NamesAndTypes.h>
#include <Planner/PlannerUncorrelatedSubqueries.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>

#include <Core/Joins.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/SelectQueryOptions.h>

#include <Planner/Planner.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace Setting
{

extern const SettingsBool join_use_nulls;
extern const SettingsUInt64 max_bytes_in_set;
extern const SettingsUInt64 max_rows_in_set;
extern const SettingsBool rewrite_in_to_join;
extern const SettingsOverflowMode set_overflow_mode;

}

namespace
{

void applySetSemantics(JoinStepLogical & join_step)
{
    auto & join_settings = join_step.getJoinSettings();
    join_settings.join_algorithms = {JoinAlgorithm::PARALLEL_HASH, JoinAlgorithm::HASH};
    join_settings.join_overflow_mode = OverflowMode::THROW;
}

/// Whether `equals` compares one key the way regular `IN` does.
bool isSetKeyComparableWithEquals(const DataTypePtr & lhs_type, const DataTypePtr & rhs_type)
{
    auto lhs_base = removeNullable(removeLowCardinality(lhs_type));
    auto rhs_base = removeNullable(removeLowCardinality(rhs_type));

    if (typeid_cast<const DataTypeTuple *>(lhs_base.get()) || lhs_base->hasDynamicStructure() || isVariant(lhs_base))
        return false;

    if (lhs_base->equals(*rhs_base))
        return true;

    return isNativeNumber(lhs_base) && isNativeNumber(rhs_base)
        && tryGetLeastSupertype(DataTypes{lhs_base, rhs_base}) != nullptr;
}

/// Empty when the right side is not a subquery, or is correlated, in which case decorrelation handles it.
NamesAndTypes getInSubqueryColumns(const QueryTreeNodePtr & subquery)
{
    if (const auto * node = subquery->as<QueryNode>())
        return node->isCorrelated() ? NamesAndTypes{} : node->getProjectionColumns();

    if (const auto * node = subquery->as<UnionNode>())
        return node->isCorrelated() ? NamesAndTypes{} : node->computeProjectionColumns();

    return {};
}

/// A constant the join carries over to the matched outer rows; the unmatched ones get 0 from the outer join.
void addStepForMarker(QueryPlan & subquery_plan, const String & marker_name)
{
    const auto & header = subquery_plan.getCurrentHeader();
    ActionsDAG marker_dag(header->getColumnsWithTypeAndName());

    auto result_type = std::make_shared<DataTypeUInt8>();
    const auto & marker = marker_dag.materializeNode(
        marker_dag.addColumn(result_type->createColumnConst(0, 1), result_type, marker_name));
    marker_dag.getOutputs().push_back(&marker);

    auto marker_step = std::make_unique<ExpressionStep>(header, std::move(marker_dag));
    marker_step->setStepDescription("Create the result of IN for the matched rows");
    subquery_plan.addStep(std::move(marker_step));
}

/// `NULL` is not a value the join can match, so a row with a `NULL` key comes out unmatched, with a `false` marker.
/// This would let `NOT IN` keep the row whereas three valued logic in `IN` would drop. This is fixed by wrapping
/// the marker in `if(isNull(key), NULL, marker)`. This is only required for a single-column `IN`.
const ActionsDAG::Node & addNodesForNullKeyMask(
    ActionsDAG & result_dag,
    const ActionsDAG::Node & marker,
    const String & key_column_name,
    const ContextPtr & query_context)
{
    auto & function_factory = FunctionFactory::instance();

    const auto * key = result_dag.tryFindInOutputs(key_column_name);
    if (!key)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The join that evaluates `IN` did not deliver its key '{}'", key_column_name);
    const auto & key_is_null = result_dag.addFunction(function_factory.get("isNull", query_context), {key}, {});

    auto null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
    auto null_field = Field();
    const auto & null_node = result_dag.addColumn(
        null_type->createColumnConst(0, null_field), null_type, calculateConstantActionNodeName(null_field, null_type));

    return result_dag.addFunction(function_factory.get("if", query_context), {&key_is_null, &null_node, &marker}, {});
}

/// Turns the marker the join produced into the result the expression above expects: masked for a `NULL` key,
/// negated for `NOT IN`, and cast to the type the `IN` function it replaces has.
void addStepForMarkerResult(
    QueryPlan & query_plan,
    const UncorrelatedInSubquery & in_subquery,
    const Names & key_column_names,
    const PlannerContextPtr & planner_context)
{
    auto & function_factory = FunctionFactory::instance();
    const auto & query_context = planner_context->getQueryContext();

    const auto & header = query_plan.getCurrentHeader();
    ActionsDAG result_dag(header->getColumnsWithTypeAndName());

    const auto * marker = result_dag.tryFindInOutputs(in_subquery.action_node_name);
    if (!marker)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "The join that evaluates `IN` did not deliver its result '{}'", in_subquery.action_node_name);

    const auto * result = marker;
    if (isNullableOrLowCardinalityNullable(in_subquery.result_type))
    {
        if (key_column_names.size() != 1)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "`IN` with a nullable result has {} key columns, expected one",
                key_column_names.size());
        result = &addNodesForNullKeyMask(result_dag, *result, key_column_names.front(), query_context);
    }

    if (in_subquery.is_negated)
        result = &result_dag.addFunction(function_factory.get("not", query_context), {result}, {});

    /// The expression reads the marker as an input of the `IN` function's result type, so the plan has to deliver exactly that type.
    if (!result->result_type->equals(*in_subquery.result_type))
        result = &result_dag.addCast(*result, in_subquery.result_type, {}, query_context);

    const auto & result_alias = result_dag.addAlias(*result, in_subquery.action_node_name);

    ActionsDAG::NodeRawConstPtrs outputs;
    outputs.reserve(result_dag.getOutputs().size());
    for (const auto * output : result_dag.getOutputs())
        outputs.push_back(output == marker ? &result_alias : output);
    result_dag.getOutputs() = std::move(outputs);

    auto result_step = std::make_unique<ExpressionStep>(header, std::move(result_dag));
    result_step->setStepDescription("Compute the result of IN from the join");
    query_plan.addStep(std::move(result_step));
}

bool containsSubquery(const QueryTreeNodePtr & expression_node)
{
    auto node_type = expression_node->getNodeType();
    if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
        return true;

    for (const auto & child : expression_node->getChildren())
        if (child && containsSubquery(child))
            return true;

    return false;
}

bool readsCorrelatedColumn(const QueryTreeNodePtr & node, const ColumnNodePtrWithHashSet & correlated_columns)
{
    if (auto column_node = std::dynamic_pointer_cast<ColumnNode>(node))
        return correlated_columns.contains(column_node);

    for (const auto & child : node->getChildren())
        if (child && readsCorrelatedColumn(child, correlated_columns))
            return true;

    return false;
}

/// The join is placed above the aggregation, so the rewrite needs the left argument to still be a column
/// after it. `collectSets` takes care of the other half, that nothing below the join reads the `IN`.
bool keyIsDroppedByAggregation(const QueryTreeNodePtr & left_key, const QueryNode & query_node)
{
    if (!query_node.hasGroupBy())
        return !collectAggregateFunctionNodes(query_node.getProjectionNode()).empty();

    /// Ignoring aliases.
    QueryTreeNodePtrWithHashIgnoreAliasesSet grouping_keys;
    for (const auto & grouping_key : query_node.getGroupBy().getNodes())
    {
        if (const auto * list_node = grouping_key->as<ListNode>())
            grouping_keys.insert_range(list_node->getNodes());
        else
            grouping_keys.insert(grouping_key);
    }

    return !grouping_keys.contains(left_key);
}

}

QueryTreeNodes getInToJoinKeyElements(const FunctionNode & function_node)
{
    const auto & left_key = function_node.getArguments().getNodes()[0];

    if (getInSubqueryColumns(function_node.getArguments().getNodes()[1]).size() > 1)
    {
        /// A tuple written as `(a, b)` is compared element by element.
        const auto * tuple_node = left_key->as<FunctionNode>();
        if (tuple_node && tuple_node->getFunctionName() == "tuple")
            return tuple_node->getArguments().getNodes();
    }

    return {left_key};
}

bool canRewriteInToJoin(
    const QueryTreeNodePtr & in_node,
    const QueryTreeNodePtr & query_node,
    const IQueryTreeNode * rewrite_root_node,
    const PlannerContext & planner_context)
{
    const auto & function_node = in_node->as<const FunctionNode &>();

    /// Also excludes `nullIn` and `globalIn`.
    if (function_node.getFunctionName() != "in" && function_node.getFunctionName() != "notIn")
        return false;

    const auto & settings = planner_context.getQueryContext()->getSettingsRef();
    if (!settings[Setting::rewrite_in_to_join])
        return false;

    const auto & arguments = function_node.getArguments().getNodes();
    if (arguments.size() != 2)
        return false;

    auto subquery_columns = getInSubqueryColumns(arguments[1]);
    if (subquery_columns.empty())
        return false;

    const auto & left_key = arguments[0];
    if (left_key->as<ConstantNode>())
        return false;

    auto key_elements = getInToJoinKeyElements(function_node);
    if (key_elements.size() != subquery_columns.size())
        return false;

    for (size_t i = 0; i < key_elements.size(); ++i)
        if (!isSetKeyComparableWithEquals(key_elements[i]->getResultType(), subquery_columns[i].type))
            return false;

    if (containsSubquery(left_key))
        return false;

    /// Only a join under `WHERE` is below the aggregation.
    const auto & typed_query_node = query_node->as<const QueryNode &>();
    const bool above_aggregation = rewrite_root_node != typed_query_node.getWhere().get();
    if (above_aggregation && keyIsDroppedByAggregation(left_key, typed_query_node))
        return false;

    if (readsCorrelatedColumn(left_key, typed_query_node.getCorrelatedColumnsSet()))
        return false;

    /// The join can only throw on overflow, so a truncated set has no equivalent.
    if (settings[Setting::set_overflow_mode] != OverflowMode::THROW && (settings[Setting::max_rows_in_set] || settings[Setting::max_bytes_in_set]))
        return false;

    return true;
}

void buildQueryPlanForUncorrelatedInSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    const UncorrelatedInSubquery & in_subquery,
    const SelectQueryOptions & select_query_options)
{
    auto subquery_options = select_query_options.subquery();
    Planner subquery_planner(
        in_subquery.subquery, subquery_options, std::make_shared<GlobalPlannerContext>(nullptr, nullptr, nullptr, FiltersForTableExpressionMap{}));
    subquery_planner.buildQueryPlanIfNeeded();
    auto subquery_plan = std::move(subquery_planner).extractQueryPlan();
    for (const auto & context : subquery_plan.getInterpretersContexts())
        query_plan.addInterpreterContext(context);

    auto subquery_column_names = subquery_plan.getCurrentHeader()->getNames();
    const auto & key_column_names = in_subquery.key_column_names;
    if (key_column_names.size() != subquery_column_names.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "`IN` has {} key columns against {} columns of its subquery",
            key_column_names.size(),
            subquery_column_names.size());

    addStepForMarker(subquery_plan, in_subquery.action_node_name);

    auto lhs_header = query_plan.getCurrentHeader();
    auto rhs_header = subquery_plan.getCurrentHeader();

    JoinExpressionActions join_expression_actions(
        lhs_header->getColumnsWithTypeAndName(), rhs_header->getColumnsWithTypeAndName());

    std::vector<JoinActionRef> predicates;
    for (size_t i = 0; i < key_column_names.size(); ++i)
    {
        std::vector<JoinActionRef> eq_arguments;
        eq_arguments.push_back(join_expression_actions.findNode(key_column_names[i], /*is_input=*/true));
        eq_arguments.push_back(join_expression_actions.findNode(subquery_column_names[i], /*is_input=*/true));
        predicates.push_back(
            JoinActionRef::transform(eq_arguments, JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    }

    /// The outer rows are the result, plus the marker the subquery side contributes.
    NameSet output_columns;
    output_columns.insert_range(lhs_header->getNames());
    output_columns.insert(in_subquery.action_node_name);

    const auto & settings = planner_context->getQueryContext()->getSettingsRef();
    auto join_step = std::make_unique<JoinStepLogical>(
        lhs_header,
        rhs_header,
        JoinOperator(JoinKind::Left, JoinStrictness::Any, JoinLocality::Unspecified, std::move(predicates)),
        std::move(join_expression_actions),
        output_columns,
        std::unordered_map<String, const ActionsDAG::Node *>{},
        /*join_use_nulls=*/false,
        JoinSettings(settings, planner_context->getQueryContext()->getJoinAnalyzeMode()),
        SortingStep::Settings(settings));
    join_step->setStepDescription("JOIN to evaluate IN");
    applySetSemantics(*join_step);

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(subquery_plan)));

    QueryPlan result_plan;
    result_plan.unitePlans(std::move(join_step), std::move(plans));

    if (in_subquery.is_negated || !in_subquery.result_type->equals(DataTypeUInt8{}))
        addStepForMarkerResult(result_plan, in_subquery, key_column_names, planner_context);

    query_plan = std::move(result_plan);
}

}
