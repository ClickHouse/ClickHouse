#include <Planner/PlannerCorrelatedSubqueries.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <Core/Joins.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/IFunction.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinOperator.h>

#include <Parsers/SelectUnionMode.h>

#include <Planner/Planner.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerJoinsLogical.h>
#include <Planner/Utils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/UnionStep.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage.h>

#include <memory>
#include <string_view>
#include <unordered_map>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{

extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;

}

namespace Setting
{

extern const SettingsBool correlated_subqueries_substitute_equivalent_expressions;
extern const SettingsBool use_variant_as_common_type;
extern const SettingsDecorrelationJoinKind correlated_subqueries_default_join_kind;
extern const SettingsBool join_use_nulls;
extern const SettingsMaxThreads max_threads;
extern const SettingsNonZeroUInt64 max_block_size;

}

void CorrelatedSubtrees::assertEmpty(std::string_view reason) const
{
    if (notEmpty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Correlated subqueries {} are not supported", reason);
}

namespace
{

using CorrelatedPlanStepMap = std::unordered_map<QueryPlan::Node *, bool>;

CorrelatedPlanStepMap buildCorrelatedPlanStepMap(QueryPlan & correlated_query_plan)
{
    CorrelatedPlanStepMap result;

    struct State
    {
        QueryPlan::Node * node;
        bool processed_children = false;
    };

    std::vector<State> nodes_to_process{ { .node = correlated_query_plan.getRootNode() } };
    while (!nodes_to_process.empty())
    {
        size_t current_index = nodes_to_process.size() - 1;
        if (nodes_to_process[current_index].processed_children)
        {
            auto * current = nodes_to_process[current_index].node;

            auto & value = result[current];
            value = current->step->hasCorrelatedExpressions();

            for (auto * child : current->children)
                value |= result[child];

            nodes_to_process.pop_back();
        }
        else
        {
            for (auto * child : nodes_to_process[current_index].node->children)
                nodes_to_process.push_back({ .node = child });
            nodes_to_process[current_index].processed_children = true;
        }
    }

    return result;
}

struct EquivalenceClasses
{
    void add(const String & a, const String & b)
    {
        auto & class_a = member_to_class[a];
        auto & class_b = member_to_class[b];

        if (!class_a && class_b)
        {
            /// Add A to existing class B
            class_a = class_b;
            class_b->push_back(a);
        }
        else if (class_a && !class_b)
        {
            /// Add B to existing class A
            class_b = class_a;
            class_a->push_back(b);
        }
        else if (!class_a && !class_b)
        {
            /// Both A and B are new, create a class for them
            auto new_class = std::make_shared<std::list<String>>();
            new_class->push_back(a);
            class_a = new_class;
            if (a != b)
            {
                new_class->push_back(b);
                class_b = new_class;
            }
        }
        else
        {
            /// A and B already belong to the same class?
            if (class_a == class_b)
                return;

            /// Merge class of smaller size into bigger one
            if (class_a->size() < class_b->size())
                mergeFromTo(class_a, class_b);
            else
                mergeFromTo(class_b, class_a);
        }
    }

    std::shared_ptr<const std::list<String>> getClass(const String & name) const
    {
        auto it = member_to_class.find(name);
        if (it == member_to_class.end())
            return {};
        return it->second;
    }

private:
    void mergeFromTo(std::shared_ptr<std::list<String>> class_from, std::shared_ptr<std::list<String>> class_to)
    {
        /// For all existing members of class From set their class to To
        for (const auto & member_from : *class_from)
            member_to_class[member_from] = class_to;
        /// Add all elements from class From to class To
        class_to->splice(class_to->end(), *class_from);
    }

    /// Elements that belong to the same class will point to the same list of all elements of this class
    std::unordered_map<String, std::shared_ptr<std::list<String>>> member_to_class;
};

struct DecorrelationContext
{
    const CorrelatedSubquery & correlated_subquery;
    const PlannerContextPtr & planner_context;
    QueryPlan query_plan; // LHS plan
    QueryPlan correlated_query_plan;
    CorrelatedPlanStepMap correlated_plan_steps;
    /// Equivalence classes stack for subqeiries. Equivalence classes should not be propagated
    /// to the subqueries of the JOIN or UNION steps.
    std::vector<EquivalenceClasses> equivalence_class_stack;
};

/// Correlated subquery is represented by implicit dependent join operator.
/// This function builds a query plan to evaluate correlated subquery by
/// pushing dependent join down and replacing it with CROSS JOIN.
QueryPlan decorrelateQueryPlan(
    DecorrelationContext & context,
    QueryPlan::Node * node
)
{
    if (!context.correlated_plan_steps[node])
    {
        /// The rest of the query plan doesn't use any correlated columns.
        const auto & settings = context.planner_context->getQueryContext()->getSettingsRef();

        if (settings[Setting::correlated_subqueries_substitute_equivalent_expressions])
        {
            const auto & decorrelated_plan_header = node->step->getOutputHeader();
            ActionsDAG dag(decorrelated_plan_header->getNamesAndTypesList());
            auto & outputs = dag.getOutputs();

            std::unordered_map<std::string_view, const ActionsDAG::Node *> decorrelated_nodes_names;
            for (const auto * output : outputs)
                decorrelated_nodes_names[output->result_name] = output;

            /// Find possible renamings for all correlated columns
            std::vector<std::pair<const ActionsDAG::Node *, const String &>> expression_renamings;
            for (const auto & correlated_column_identifier : context.correlated_subquery.correlated_column_identifiers)
            {
                auto equivalence_class = context.equivalence_class_stack.back().getClass(correlated_column_identifier);
                if (equivalence_class)
                {
                    for (const auto & column_name : *equivalence_class)
                    {
                        auto it = decorrelated_nodes_names.find(column_name);
                        if (it != decorrelated_nodes_names.end())
                        {
                            expression_renamings.emplace_back(it->second, correlated_column_identifier);
                            break;
                        }
                    }
                }
            }

            /// If all columns from outer query have equivalent expressions in the current subplan,
            /// we can safely replace them and avoid introduction of CROSS JOIN.
            if (context.correlated_subquery.correlated_column_identifiers.size() == expression_renamings.size())
            {
                for (const auto & [from, to] : expression_renamings)
                    outputs.push_back(&dag.addAlias(*from, to));

                auto result_plan = context.correlated_query_plan.extractSubplan(node);
                auto renaming_step = std::make_unique<ExpressionStep>(result_plan.getCurrentHeader(), std::move(dag));
                renaming_step->setStepDescription("Renaming correlated columns to equivalent expressions in subquery");
                result_plan.addStep(std::move(renaming_step));
                return result_plan;
            }
        }

        QueryPlan lhs_plan = context.correlated_query_plan.extractSubplan(node);
        QueryPlan rhs_plan;

        auto default_join_kind = settings[Setting::correlated_subqueries_default_join_kind];
        context.query_plan.addStep(std::make_unique<CommonSubplanStep>(context.query_plan.getCurrentHeader()));

        auto buffer_header = std::make_shared<Block>();
        for (const auto & column : context.correlated_subquery.correlated_column_identifiers)
            buffer_header->insert(context.query_plan.getCurrentHeader()->getByName(column));

        rhs_plan.addStep(std::make_unique<CommonSubplanReferenceStep>(
            buffer_header,
            context.query_plan.getRootNode(),
            context.correlated_subquery.correlated_column_identifiers));
        rhs_plan.getRootNode()->step->setStepDescription("Input for " + context.correlated_subquery.action_node_name, 100);

        if (default_join_kind == DecorrelationJoinKind::LEFT)
            std::swap(lhs_plan, rhs_plan);

        auto lhs_plan_header = lhs_plan.getCurrentHeader();
        auto rhs_plan_header = rhs_plan.getCurrentHeader();

        JoinExpressionActions join_expression_actions(
            lhs_plan_header->getColumnsWithTypeAndName(),
            rhs_plan_header->getColumnsWithTypeAndName());

        NameSet output_columns;
        output_columns.insert_range(lhs_plan_header->getNames());
        output_columns.insert_range(rhs_plan_header->getNames());

        auto decorrelated_join = std::make_unique<JoinStepLogical>(
            /*left_header_=*/lhs_plan_header,
            /*right_header_=*/rhs_plan_header,
            JoinOperator(JoinKind::Cross),
            std::move(join_expression_actions),
            output_columns,
            std::unordered_map<String, const ActionsDAG::Node *>{},
            settings[Setting::join_use_nulls],
            JoinSettings(settings),
            SortingStep::Settings(settings));
        decorrelated_join->setStepDescription("JOIN to evaluate correlated expression");

        /// Add CROSS JOIN to combine data streams from left and right plans.
        QueryPlan result_plan;

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(lhs_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(rhs_plan)));

        result_plan.unitePlans(std::move(decorrelated_join), {std::move(plans)});

        return result_plan;
    }

    if (auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
    {
        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());

        auto input_header = decorrelated_query_plan.getCurrentHeader();

        expression_step->decorrelateActions();
        expression_step->getExpression().appendInputsForUnusedColumns(*input_header);
        for (const auto & column : input_header->getColumnsWithTypeAndName())
            expression_step->getExpression().tryRestoreColumn(column.name);

        expression_step->updateInputHeader(input_header);

        decorrelated_query_plan.addStep(std::move(node->step));
        return decorrelated_query_plan;
    }
    if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
    {
        auto & dag = filter_step->getExpression();
        auto * predicate = const_cast<ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
        auto conjuncts_list = getConjunctsList(predicate);
        for (const auto * conjunct : conjuncts_list)
        {
            bool is_equality = conjunct->type == ActionsDAG::ActionType::FUNCTION && conjunct->function_base->getName() == "equals";
            if (is_equality)
            {
                const auto & arguments = conjunct->children;
                if (arguments.size() != 2)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Correlated subquery equality predicate must have exactly two arguments, but has {}",
                        arguments.size());

                if (!arguments[0]->result_type->equals(*arguments[1]->result_type))
                    continue;

                const auto & lhs = arguments[0]->result_name;
                const auto & rhs = arguments[1]->result_name;

                context.equivalence_class_stack.back().add(lhs, rhs);
            }
        }
        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());
        auto input_header = decorrelated_query_plan.getCurrentHeader();

        filter_step->decorrelateActions();
        filter_step->getExpression().appendInputsForUnusedColumns(*input_header);
        for (const auto & column : input_header->getColumnsWithTypeAndName())
            filter_step->getExpression().tryRestoreColumn(column.name);

        node->step->updateInputHeader(input_header);

        decorrelated_query_plan.addStep(std::move(node->step));
        return decorrelated_query_plan;
    }
    if (auto * union_step = typeid_cast<UnionStep *>(node->step.get()))
    {
        /// Subplans must be decorrelated separately, because every subquery in the UNION step
        /// can have its own equivalence classes. The equivalence classes in one subquery
        /// should not be visible by another subquery. Example:
        ///
        /// SELECT *
        /// FROM t
        /// WHERE EXISTS (
        ///     SELECT *
        ///     FROM t1
        ///     WHERE t.x = t1.x
        ///     UNION ALL
        ///     SELECT *
        ///     FROM t2
        ///     WHERE t.x = t2.y
        /// )
        const auto & settings = context.planner_context->getQueryContext()->getSettingsRef();
        auto process_isolated_subplan = [](
            DecorrelationContext & current_context,
            QueryPlan::Node * subplan_root
        ) -> QueryPlan
        {
            current_context.equivalence_class_stack.emplace_back();
            auto decorrelated_isolated_plan = decorrelateQueryPlan(current_context, subplan_root);
            current_context.equivalence_class_stack.pop_back();
            return decorrelated_isolated_plan;
        };

        auto decorrelated_lhs_plan = process_isolated_subplan(context, node->children.front());
        auto decorrelated_rhs_plan = process_isolated_subplan(context, node->children.back());

        SharedHeaders query_plans_headers{ decorrelated_lhs_plan.getCurrentHeader(), decorrelated_rhs_plan.getCurrentHeader() };

        std::vector<QueryPlanPtr> child_plans;
        child_plans.emplace_back(std::make_unique<QueryPlan>(std::move(decorrelated_lhs_plan)));
        child_plans.emplace_back(std::make_unique<QueryPlan>(std::move(decorrelated_rhs_plan)));

        Block union_common_header = buildCommonHeaderForUnion(
            query_plans_headers,
            SelectUnionMode::UNION_ALL,
            settings[Setting::use_variant_as_common_type]); // Union mode doesn't matter here
        addConvertingToCommonHeaderActionsIfNeeded(child_plans, union_common_header, query_plans_headers, context.planner_context->getQueryContext());

        union_step->updateInputHeaders(std::move(query_plans_headers));

        QueryPlan result_plan;
        result_plan.unitePlans(std::move(node->step), std::move(child_plans));

        return result_plan;
    }
    if (auto * aggeregating_step = typeid_cast<AggregatingStep *>(node->step.get()))
    {
        auto decorrelated_query_plan = decorrelateQueryPlan(context, node->children.front());
        auto input_header = decorrelated_query_plan.getCurrentHeader();

        if (aggeregating_step->isGroupingSets())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Decorrelation of GROUP BY GROUPING SETS is not supported yet");

        auto new_aggregator_params = aggeregating_step->getAggregatorParameters();

        for (const auto & correlated_column_identifier : context.correlated_subquery.correlated_column_identifiers)
        {
            new_aggregator_params.keys.push_back(correlated_column_identifier);
        }
        new_aggregator_params.keys_size = new_aggregator_params.keys.size();

        auto result_step = std::make_unique<AggregatingStep>(
            std::move(input_header),
            std::move(new_aggregator_params),
            aggeregating_step->getGroupingSetsParamsList(),
            aggeregating_step->getFinal(),
            aggeregating_step->getMaxBlockSize(),
            aggeregating_step->getMaxBlockSizeForAggregationInOrder(),
            aggeregating_step->getMergeThreads(),
            aggeregating_step->getTemporaryDataMergeThreads(),
            false /*storage_has_evenly_distributed_read_*/,
            aggeregating_step->isGroupByUseNulls(),
            SortDescription{} /*sort_description_for_merging_*/,
            SortDescription{} /*group_by_sort_description_*/,
            aggeregating_step->shouldProduceResultsInBucketOrder(),
            aggeregating_step->usingMemoryBoundMerging(),
            aggeregating_step->explicitSortingRequired()
        );
        result_step->setStepDescription(*aggeregating_step);

        decorrelated_query_plan.addStep(std::move(result_step));

        return decorrelated_query_plan;
    }
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Cannot decorrelate query, because '{}' step is not supported",
        node->step->getName());
}

void buildRenamingForScalarSubquery(
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery
)
{
    ActionsDAG dag(query_plan.getCurrentHeader()->getNamesAndTypesList());
    const auto * result_node = &dag.findInOutputs(correlated_subquery.action_node_name);

    ActionsDAG::NodeRawConstPtrs new_outputs{ result_node };
    new_outputs.reserve(correlated_subquery.correlated_column_identifiers.size() + 1);

    for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
    {
        new_outputs.push_back(&dag.addAlias(dag.findInOutputs(column_name), fmt::format("{}.{}", correlated_subquery.action_node_name, column_name)));
    }

    dag.getOutputs() = std::move(new_outputs);

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(dag));
    expression_step->setStepDescription("Create renaming actions for scalar subquery");
    query_plan.addStep(std::move(expression_step));
}

void buildExistsResultExpression(
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery,
    bool project_only_correlated_columns
)
{
    ActionsDAG dag(query_plan.getCurrentHeader()->getNamesAndTypesList());
    auto result_type = std::make_shared<DataTypeUInt8>();
    auto column = result_type->createColumnConst(1, 1);
    const auto * exists_result = &dag.materializeNode(dag.addColumn(ColumnWithTypeAndName(column, result_type, correlated_subquery.action_node_name)));

    if (project_only_correlated_columns)
    {
        ActionsDAG::NodeRawConstPtrs new_outputs;
        new_outputs.reserve(correlated_subquery.correlated_column_identifiers.size() + 1);

        for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
        {
            new_outputs.push_back(&dag.addAlias(dag.findInOutputs(column_name), fmt::format("{}.{}", correlated_subquery.action_node_name, column_name)));
        }
        new_outputs.push_back(exists_result);

        dag.getOutputs() = std::move(new_outputs);
    }
    else
    {
        dag.addOrReplaceInOutputs(*exists_result);
    }

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(dag));
    expression_step->setStepDescription("Create result for always true EXISTS expression");
    query_plan.addStep(std::move(expression_step));
}

QueryPlan buildLogicalJoin(
    const PlannerContextPtr & planner_context,
    QueryPlan input_stream_plan,
    QueryPlan decorrelated_plan,
    const CorrelatedSubquery & correlated_subquery
)
{
    auto lhs_plan_header = decorrelated_plan.getCurrentHeader();
    auto rhs_plan_header = input_stream_plan.getCurrentHeader();

    using ColumnNameGetter = std::function<String(const String &)>;
    ColumnNameGetter get_lhs_column_name = [&](const String & column_name) -> String {
        return fmt::format("{}.{}", correlated_subquery.action_node_name, column_name);
    };
    ColumnNameGetter get_rhs_column_name = [&](const String & column_name) -> String {
        return column_name;
    };

    auto lhs_plan = std::move(decorrelated_plan);
    auto rhs_plan = std::move(input_stream_plan);

    NameSet output_columns;
    output_columns.insert_range(rhs_plan_header->getNames());
    output_columns.insert(correlated_subquery.action_node_name);

    const auto & settings = planner_context->getQueryContext()->getSettingsRef();

    if (settings[Setting::correlated_subqueries_default_join_kind] == DecorrelationJoinKind::LEFT)
    {
        std::swap(lhs_plan, rhs_plan);
        std::swap(lhs_plan_header, rhs_plan_header);
        std::swap(get_lhs_column_name, get_rhs_column_name);
    }

    JoinExpressionActions join_expression_actions(
        lhs_plan_header->getColumnsWithTypeAndName(),
        rhs_plan_header->getColumnsWithTypeAndName());

    std::vector<JoinActionRef> predicates;
    for (const auto & column_name : correlated_subquery.correlated_column_identifiers)
    {
        std::vector<JoinActionRef> eq_arguments;
        eq_arguments.push_back(join_expression_actions.findNode(get_lhs_column_name(column_name), /* is_input= */ true));
        eq_arguments.push_back(join_expression_actions.findNode(get_rhs_column_name(column_name), /* is_input= */ true));
        auto eq_node = JoinActionRef::transform(eq_arguments, JoinActionRef::AddFunction(JoinConditionOperator::Equals));
        predicates.push_back(std::move(eq_node));
    }

    auto join_kind_to_use = settings[Setting::correlated_subqueries_default_join_kind] == DecorrelationJoinKind::RIGHT ? JoinKind::Right : JoinKind::Left;

    /// Add ANY OUTER JOIN
    auto result_join = std::make_unique<JoinStepLogical>(
        lhs_plan_header,
        rhs_plan_header,
        JoinOperator(join_kind_to_use, JoinStrictness::Any, JoinLocality::Unspecified, std::move(predicates)),
        std::move(join_expression_actions),
        output_columns,
        std::unordered_map<String, const ActionsDAG::Node *>{},
        /*join_use_nulls=*/false,
        JoinSettings(settings),
        SortingStep::Settings(settings));
    result_join->setStepDescription("JOIN to generate result stream");

    QueryPlan result_plan;

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(lhs_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(rhs_plan)));

    result_plan.unitePlans(std::move(result_join), {std::move(plans)});
    return result_plan;
}

Planner buildPlannerForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options
)
{
    auto subquery_options = select_query_options.subquery();
    auto global_planner_context = std::make_shared<GlobalPlannerContext>(nullptr, nullptr, FiltersForTableExpressionMap{});
    /// Register table expression data for correlated columns sources in the global context.
    /// Table expression data would be reused because it can't be initialized
    /// during plan construction for correlated subquery.
    global_planner_context->collectTableExpressionDataForCorrelatedColumns(correlated_subquery.query_tree, planner_context);

    Planner subquery_planner(
        correlated_subquery.query_tree,
        subquery_options,
        std::move(global_planner_context));
    subquery_planner.buildQueryPlanIfNeeded();

    return subquery_planner;
}

void addStepForResultRenaming(
    const CorrelatedSubquery & correlated_subquery,
    QueryPlan & correlated_subquery_plan,
    const PlannerContextPtr & planner_context
)
{
    const auto & header = correlated_subquery_plan.getCurrentHeader();
    const auto & subquery_result_columns = header->getColumnsWithTypeAndName();

    if (subquery_result_columns.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected to get only 1 result column of correlated subquery, but got {}",
            subquery_result_columns.size());

    const auto & result_column = subquery_result_columns[0];
    auto expected_result_type = correlated_subquery.query_tree->getResultType();
    /// Scalar correlated subquery must return nullable result. See method `QueryNode::getResultType()` for details.
    if (!expected_result_type->equals(*makeNullableOrLowCardinalityNullableSafe(result_column.type)))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected {} as correlated subquery result, but got {}",
            expected_result_type->getName(),
            result_column.type->getName());

    ActionsDAG dag(subquery_result_columns);

    const ActionsDAG::Node * result_node = nullptr;
    if (!expected_result_type->equals(*result_column.type))
    {
        result_node = &dag.addCast(
            *dag.getOutputs()[0],
            expected_result_type,
            correlated_subquery.action_node_name,
            planner_context->getQueryContext());
    }
    else
    {
        result_node = &dag.addAlias(*dag.getOutputs()[0], correlated_subquery.action_node_name);
    }

    dag.getOutputs() = { result_node };

    auto expression_step = std::make_unique<ExpressionStep>(header, std::move(dag));
    expression_step->setStepDescription("Create correlated subquery result alias");
    correlated_subquery_plan.addStep(std::move(expression_step));
}

}

/* Build query plan for correlated subquery using decorrelation algorithm
 * on top of relational algebra operators proposed by TU Munich researchers
 * Thomas Neumann and Alfons Kemper.
 *
 * Original research paper "Unnesting Arbitrary Queries": https://cs.emis.de/LNI/Proceedings/Proceedings241/383.pdf
 * See also a follow-up paper, "Improving Unnesting of Complex Queries": https://dl.gi.de/items/b9df4765-d1b0-4267-a77c-4ce4ab0ee62d
 *
 * NOTE: ClickHouse does not explicitly build SQL query into relational algebra expression.
 * Instead, it produces a query plan where almost every step has an analog from relational algebra.
 * This function implements a decorrelation algorithm using the ClickHouse query plan.
 *
 * TODO: Support scalar correlated subqueries.
 * TODO: Support decorrelation of all kinds of query plan steps.
 * TODO: Implement left table substitution optimization: T_left DEPENDENT JOIN T_right is a subset of T_right
 * if T_right has all the necessary columns of T_left.
 */
void buildQueryPlanForCorrelatedSubquery(
    const PlannerContextPtr & planner_context,
    QueryPlan & query_plan,
    const CorrelatedSubquery & correlated_subquery,
    const SelectQueryOptions & select_query_options)
{
    auto * query_node = correlated_subquery.query_tree->as<QueryNode>();  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    auto * union_node = correlated_subquery.query_tree->as<UnionNode>();  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(query_node != nullptr && query_node->isCorrelated() || union_node != nullptr && union_node->isCorrelated());

    switch (correlated_subquery.kind)
    {
        case DB::CorrelatedSubqueryKind::SCALAR:
        {
            Planner subquery_planner = buildPlannerForCorrelatedSubquery(planner_context, correlated_subquery, select_query_options);
            /// Logical plan for correlated subquery
            auto & correlated_query_plan = subquery_planner.getQueryPlan();

            addStepForResultRenaming(correlated_subquery, correlated_query_plan, planner_context);

            /// Mark all query plan steps if they or their subplans contain usage of correlated subqueries.
            /// It's needed to identify the moment when dependent join can be replaced by CROSS JOIN.
            auto correlated_step_map = buildCorrelatedPlanStepMap(correlated_query_plan);

            DecorrelationContext context{
                .correlated_subquery = correlated_subquery,
                .planner_context = planner_context,
                .query_plan = std::move(query_plan),
                .correlated_query_plan = std::move(subquery_planner).extractQueryPlan(),
                .correlated_plan_steps = std::move(correlated_step_map),
                .equivalence_class_stack = { EquivalenceClasses{} }
            };

            auto decorrelated_plan = decorrelateQueryPlan(context, context.correlated_query_plan.getRootNode());
            buildRenamingForScalarSubquery(decorrelated_plan, correlated_subquery);

            /// Use LEFT OUTER JOIN to produce the result plan.
            query_plan = buildLogicalJoin(
                planner_context,
                std::move(context.query_plan),
                std::move(decorrelated_plan),
                correlated_subquery);
            break;
        }
        case CorrelatedSubqueryKind::EXISTS:
        {
            Planner subquery_planner = buildPlannerForCorrelatedSubquery(planner_context, correlated_subquery, select_query_options);
            /// Logical plan for correlated subquery
            auto & correlated_query_plan = subquery_planner.getQueryPlan();

            /// For EXISTS expression we can remove plan steps that doesn't change the number of result rows.
            /// It may also result in non-correlated subquery plan
            /// Example:
            /// SELECT * FROM numbers(1) WHERE EXISTS (SELECT a = number FROM table)
            if (optimizePlanForExists(correlated_query_plan))
            {
                /// Subquery always produces at least 1 row.
                buildExistsResultExpression(query_plan, correlated_subquery, /*project_only_correlated_columns=*/false);
                return;
            }

            /// Mark all query plan steps if they or their subplans contain usage of correlated subqueries.
            /// It's needed to identify the moment when dependent join can be replaced by CROSS JOIN.
            auto correlated_step_map = buildCorrelatedPlanStepMap(correlated_query_plan);

            DecorrelationContext context{
                .correlated_subquery = correlated_subquery,
                .planner_context = planner_context,
                .query_plan = std::move(query_plan),
                .correlated_query_plan = std::move(subquery_planner).extractQueryPlan(),
                .correlated_plan_steps = std::move(correlated_step_map),
                .equivalence_class_stack = { EquivalenceClasses{} }
            };

            auto decorrelated_plan = decorrelateQueryPlan(context, context.correlated_query_plan.getRootNode());
            /// Add a 'exists(<table expression id>)' expression that is always true.
            buildExistsResultExpression(decorrelated_plan, correlated_subquery, /*project_only_correlated_columns=*/true);

            /// Use LEFT OUTER JOIN to produce the result plan.
            /// If there's no corresponding rows from the right side, 'exists(<table expression id>)' would be replaced by default value (false).
            query_plan = buildLogicalJoin(
                planner_context,
                std::move(context.query_plan),
                std::move(decorrelated_plan),
                correlated_subquery);
            break;
        }
    }
}

}
