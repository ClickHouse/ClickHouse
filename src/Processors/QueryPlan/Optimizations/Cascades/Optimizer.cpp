#include <Processors/QueryPlan/Optimizations/Cascades/Optimizer.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/Cascades/JoinGraph.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Task.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <Core/Block_fwd.h>
#include <Core/Settings.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <algorithm>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool join_use_nulls;
}

static String dumpQueryPlanShort(const QueryPlan & query_plan)
{
    WriteBufferFromOwnString out;
    query_plan.explainPlan(out, {.estimates = true});
    return out.str();
}

CascadesOptimizer::CascadesOptimizer(QueryPlan & query_plan_)
    : query_plan(query_plan_)
{}

bool collectJoins(QueryPlan::Node * node, JoinGraphBuilder & join_graph_builder)
{
    if (!node)
        return false;

    auto * join_step = typeid_cast<JoinStepLogical *>(node->step.get());
    if (join_step && join_step->getJoinOperator().kind == JoinKind::Inner)
    {
        if (node->children.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Join step has {} children instead of 2", node->children.size());

        if (!collectJoins(node->children[0], join_graph_builder))
            join_graph_builder.addRelation({}, *node->children[0]);

        if (!collectJoins(node->children[1], join_graph_builder))
            join_graph_builder.addRelation({}, *node->children[1]);

        for (const auto & e : join_step->getJoinOperator().expression)
        {
            const auto predicate = e.asBinaryPredicate();
            if (std::get<0>(predicate) == JoinConditionOperator::Equals)
            {
                const auto & lhs_column = get<1>(predicate).getColumnName();
                const auto & rhs_column = get<2>(predicate).getColumnName();
                join_graph_builder.addEqualityPredicate(lhs_column, rhs_column);
            }
        }

        return true;
    }

    auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (expression_step)
    {
        return collectJoins(node->children[0], join_graph_builder);
    }

    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (filter_step)
    {
        return collectJoins(node->children[0], join_graph_builder);
    }

    return false;
}

GroupId CascadesOptimizer::fillMemoFromQueryPlan(OptimizerContext & optimizer_context)
{
    /// Traverse from the root till the first join node
    /// Create groups for plan steps above JOIN graph
    QueryPlan::Node * node = query_plan.getRootNode();
    GroupExpressionPtr expression_above_join_graph;
    std::optional<GroupId> root_group_id;
    while (node && node->children.size() == 1)
    {
        auto group_expression = std::make_shared<GroupExpression>(std::move(node->step));
        auto group_id = optimizer_context.memo.addGroup(group_expression);
        if (!root_group_id.has_value())
            root_group_id = group_id;
        if (expression_above_join_graph)
            expression_above_join_graph->inputs = {{group_id, {}}};
        expression_above_join_graph = group_expression;
        node = node->children.front();
    }

    JoinGraphBuilder join_graph_builder;
    if (!collectJoins(node, join_graph_builder))
        join_graph_builder.addRelation({}, *node);
    JoinGraph join_graph(std::move(join_graph_builder));

    LOG_TEST(optimizer_context.log, "JOIN Graph:\n{}", join_graph.dump());

    auto join_graph_group_id = populateMemoFromJoinGraph(join_graph, optimizer_context);

    if (expression_above_join_graph)
    {
        expression_above_join_graph->inputs = {{join_graph_group_id, {}}};
        return root_group_id.value();
    }
    else
    {
        return join_graph_group_id;
    }
}

static bool setsIntersect(const std::unordered_set<JoinGraph::RelationId> & a, const std::unordered_set<JoinGraph::RelationId> & b)
{
    if (&a == &b)
        return true;

    if (a.size() > b.size())
        return setsIntersect(b, a);

    for (const auto & ai : a)
    {
        if (b.contains(ai))
            return true;
    }

    return false;
}

using RelationsSet = std::unordered_set<JoinGraph::RelationId>;

String normalizedGroupName(const RelationsSet & relations)
{
    std::vector<JoinGraph::RelationId> sorted(relations.begin(), relations.end());
    std::sort(sorted.begin(), sorted.end());
    return fmt::format("{}", fmt::join(sorted, ","));
}

JoinGraph::PredicatesSet listEdges(
    const JoinGraph & join_graph,
    const RelationsSet & from_relations,
    const RelationsSet & to_relations)
{
    JoinGraph::PredicatesSet result;

    // FIXME: Use equivalence classes for join graph to include only 1 predicate with left an right columns from the same class
    std::unordered_set<String> already_added_left_columns;
    std::unordered_set<String> already_added_right_columns;

    for (const auto & from : from_relations)
    {
        for (const auto & to : to_relations)
        {
            auto predicates = join_graph.getPredicates(from, to);

            for (const auto & predicate : predicates)
            {
                /// Skip adding transitive predicate multiple times:
                /// e.g. (ps_suppkey = l_suppkey) AND (ps_suppkey = s_suppkey)
                /// when doing 'ps JOIN (l JOIN s ON l_suppkey = s_suppkey) ON ps_suppkey = l_suppkey'
                if (already_added_left_columns.contains(predicate.first) || already_added_right_columns.contains(predicate.second))
                    continue;
                result.insert(predicate);
                already_added_left_columns.insert(predicate.first);
                already_added_right_columns.insert(predicate.second);
            }
        }
    }

    return result;
}

GroupId CascadesOptimizer::populateMemoFromJoinGraph(const JoinGraph & join_graph, OptimizerContext & optimizer_context)
{
    struct GroupInfo
    {
        const GroupId group_id;   /// in Memo
        RelationsSet relations;   /// All relations
        SharedHeader output_columns;
    };

    const size_t relations_count = join_graph.size();

    std::unordered_map<String, GroupId> known_join_groups;

    std::vector<std::list<GroupInfo>> join_groups_by_size;
    join_groups_by_size.reserve(relations_count);

    /// Add group of size = 1 for each input relation of the join graph
    join_groups_by_size.resize(2);  /// 0-sized will be empty, and 1-sized will contain source relations
    for (JoinGraph::RelationId relation_id = 0; relation_id < relations_count; ++relation_id)
    {
        QueryPlan::Node & relation_node = join_graph.getRelationNode(relation_id);
        auto header = relation_node.step->getOutputHeader();
        auto relation_group_id = optimizer_context.addGroup(relation_node);
        join_groups_by_size[1].push_back(GroupInfo{relation_group_id, {relation_id}, header});
    }

    /// Generate groups of all sizes from 2 to number of relations
    for (size_t group_size = 2; group_size <= relations_count; ++group_size)
    {
        join_groups_by_size.emplace_back();
        /// Iterate over possible sizes for 2 subroups that can be joined into a group of this size
        for (size_t larger_subgroup_size = (group_size + 1) / 2; larger_subgroup_size < group_size; ++larger_subgroup_size)
        {
            const size_t smaller_subgroup_size = group_size - larger_subgroup_size;
            for (const auto & larger_subgroup : join_groups_by_size[larger_subgroup_size])
            {
                for (const auto & smaller_subgroup : join_groups_by_size[smaller_subgroup_size])
                {
                    /// If larger_subgroup_size == smaller_subgroup_size then we should not enumerate the same pair twice as {i,j} and {j,i}
                    if (larger_subgroup_size == smaller_subgroup_size && larger_subgroup.group_id <= smaller_subgroup.group_id)
                        continue;

                    /// We can join 2 subgroups if they do not intersect
                    if (setsIntersect(larger_subgroup.relations, smaller_subgroup.relations))
                        continue;

                    /// And if there are predicates connecting relations from these 2 groups
                    auto predicates = listEdges(join_graph, larger_subgroup.relations, smaller_subgroup.relations);
                    if (predicates.empty())
                        continue;

                    Block joined_output_columns(larger_subgroup.output_columns->cloneEmpty());
                    for (const auto & column_from_smaller_group : smaller_subgroup.output_columns->getColumnsWithTypeAndName())
                        joined_output_columns.insert(column_from_smaller_group);

                    /// Add expression for joining larger_subgroup with smaller_subgroup on the predicates from edges
                    auto join_expression = std::make_shared<GroupExpression>(nullptr); // TODO:
                    join_expression->inputs = {{larger_subgroup.group_id, {}}, {smaller_subgroup.group_id, {}}};
#if 0
                    {

                        JoinExpressionActions join_expression_actions(
                            larger_subgroup.output_columns->getColumnsWithTypeAndName(),
                            smaller_subgroup.output_columns->getColumnsWithTypeAndName(),
                            joined_output_columns.getColumnsWithTypeAndName());

                        std::vector<JoinPredicate> join_predicates;
                        for (const auto & predicate : predicates)
                        {
                            const auto * left_node = &join_expression_actions.left_pre_join_actions->findInOutputs(predicate.first);
                            const auto * right_node = &join_expression_actions.right_pre_join_actions->findInOutputs(predicate.second);

                            JoinPredicate join_predicate{
                                .left_node = JoinActionRef(left_node, join_expression_actions.left_pre_join_actions.get()),
                                .right_node = JoinActionRef(right_node, join_expression_actions.right_pre_join_actions.get()),
                                .op = PredicateOperator::Equals
                            };

                            join_predicates.emplace_back(std::move(join_predicate));
                        }

                        const auto & settings = CurrentThread::get().getQueryContext()->getSettingsRef();

                        auto join_step = std::make_unique<JoinStepLogical>(
                            larger_subgroup.output_columns,
                            smaller_subgroup.output_columns,
                            JoinInfo{
                                .expression = JoinExpression{
                                    .condition = JoinCondition{
                                        .predicates = std::move(join_predicates),
                                        .left_filter_conditions = {},
                                        .right_filter_conditions = {},
                                        .residual_conditions = {}
                                    },
                                    .disjunctive_conditions = {}
                                },
                                .kind = JoinKind::Inner,
                                .strictness = JoinStrictness::All,
                                .locality = JoinLocality::Local
                            },
                            std::move(join_expression_actions),
                            joined_output_columns.getNames(),
                            settings[Setting::join_use_nulls],
                            JoinSettings{settings},
                            SortingStep::Settings{settings});

                        join_step->setStepDescription("JOIN '" +
                                normalizedGroupName(larger_subgroup.relations) + "', '" +
                                normalizedGroupName(smaller_subgroup.relations) + "'");

                        join_expression->plan_step = std::move(join_step);
                    }
#else
                    {
                        const auto & settings = CurrentThread::get().getQueryContext()->getSettingsRef();

                        auto left_header = larger_subgroup.output_columns;
                        auto right_header = smaller_subgroup.output_columns;

                        JoinExpressionActions join_expression_actions(*left_header, *right_header);

                        std::vector<JoinActionRef> predicate_action_refs;
                        for (const auto & predicate : predicates)
                        {
                            std::vector<JoinActionRef> eq_arguments;
                            eq_arguments.push_back(join_expression_actions.findNode(predicate.first, /* is_input= */ true));
                            eq_arguments.push_back(join_expression_actions.findNode(predicate.second, /* is_input= */ true));
                            auto eq_node = JoinActionRef::transform(eq_arguments, JoinActionRef::AddFunction(JoinConditionOperator::Equals));
                            predicate_action_refs.push_back(std::move(eq_node));
                        }

                        JoinOperator join_operator(
                            predicate_action_refs.empty() ? JoinKind::Cross : JoinKind::Inner,
                            JoinStrictness::All,
                            JoinLocality::Unspecified,
                            std::move(predicate_action_refs)
                        );

                        auto join_step = std::make_unique<JoinStepLogical>(
                            larger_subgroup.output_columns,
                            smaller_subgroup.output_columns,
                            std::move(join_operator),
                            std::move(join_expression_actions),
                            joined_output_columns.getNamesAndTypesList().getNameSet(),
                            std::unordered_map<String, const ActionsDAG::Node *>{},
                            /*join_use_nulls=*/false,
                            JoinSettings{settings},
                            SortingStep::Settings{settings});

                        join_step->setStepDescription(fmt::format("JOIN '{}' and '{}'",
                                normalizedGroupName(larger_subgroup.relations),
                                normalizedGroupName(smaller_subgroup.relations)), 200);

                        join_expression->plan_step = std::move(join_step);
                    }
#endif
                    /// Add or create new group for the combined set of relations
                    RelationsSet joined_relations(larger_subgroup.relations);
                    joined_relations.insert(smaller_subgroup.relations.begin(), smaller_subgroup.relations.end());
                    auto joined_group_name = normalizedGroupName(joined_relations);

                    auto known_group = known_join_groups.find(joined_group_name);
                    if (known_group == known_join_groups.end())
                    {
                        auto joined_group_id = optimizer_context.memo.addGroup(join_expression);
                        join_groups_by_size[group_size].push_back(GroupInfo{joined_group_id, joined_relations, std::make_shared<const Block>(joined_output_columns)});
                        known_join_groups[joined_group_name] = joined_group_id;
                        LOG_TEST(optimizer_context.log, "Created new group for '{}', id {}", joined_group_name, joined_group_id);
                    }
                    else
                    {
                        auto existing_group = optimizer_context.getGroup(known_group->second);
                        existing_group->addLogicalExpression(join_expression);
                        LOG_TEST(optimizer_context.log, "Adding to existing group for '{}' id {}", joined_group_name, known_group->second);
                    }
                }
            }
        }
    }

    /// The biggest group is the root
    chassert(join_groups_by_size.back().size() == 1);
    return join_groups_by_size.back().front().group_id;
}

void CascadesOptimizer::optimize()
{
    Stopwatch optimizer_timer;
    OptimizerStatisticsPtr statistics;
    /// FIXME: statistics stub for testing
    auto query_context = CurrentThread::get().getQueryContext();
    constexpr auto stats_hint_param_name = "_internal_join_table_stat_hints";
    if (query_context->getQueryParameters().contains(stats_hint_param_name))
        statistics = createStatisticsFromHint(query_context->getQueryParameters().at(stats_hint_param_name));
    else
        statistics = createEmptyStatistics();

    OptimizerContext optimizer_context(*statistics);

    LOG_TEST(optimizer_context.log, "Initial query plan:\n{}", dumpQueryPlanShort(query_plan));

    auto root_group_id = fillMemoFromQueryPlan(optimizer_context);

    LOG_TEST(optimizer_context.log, "Initial memo:\n{}", optimizer_context.memo.dump());

    /// Add task to optimize root group
    CostLimit initial_cost_limit = CostLimit(std::numeric_limits<Int64>::max());
    optimizer_context.pushTask(std::make_shared<OptimizeGroupTask>(root_group_id, ExpressionProperties{}, initial_cost_limit));

    /// Limit the time in terms of optimization tasks instead of wall clock time. This is done for stability of generated plans regardless of system load.
    /// Guys from MS SQL Server describe this in Andy Pavlo's seminar: https://www.youtube.com/watch?v=pQe1LQJiXN0
    const size_t executed_tasks_limit = 100000;
    size_t executed_tasks_count = 0;
    for (; !optimizer_context.tasks.empty() && executed_tasks_count < executed_tasks_limit; ++executed_tasks_count)
    {
        auto task = optimizer_context.tasks.top();
        optimizer_context.tasks.pop();
        task->execute(optimizer_context);
    }

    LOG_TEST(optimizer_context.log, "Executed {} tasks, Memo after:\n{}", executed_tasks_count, optimizer_context.memo.dump());

    /// Get the best plan for the root group
    auto best_plan = buildBestPlan(root_group_id, {}, optimizer_context.memo);

    LOG_TEST(optimizer_context.log, "Optimized plan:\n{}", dumpQueryPlanShort(*best_plan));

    /// Update the original plan in-place because there might be references to the root node of the original plan
    query_plan.replaceNodeWithPlan(query_plan.getRootNode(), std::move(*best_plan));

    LOG_TRACE(optimizer_context.log, "Optimization took {} ms", optimizer_timer.elapsedMilliseconds());
}

/// Drop unused columns and reorder columns between steps if needed
void addConvertingExpression(QueryPlan & plan, const SharedHeader & expected_header)
{
    if (!blocksHaveEqualStructure(*plan.getCurrentHeader(), *expected_header))
    {
        auto actions_dag = ActionsDAG::makeConvertingActions(
                plan.getCurrentHeader()->getColumnsWithTypeAndName(),
                expected_header->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                nullptr);
        auto converting_step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(actions_dag));
        converting_step->setStepDescription("Convert column list");
        plan.addStep(std::move(converting_step));
    }
}

QueryPlanPtr CascadesOptimizer::buildBestPlan(GroupId subtree_root_group_id, ExpressionProperties required_properties, const Memo & memo)
{
    auto group = memo.getGroup(subtree_root_group_id);
    auto group_best_expression = group->getBestImplementation(required_properties).expression;
    QueryPlanPtr plan_for_group;
    if (group_best_expression->inputs.empty())
    {
        auto leaf_plan = std::make_unique<QueryPlan>();
        leaf_plan->addStep(group_best_expression->getQueryPlanStep()->clone());
        plan_for_group = std::move(leaf_plan);
    }
    else if (group_best_expression->inputs.size() == 1)
    {
        const auto & input = group_best_expression->inputs.front();
        auto child_plan = buildBestPlan(input.group_id, input.required_properties, memo);
        auto step = group_best_expression->getQueryPlanStep()->clone();
        addConvertingExpression(*child_plan, step->getInputHeaders().at(0));
        child_plan->addStep(std::move(step));
        plan_for_group = std::move(child_plan);
    }
    else
    {
        std::vector<QueryPlanPtr> input_plans;
        input_plans.reserve(group_best_expression->inputs.size());
        auto step = group_best_expression->getQueryPlanStep()->clone();
        for (size_t i = 0; i < group_best_expression->inputs.size(); ++i)
        {
            const auto & input = group_best_expression->inputs[i];
            auto input_plan = buildBestPlan(input.group_id, input.required_properties, memo);
            addConvertingExpression(*input_plan, step->getInputHeaders().at(i));
            input_plans.push_back(std::move(input_plan));
        }
        auto united_plan = std::make_unique<QueryPlan>();
        united_plan->unitePlans(std::move(step), std::move(input_plans));
        plan_for_group = std::move(united_plan);
    }

    /// Put all property enforcer steps on top
    for (auto & enforcer_step : group_best_expression->property_enforcer_steps)
    {
        plan_for_group->addStep(std::move(enforcer_step));
    }

    plan_for_group->getRootNode()->cost_estimation = CostEstimationInfo
        {
            .cost = group_best_expression->cost->subtree_cost,
            .rows = group_best_expression->statistics->estimated_row_count
        };

    LOG_TEST(getLogger("buildBestPlan"), "Plan for group #{}:\n{}", subtree_root_group_id, dumpQueryPlanShort(*plan_for_group));

    return plan_for_group;
}

}
