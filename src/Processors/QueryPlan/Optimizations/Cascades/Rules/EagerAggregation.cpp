#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Functions/FunctionFactory.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// An element in the chain from aggregation down to a target join.
struct ChainElement
{
    GroupExpressionPtr expr;
    bool is_join = false;
    /// For joins: which input index leads toward the target join.
    size_t downward_input_idx = 0;
};

struct JoinKeyPair { String left; String right; };

/// Reconstruct a `JoinStepLogical` with updated input headers, re-creating
/// equi-join predicates in fresh `JoinExpressionActions`.  Returns nullptr
/// if any predicate column is missing from the new headers.
static std::unique_ptr<JoinStepLogical> reconstructJoin(
    const JoinStepLogical & original,
    SharedHeader left_hdr,
    SharedHeader right_hdr,
    const String & description_prefix)
{
    JoinExpressionActions expr_actions(*left_hdr, *right_hdr);

    auto join_operator = original.getJoinOperator();
    join_operator.expression.clear();
    join_operator.residual_filter.clear();
    for (const auto & predicate : original.getJoinOperator().expression)
    {
        auto [op, left_node, right_node] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            continue;

        auto lref = expr_actions.findNode(left_node.getColumnName(), /*is_input=*/true, /*throw_if_not_found=*/false);
        auto rref = expr_actions.findNode(right_node.getColumnName(), /*is_input=*/true, /*throw_if_not_found=*/false);
        if (!lref || !rref)
            return nullptr;

        auto eq_resolver = FunctionFactory::instance().get("equals", nullptr);
        const auto * eq_node = &expr_actions.getActionsDAG()->addFunction(
            eq_resolver,
            {lref.getNode(), rref.getNode()},
            fmt::format("equals({}, {})", left_node.getColumnName(), right_node.getColumnName()));
        join_operator.expression.push_back(JoinActionRef(eq_node, expr_actions));
    }

    std::vector<const ActionsDAG::Node *> outputs;
    for (const auto * node : expr_actions.getActionsDAG()->getInputs())
        outputs.push_back(node);
    expr_actions.getActionsDAG()->getOutputs() = {outputs.begin(), outputs.end()};

    auto step = std::make_unique<JoinStepLogical>(
        left_hdr, right_hdr,
        std::move(join_operator),
        std::move(expr_actions),
        std::move(outputs),
        original.getJoinSettings(),
        original.getSortingSettings());
    step->setStepDescription(
        fmt::format("{}{}", description_prefix, original.getStepDescription()), 200);
    return step;
}

/// Determine which join input (0 or 1) has the aggregate argument columns.
/// Checks headers directly, then traces through Expression DAGs to find
/// base columns.  Returns {push_side, true} on success, {0, false} if
/// the agg arguments are not in either side of this join.
static std::pair<size_t, bool> determinePushSide(
    const Block & header0,
    const Block & header1,
    const NameSet & agg_argument_columns,
    const std::vector<GroupExpressionPtr> & passthrough_steps)
{
    /// Direct match: agg argument column name in a header.
    for (const auto & arg : agg_argument_columns)
    {
        if (header0.has(arg)) return {0, true};
        if (header1.has(arg)) return {1, true};
    }

    /// Trace through passthrough Expression DAGs to find base INPUT columns.
    for (const auto & pt_step : passthrough_steps)
    {
        const auto * expr_step = typeid_cast<const ExpressionStep *>(pt_step->getQueryPlanStep());
        if (!expr_step)
            continue;

        for (const auto & arg : agg_argument_columns)
        {
            const auto * arg_node = expr_step->getExpression().tryFindInOutputs(arg);
            if (!arg_node)
                continue;

            /// BFS to find INPUT nodes reachable from this arg.
            std::vector<const ActionsDAG::Node *> stack = {arg_node};
            while (!stack.empty())
            {
                const auto * node = stack.back();
                stack.pop_back();
                if (node->type == ActionsDAG::ActionType::INPUT)
                {
                    const auto & col = node->result_name;
                    if (header0.has(col) && !header1.has(col))
                        return {0, true};
                    if (header1.has(col) && !header0.has(col))
                        return {1, true};
                }
                for (const auto * child : node->children)
                    stack.push_back(child);
            }
        }
    }
    return {0, false};
}

/// Check that ALL GROUP BY keys are either join keys on the pushed side,
/// or available from the other side of this join or an intermediate join.
static bool checkKeysCompatible(
    const Names & group_keys,
    const NameSet & push_side_join_key_set,
    const JoinStepLogical & join_step,
    size_t push_side,
    const std::vector<ChainElement> & chain)
{
    NameSet available_columns;
    {
        size_t other_side_idx = 1 - push_side;
        const auto & other_header = *join_step.getInputHeaders()[other_side_idx];
        for (const auto & col : other_header)
            available_columns.insert(col.name);
    }
    for (const auto & elem : chain)
    {
        if (!elem.is_join)
            continue;
        const auto * inter_join = typeid_cast<const JoinStepLogical *>(elem.expr->getQueryPlanStep());
        if (!inter_join)
            continue;
        size_t other_idx = 1 - elem.downward_input_idx;
        const auto & other_hdr = *inter_join->getInputHeaders()[other_idx];
        for (const auto & col : other_hdr)
            available_columns.insert(col.name);
    }

    for (const auto & key : group_keys)
    {
        if (push_side_join_key_set.contains(key) || available_columns.contains(key))
            continue;
        return false;
    }
    return true;
}

/// Extract the minimal ActionsDAG subgraph that computes the aggregate
/// argument expressions from push-side columns.  Also adds join keys as
/// passthrough INPUT→OUTPUT if they are not in the subgraph.  Returns
/// nullopt if the agg arguments cannot be extracted from any Expression.
static std::optional<ActionsDAG> extractAggArgumentDAG(
    const NameSet & agg_argument_columns,
    const Names & push_side_join_keys,
    const Block & push_side_header,
    const std::vector<GroupExpressionPtr> & passthrough_steps)
{
    for (const auto & pt_step : passthrough_steps)
    {
        const auto * expr_step = typeid_cast<const ExpressionStep *>(pt_step->getQueryPlanStep());
        if (!expr_step)
            continue;

        ActionsDAG::NodeRawConstPtrs needed_outputs;
        const auto & dag = expr_step->getExpression();
        bool all_found = true;

        for (const auto & arg_col : agg_argument_columns)
        {
            const auto * node = dag.tryFindInOutputs(arg_col);
            if (!node) { all_found = false; break; }
            needed_outputs.push_back(node);
        }
        /// Join keys may or may not be in the Expression outputs —
        /// they are raw push-side columns, so include if present.
        for (const auto & jk : push_side_join_keys)
        {
            const auto * node = dag.tryFindInOutputs(jk);
            if (node)
                needed_outputs.push_back(node);
        }
        if (!all_found)
            continue;

        auto sub_dag = ActionsDAG::cloneSubDAG(needed_outputs, /*remove_aliases=*/false);

        bool inputs_ok = true;
        for (const auto * input_node : sub_dag.getInputs())
        {
            if (!push_side_header.has(input_node->result_name))
            {
                inputs_ok = false;
                break;
            }
        }
        if (!inputs_ok)
            continue;

        /// Add join key columns as passthrough INPUT→OUTPUT if the
        /// subgraph doesn't already produce them.
        for (const auto & jk : push_side_join_keys)
        {
            if (!sub_dag.tryFindInOutputs(jk) && push_side_header.has(jk))
            {
                const auto & col = push_side_header.getByName(jk);
                const auto * input_node = &sub_dag.addInput(col.name, col.type);
                sub_dag.getOutputs().push_back(input_node);
            }
        }

        return std::move(sub_dag);
    }
    return std::nullopt;
}

/// Validate that a header has all required key and aggregate argument columns.
static bool validateHeader(
    const Block & header,
    const Names & keys,
    const std::vector<AggregateDescription> & aggregates)
{
    for (const auto & key : keys)
        if (!header.has(key))
            return false;
    for (const auto & agg_desc : aggregates)
        for (const auto & arg : agg_desc.argument_names)
            if (!header.has(arg))
                return false;
    return true;
}

/// Walk the chain bottom-up, reconstructing intermediate joins with updated
/// headers.  Returns {final_group_id, final_header} or nullopt on failure.
static std::optional<std::pair<GroupId, SharedHeader>> reconstructChain(
    const std::vector<ChainElement> & chain,
    GroupId start_group_id,
    SharedHeader start_header,
    Memo & memo)
{
    GroupId current_group_id = start_group_id;
    SharedHeader current_header = start_header;

    for (auto it = chain.rbegin(); it != chain.rend(); ++it)
    {
        const auto & elem = *it;
        if (!elem.is_join)
            continue;

        const auto * inter_join = typeid_cast<const JoinStepLogical *>(elem.expr->getQueryPlanStep());
        if (!inter_join)
            return std::nullopt;

        size_t down_idx = elem.downward_input_idx;
        size_t up_idx = 1 - down_idx;

        SharedHeader left_hdr = (down_idx == 0) ? current_header : inter_join->getInputHeaders()[up_idx];
        SharedHeader right_hdr = (down_idx == 0) ? inter_join->getInputHeaders()[up_idx] : current_header;

        auto inter_join_step = reconstructJoin(*inter_join, left_hdr, right_hdr, "EagerChain: ");
        if (!inter_join_step)
            return std::nullopt;

        current_header = inter_join_step->getOutputHeader();

        GroupExpressionPtr inter_expr = std::make_shared<GroupExpression>(std::move(inter_join_step));
        if (down_idx == 0)
            inter_expr->inputs = {{current_group_id, {}}, elem.expr->inputs[up_idx]};
        else
            inter_expr->inputs = {elem.expr->inputs[up_idx], {current_group_id, {}}};
        current_group_id = memo.addGroup(inter_expr);
    }

    return std::pair{current_group_id, current_header};
}


// ────────────────────────────────────────────────────────────────────────────
// EagerAggregation rule
// ────────────────────────────────────────────────────────────────────────────

/// Pushes partial aggregation below a join when GROUP BY keys include the
/// join key.  Reduces rows before the join, shrinking intermediate results.
///
/// For multi-join queries (e.g. TPC-H Q10), the rule can push through
/// intermediate joins by reconstructing the chain with updated headers.
///
/// Pattern: Aggregating(final) → ... → JoinLogical(left, right)
/// Result:  MergingAggregated → ... → JoinLogical(left, Aggregating(partial, right))
class EagerAggregation : public IOptimizationRule
{
public:
    String getName() const override { return "EagerAggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 3000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool EagerAggregation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    if (!agg_step)
        return false;
    if (expression->strategy != nullptr)
        return false;
    if (!agg_step->getFinal())
        return false;
    if (agg_step->getParams().keys.empty())
        return false;
    if (agg_step->isGroupingSets() || agg_step->getParams().overflow_row)
        return false;

    /// Find a JoinLogical with an equi-join predicate below the aggregation.
    chassert(expression->inputs.size() == 1);

    auto find_join = [&](GroupId group_id, auto & self, int depth) -> bool
    {
        if (depth > 10)
            return false;
        auto group = memo.getGroup(group_id);
        for (const auto & child_expr : group->logical_expressions)
        {
            if (const auto * join_step = typeid_cast<const JoinStepLogical *>(child_expr->getQueryPlanStep()))
            {
                for (const auto & predicate : join_step->getJoinOperator().expression)
                {
                    auto [op, left_node, right_node] = predicate.asBinaryPredicate();
                    if (op == JoinConditionOperator::Equals
                        && ((left_node.fromLeft() && right_node.fromRight())
                            || (left_node.fromRight() && right_node.fromLeft())))
                        return true;
                }
                for (const auto & input : child_expr->inputs)
                    if (self(input.group_id, self, depth + 1))
                        return true;
            }
            else if (child_expr->inputs.size() == 1
                && typeid_cast<const ExpressionStep *>(child_expr->getQueryPlanStep()))
            {
                if (self(child_expr->inputs[0].group_id, self, depth + 1))
                    return true;
            }
        }
        return false;
    };

    return find_join(expression->inputs[0].group_id, find_join, 0);
}

std::vector<GroupExpressionPtr> EagerAggregation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    const auto * agg_step = typeid_cast<const AggregatingStep *>(expression->getQueryPlanStep());
    chassert(agg_step && expression->inputs.size() == 1);

    /// Collect all join candidates reachable from the agg, tracking the chain
    /// of intermediate steps (Expression steps and joins) between agg and each candidate.
    /// FilterSteps are NOT followed — pushing aggregation past a filter would
    /// aggregate rows that should have been discarded.
    struct JoinCandidate
    {
        GroupExpressionPtr join_expr;
        std::vector<ChainElement> chain;
    };
    std::vector<JoinCandidate> join_candidates;

    auto collect_joins = [&](GroupId group_id, std::vector<ChainElement> chain, auto & self, int depth) -> void
    {
        if (depth > 10)
            return;
        auto group = memo.getGroup(group_id);
        for (const auto & child_expr : group->logical_expressions)
        {
            if (typeid_cast<const JoinStepLogical *>(child_expr->getQueryPlanStep())
                && child_expr->inputs.size() == 2)
            {
                join_candidates.push_back({child_expr, chain});
                for (size_t i = 0; i < child_expr->inputs.size(); ++i)
                {
                    auto deeper_chain = chain;
                    deeper_chain.push_back({child_expr, /*is_join=*/true, /*downward_input_idx=*/i});
                    self(child_expr->inputs[i].group_id, deeper_chain, self, depth + 1);
                }
            }
            else if (child_expr->inputs.size() == 1
                && typeid_cast<const ExpressionStep *>(child_expr->getQueryPlanStep()))
            {
                auto new_chain = chain;
                new_chain.push_back({child_expr, /*is_join=*/false});
                self(child_expr->inputs[0].group_id, new_chain, self, depth + 1);
            }
        }
    };
    collect_joins(expression->inputs[0].group_id, {}, collect_joins, 0);

    /// Build set of all columns referenced by aggregate functions.
    NameSet agg_argument_columns;
    for (const auto & agg_desc : agg_step->getParams().aggregates)
        for (const auto & arg : agg_desc.argument_names)
            agg_argument_columns.insert(arg);

    std::vector<GroupExpressionPtr> result;

    for (const auto & [join_expr_ptr, chain] : join_candidates)
    {
        const auto * join_step = typeid_cast<const JoinStepLogical *>(join_expr_ptr->getQueryPlanStep());
        chassert(join_step);

        /// Extract equi-join key pairs (left = fromLeft, right = fromRight).
        std::vector<JoinKeyPair> equi_keys;
        for (const auto & predicate : join_step->getJoinOperator().expression)
        {
            auto [op, left_node, right_node] = predicate.asBinaryPredicate();
            if (op != JoinConditionOperator::Equals)
                continue;
            if (left_node.fromRight() && right_node.fromLeft())
                std::swap(left_node, right_node);
            else if (!left_node.fromLeft() || !right_node.fromRight())
                continue;
            equi_keys.push_back({left_node.getColumnName(), right_node.getColumnName()});
        }
        if (equi_keys.empty())
            continue;

        /// Extract passthrough (non-join) steps from the chain.
        std::vector<GroupExpressionPtr> passthrough_steps;
        for (const auto & elem : chain)
            if (!elem.is_join)
                passthrough_steps.push_back(elem.expr);

        /// Determine which join side has the aggregate argument columns.
        const auto & header0 = *join_step->getInputHeaders()[0];
        const auto & header1 = *join_step->getInputHeaders()[1];
        auto [push_side, found_push] = determinePushSide(header0, header1, agg_argument_columns, passthrough_steps);
        if (!found_push)
        {
            LOG_TEST(getLogger("EagerAgg"), "skip: agg arguments not in either join side");
            continue;
        }

        /// Determine join keys for the pushed side.
        const auto & push_hdr = *join_step->getInputHeaders()[push_side];
        Names push_side_join_keys;
        for (const auto & [lc, rc] : equi_keys)
        {
            if (push_hdr.has(lc))
                push_side_join_keys.push_back(lc);
            else if (push_hdr.has(rc))
                push_side_join_keys.push_back(rc);
        }

        NameSet push_side_join_key_set(push_side_join_keys.begin(), push_side_join_keys.end());
        if (!checkKeysCompatible(agg_step->getParams().keys, push_side_join_key_set, *join_step, push_side, chain))
        {
            LOG_TEST(getLogger("EagerAgg"), "skip: keys not compatible. "
                "group_keys=[{}], push_join_keys=[{}]",
                fmt::join(agg_step->getParams().keys, ","),
                fmt::join(push_side_join_keys, ","));
            continue;
        }

        /// If agg arguments are computed expressions (not raw push-side columns),
        /// extract the minimal Expression subgraph that computes them.
        const auto & push_side_input_header = *join_step->getInputHeaders()[push_side];
        bool header_has_args = true;
        for (const auto & arg_col : agg_argument_columns)
        {
            if (!push_side_input_header.has(arg_col))
            {
                header_has_args = false;
                break;
            }
        }

        std::optional<ActionsDAG> extracted_dag;
        if (!header_has_args)
        {
            extracted_dag = extractAggArgumentDAG(agg_argument_columns, push_side_join_keys, push_side_input_header, passthrough_steps);
            if (!extracted_dag)
            {
                LOG_TEST(getLogger("EagerAgg"), "skip: no extracted DAG");
                continue;
            }
        }

        /// Build partial aggregation: keys = push-side join keys.
        const auto & orig_params = agg_step->getParams();
        Aggregator::Params inner_params(
            push_side_join_keys,
            orig_params.aggregates,
            /*overflow_row=*/false,
            orig_params.max_rows_to_group_by,
            orig_params.group_by_overflow_mode,
            orig_params.group_by_two_level_threshold,
            orig_params.group_by_two_level_threshold_bytes,
            orig_params.max_bytes_before_external_group_by,
            orig_params.empty_result_for_aggregation_by_empty_set,
            orig_params.tmp_data_scope,
            orig_params.max_threads,
            orig_params.min_free_disk_space,
            orig_params.compile_aggregate_expressions,
            orig_params.min_count_to_compile_aggregate_expression,
            orig_params.max_block_size,
            orig_params.enable_prefetch,
            /*only_merge=*/false,
            /*optimize_group_by_constant_keys=*/false,
            orig_params.min_hit_rate_to_use_consecutive_keys_optimization,
            orig_params.stats_collecting_params,
            orig_params.enable_producing_buckets_out_of_order_in_aggregation,
            orig_params.serialize_string_with_zero_byte);

        SharedHeader partial_input_header;
        if (extracted_dag)
        {
            Block header_block;
            for (const auto & col : extracted_dag->getResultColumns())
                header_block.insert(ColumnWithTypeAndName(col.type->createColumn(), col.type, col.name));
            partial_input_header = std::make_shared<Block>(std::move(header_block));
        }
        else
        {
            partial_input_header = join_step->getInputHeaders()[push_side];
        }

        if (!validateHeader(*partial_input_header, push_side_join_keys, orig_params.aggregates))
        {
            LOG_TEST(getLogger("EagerAgg"), "skip: partial header missing columns");
            continue;
        }

        auto partial_step_ptr = std::make_unique<AggregatingStep>(
            partial_input_header,
            std::move(inner_params),
            /*grouping_sets_params=*/GroupingSetsParamsList{},
            /*final=*/false,
            agg_step->getMaxBlockSize(),
            agg_step->getMaxBlockSizeForAggregationInOrder(),
            /*merge_threads=*/0,
            /*temporary_data_merge_threads=*/0,
            /*storage_has_evenly_distributed_read=*/false,
            /*group_by_use_nulls=*/false,
            /*sort_description_for_merging=*/SortDescription{},
            /*group_by_sort_description=*/SortDescription{},
            /*should_produce_results_in_order_of_bucket_number=*/false,
            /*memory_bound_merging=*/false,
            /*explicit_sorting_required_for_aggregation_in_order=*/false);
        partial_step_ptr->setStepDescription(fmt::format("EagerPartial: {}", agg_step->getStepDescription()), 200);

        const auto agg_output_header = partial_step_ptr->getOutputHeader();

        /// If agg arguments are computed, place an ExpressionStep between
        /// the raw table and the partial agg.
        GroupExpression::Input partial_input;
        if (extracted_dag)
        {
            auto expr_step = std::make_unique<ExpressionStep>(
                join_step->getInputHeaders()[push_side], std::move(*extracted_dag));
            expr_step->setStepDescription("EagerExpr");
            GroupExpressionPtr expr_group_expr = std::make_shared<GroupExpression>(std::move(expr_step));
            expr_group_expr->inputs = {join_expr_ptr->inputs[push_side]};
            GroupId expr_group_id = memo.addGroup(expr_group_expr);
            partial_input = {expr_group_id, {}};
        }
        else
        {
            partial_input = join_expr_ptr->inputs[push_side];
        }

        GroupExpressionPtr partial_expr = std::make_shared<GroupExpression>(std::move(partial_step_ptr));
        partial_expr->inputs = {partial_input};
        GroupId partial_group_id = memo.addGroup(partial_expr);

        /// Build the target join with the aggregated side's header replaced.
        size_t other_side = 1 - push_side;
        auto agg_side_header = std::make_shared<Block>(*agg_output_header);
        const auto & other_side_header = join_step->getInputHeaders()[other_side];

        SharedHeader new_left_header = (push_side == 0) ? SharedHeader(agg_side_header) : other_side_header;
        SharedHeader new_right_header = (push_side == 0) ? other_side_header : SharedHeader(agg_side_header);

        auto new_join_step_ptr = reconstructJoin(*join_step, new_left_header, new_right_header, "EagerJoin: ");
        if (!new_join_step_ptr)
        {
            LOG_TEST(getLogger("EagerAgg"), "skip: target join reconstruction failed");
            continue;
        }

        auto target_header = new_join_step_ptr->getOutputHeader();

        GroupExpressionPtr new_join_expr = std::make_shared<GroupExpression>(std::move(new_join_step_ptr));
        if (push_side == 0)
            new_join_expr->inputs = {{partial_group_id, {}}, join_expr_ptr->inputs[1]};
        else
            new_join_expr->inputs = {join_expr_ptr->inputs[0], {partial_group_id, {}}};
        GroupId target_group_id = memo.addGroup(new_join_expr);

        /// Reconstruct intermediate joins in the chain (bottom-up).
        auto chain_result = reconstructChain(chain, target_group_id, target_header, memo);
        if (!chain_result)
        {
            LOG_TEST(getLogger("EagerAgg"), "skip: chain reconstruction failed");
            continue;
        }
        auto [final_group_id, final_header] = *chain_result;

        /// Validate that the final header has all GROUP BY keys and aggregate
        /// state columns needed by the merge step.
        bool merge_valid = true;
        for (const auto & key : agg_step->getParams().keys)
            if (!final_header->has(key))
                merge_valid = false;
        for (const auto & agg_desc : agg_step->getParams().aggregates)
            if (!final_header->has(agg_desc.column_name))
                merge_valid = false;
        if (!merge_valid)
        {
            LOG_TEST(getLogger("EagerAgg"), "skip: merge header missing columns");
            continue;
        }

        /// MergingAggregated on top merges partial states with the full key set.
        auto merge_params = agg_step->getParams();
        merge_params.only_merge = true;
        auto merge_step_ptr = std::make_unique<MergingAggregatedStep>(
            final_header,
            std::move(merge_params),
            agg_step->getGroupingSetsParamsList(),
            /*final_=*/true,
            /*memory_efficient_aggregation_=*/false,
            /*memory_efficient_merge_threads_=*/0,
            agg_step->shouldProduceResultsInBucketOrder(),
            agg_step->getMaxBlockSize(),
            agg_step->getMaxBlockSizeForAggregationInOrder(),
            agg_step->usingMemoryBoundMerging());
        merge_step_ptr->setStepDescription(fmt::format("EagerMerge: {}", agg_step->getStepDescription()), 200);

        GroupExpressionPtr merge_expr = std::make_shared<GroupExpression>(std::move(merge_step_ptr));
        merge_expr->inputs = {{final_group_id, {}}};
        merge_expr->setApplied(*this, {});
        memo.getGroup(expression->group_id)->addLogicalExpression(merge_expr);
        result.push_back(merge_expr);
    }

    return result;
}

OptimizationRulePtr createEagerAggregation() { return std::make_shared<EagerAggregation>(); }

}
