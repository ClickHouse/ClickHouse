#include <DataTypes/getLeastSupertype.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

/// Produces all applicable hash join implementations in a single rule:
///   - Local join: both inputs gathered to one node (always applicable)
///   - Broadcast join: left partitioned any way, right replicated (only when node_count > 1)
///   - Partitioned (shuffle) join: both inputs shuffled by join keys (only when node_count > 1
///     and the join has equi-join predicates)
///
/// When node_count == 1 all three strategies produce the same plan, so only the local join
/// is emitted to avoid redundant identical alternatives in the memo.
class HashJoinImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "HashJoin"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool HashJoinImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep()) != nullptr &&
        expression->strategy == nullptr;
}

std::vector<GroupExpressionPtr> HashJoinImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    const auto * join_step = typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep());
    chassert(join_step);
    chassert(expression->inputs.size() == 2);

    const size_t cluster_node_count = memo.getClusterNodeCount();
    const auto candidate_node_counts = getCandidateNodeCounts(cluster_node_count);

    std::vector<GroupExpressionPtr> result;

    /// Strategy 1: Local join — both inputs gathered to a single node.
    /// Always applicable; when cluster has only 1 node it is also the only strategy
    /// because all distributed strategies produce the same plan on a single-node cluster.
    {
        GroupExpressionPtr local_join = std::make_shared<GroupExpression>(*expression);
        local_join->strategy = std::make_shared<LocalJoinStrategy>();

        DistributionDescription single_node;     /// node_count=1, not replicated (default)
        local_join->inputs[0].required_properties.distribution = single_node;
        local_join->inputs[1].required_properties.distribution = single_node;
        local_join->properties.distribution = single_node;

        local_join->setApplied(*this, required_properties);
        memo.getGroup(expression->group_id)->addPhysicalExpression(local_join);
        result.push_back(local_join);
    }

    /// For a single-node cluster all distributed strategies are identical to local join — skip them.
    if (candidate_node_counts.empty())
        return result;

    /// Pre-compute equi-join key pairs for shuffle strategies (shared across all node counts).
    /// `hash_type_name` is the least supertype both sides are cast to before hashing when
    /// the raw key types differ (otherwise the sides hash to different buckets); empty when
    /// they agree. Keys with no common supertype are skipped.
    struct JoinKeyPair { String left; String right; String hash_type_name; String raw_type_name; };
    std::vector<JoinKeyPair> equi_keys;
    bool needs_hash_cast = false;
    if (!join_step->getJoinOperator().expression.empty())
    {
        for (const auto & predicate : join_step->getJoinOperator().expression)
        {
            auto [op, left_node, right_node] = predicate.asBinaryPredicate();
            if (op != JoinConditionOperator::Equals)
                continue;

            if (left_node.fromRight() && right_node.fromLeft())
                std::swap(left_node, right_node);
            else if (!left_node.fromLeft() || !right_node.fromRight())
                continue;

            String hash_type_name;
            DataTypePtr left_type = left_node.getType();
            DataTypePtr right_type = right_node.getType();
            if (!left_type->equals(*right_type))
            {
                DataTypePtr common_type = tryGetLeastSupertype(DataTypes{left_type, right_type});
                if (!common_type)
                    continue;
                hash_type_name = common_type->getName();
                needs_hash_cast = true;
            }

            equi_keys.push_back({left_node.getColumnName(), right_node.getColumnName(), std::move(hash_type_name), left_type->getName()});
        }
    }

    /// When any key needs a cast, the cast type must be pinned for every shuffle key so
    /// that the per-key type lists on both sides stay aligned with the key order.
    auto hash_type_for = [&](const JoinKeyPair & key) -> const String &
    {
        return key.hash_type_name.empty() ? key.raw_type_name : key.hash_type_name;
    };

    /// Broadcast replicates the right side, so it is only safe when every output row is
    /// driven by the partitioned left side: RIGHT and FULL emit unmatched right-side rows
    /// on every node, and PASTE pairs rows by position. JoinCommutativity can turn
    /// RIGHT Semi/Anti/Any into LEFT, but not RIGHT ALL or FULL.
    const auto join_kind = join_step->getJoinOperator().kind;
    const bool broadcast_unsafe
        = !(join_kind == JoinKind::Inner
            || join_kind == JoinKind::Left
            || join_kind == JoinKind::Cross
            || join_kind == JoinKind::Comma);

    /// Enumerate distributed strategies at each candidate node count.
    for (size_t candidate_node_count : candidate_node_counts)
    {
        /// Strategy 2: Broadcast join — left input partitioned any way across N nodes,
        /// right input replicated to all N nodes.
        /// Skip when the replicated (right) side can produce output rows —
        /// replicating it causes duplicate rows across nodes.
        if (!broadcast_unsafe)
        {
            /// Left input: partitioned across N nodes (any column set is acceptable)
            DistributionDescription left_dist;
            left_dist.node_count = candidate_node_count;

            /// Right input: replicated to all N nodes
            DistributionDescription right_dist;
            right_dist.node_count = candidate_node_count;
            right_dist.is_replicated = true;

            GroupExpressionPtr broadcast_join = std::make_shared<GroupExpression>(*expression);
            broadcast_join->strategy = std::make_shared<BroadcastJoinStrategy>();
            broadcast_join->inputs[0].required_properties.distribution = left_dist;
            broadcast_join->inputs[1].required_properties.distribution = right_dist;
            /// Output inherits the left input's partitioning (any N-node partitioned distribution)
            broadcast_join->properties.distribution = left_dist;

            broadcast_join->setApplied(*this, required_properties);
            memo.getGroup(expression->group_id)->addPhysicalExpression(broadcast_join);
            result.push_back(broadcast_join);
        }

        /// Strategy 3: Partitioned (shuffle) join — both inputs shuffled by join key columns.
        /// Only applicable when the join has equi-join predicates.
        if (!equi_keys.empty())
        {
            DistributionDescription left_dist;
            left_dist.node_count = candidate_node_count;

            DistributionDescription right_dist;
            right_dist.node_count = candidate_node_count;

            DistributionDescription output_dist;
            output_dist.node_count = candidate_node_count;

            auto add_key = [&](const JoinKeyPair & key)
            {
                left_dist.columns.push_back({key.left});
                right_dist.columns.push_back({key.right});
                output_dist.columns.push_back({key.left, key.right});
                if (needs_hash_cast)
                {
                    String type_name = hash_type_for(key);
                    left_dist.hash_type_names.push_back(type_name);
                    right_dist.hash_type_names.push_back(type_name);
                    output_dist.hash_type_names.push_back(type_name);
                }
            };
            auto clear_keys = [&]()
            {
                left_dist.columns.clear();
                right_dist.columns.clear();
                output_dist.columns.clear();
                left_dist.hash_type_names.clear();
                right_dist.hash_type_names.clear();
                output_dist.hash_type_names.clear();
            };

            /// If the parent requires specific distribution columns, try to match them to join
            /// keys so the join output directly satisfies the parent's distribution requirement.
            /// Fall back to all equi-join keys if not all required columns can be matched.
            /// Note: with pinned hash types the parent requirement (no types) never matches,
            /// so this colocation shortcut applies only to equal-type keys.
            if (!required_properties.distribution.columns.empty())
            {
                bool all_matched = true;
                for (const auto & required_col_set : required_properties.distribution.columns)
                {
                    bool found = false;
                    for (const auto & key : equi_keys)
                    {
                        if (required_col_set.contains(key.left) || required_col_set.contains(key.right))
                        {
                            add_key(key);
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        all_matched = false;
                        break;
                    }
                }

                if (!all_matched)
                {
                    /// Required columns cannot all be matched to join keys — use all equi-join keys.
                    clear_keys();
                    for (const auto & key : equi_keys)
                        add_key(key);
                }
            }
            else
            {
                for (const auto & key : equi_keys)
                    add_key(key);
            }

            GroupExpressionPtr partitioned_join = std::make_shared<GroupExpression>(*expression);
            partitioned_join->strategy = std::make_shared<ShuffleJoinStrategy>();
            partitioned_join->inputs[0].required_properties.distribution = left_dist;
            partitioned_join->inputs[1].required_properties.distribution = right_dist;
            partitioned_join->properties.distribution = output_dist;

            partitioned_join->setApplied(*this, required_properties);
            memo.getGroup(expression->group_id)->addPhysicalExpression(partitioned_join);
            result.push_back(partitioned_join);
        }

        /// Strategy 3b: Single-key shuffle alternatives.
        /// For joins with 2+ equi-join keys, generate a shuffle alternative for EACH individual
        /// key pair. This lets the cost model pick a single-key shuffle when the input is already
        /// distributed by that key, avoiding unnecessary re-shuffles.
        /// Correctness: hash join on (A=A', B=B') shuffled by only A/A' is correct because
        /// matching pairs where A=A' are co-located; B=B' is checked locally in the hash table.
        if (equi_keys.size() >= 2)
        {
            for (const auto & key : equi_keys)
            {
                DistributionDescription single_left_dist;
                single_left_dist.node_count = candidate_node_count;
                single_left_dist.columns.push_back({key.left});

                DistributionDescription single_right_dist;
                single_right_dist.node_count = candidate_node_count;
                single_right_dist.columns.push_back({key.right});

                DistributionDescription single_output_dist;
                single_output_dist.node_count = candidate_node_count;
                single_output_dist.columns.push_back({key.left, key.right});

                if (!key.hash_type_name.empty())
                {
                    single_left_dist.hash_type_names.push_back(key.hash_type_name);
                    single_right_dist.hash_type_names.push_back(key.hash_type_name);
                    single_output_dist.hash_type_names.push_back(key.hash_type_name);
                }

                GroupExpressionPtr single_key_join = std::make_shared<GroupExpression>(*expression);
                single_key_join->strategy = std::make_shared<ShuffleJoinStrategy>();
                single_key_join->description_suffix = fmt::format("(by {})", key.left);
                single_key_join->inputs[0].required_properties.distribution = single_left_dist;
                single_key_join->inputs[1].required_properties.distribution = single_right_dist;
                single_key_join->properties.distribution = single_output_dist;

                single_key_join->setApplied(*this, required_properties);
                memo.getGroup(expression->group_id)->addPhysicalExpression(single_key_join);
                result.push_back(single_key_join);
            }
        }
    }

    return result;
}

OptimizationRulePtr createHashJoinImplementation();
OptimizationRulePtr createHashJoinImplementation() { return std::make_shared<HashJoinImplementation>(); }

}
