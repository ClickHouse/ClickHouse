#include <memory>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Settings.h>
#include <Core/Block.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <Common/logger_useful.h>
#include <DataTypes/getLeastSupertype.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

const ActionsDAG::Node & createRuntimeFilterCondition(
    ActionsDAG & actions_dag,
    const String & filter_name,
    const ColumnWithTypeAndName & key_column,
    const DataTypePtr & filter_element_type)
{
    const auto & filter_name_node = actions_dag.addColumn(
        ColumnWithTypeAndName(
            DataTypeString().createColumnConst(0, filter_name),
            std::make_shared<DataTypeString>(),
            filter_name));

    const auto & key_column_node = actions_dag.findInOutputs(key_column.name);
    const auto * filter_argument = &key_column_node;

    /// Cast to the type of filter element if needed
    if (!key_column.type->equals(*filter_element_type))
        filter_argument = &actions_dag.addCast(key_column_node, filter_element_type, {}, nullptr);

    auto filter_function = FunctionFactory::instance().get("__applyFilter", /*query_context*/nullptr);
    const auto & condition = actions_dag.addFunction(filter_function, {&filter_name_node, filter_argument}, {});

    return condition;
}

static bool supportsRuntimeFilter(JoinAlgorithm join_algorithm)
{
    /// Runtime filter can only be applied to join algorithms that first read the right side and only after that read the left side.
    return
        join_algorithm == JoinAlgorithm::HASH ||
        join_algorithm == JoinAlgorithm::PARALLEL_HASH ||
        join_algorithm == JoinAlgorithm::GRACE_HASH;
}

namespace
{

const ReadFromMergeTree * getMergeTreeStep(QueryPlan::Node * node)
{
    while (node)
    {
        if (const auto * read = typeid_cast<const ReadFromMergeTree *>(node->step.get()))
            return read;

        if (node->children.size() != 1)
            return nullptr;

        node = node->children.front();
    }
    return nullptr;
}

}

struct JoinKeyStats
{
    UInt64 ndv;
    UInt64 total_rows;
};

/**
 * Retrieves statistics (NDV and Total Rows) for a specific join key on the build side.
 *
 * This function performs a "best effort" estimation by combining three sources of truth:
 * 1. Plan Structure: Checks for explicit `LimitStep` nodes (e.g., subqueries with LIMIT).
 * 2. Optimizer Estimates: Uses `ConditionSelectivityEstimator` which accounts for WHERE clauses.
 * 3. Storage Metadata: Falls back to `MergeTree` data parts if the optimizer is uninitialized.
 */
static std::optional<JoinKeyStats> getJoinKeyStats(
    QueryPlan::Node * build_filter_node,
    const String & key_column_name)
{
    /// 1. Inspect the Query Plan for LIMIT constraints.
    /// If the build side is a subquery like (SELECT * FROM table LIMIT 10), the cardinality 'n'
    /// cannot exceed 10, regardless of how large the underlying table is.
    std::optional<size_t> limit_value;
    QueryPlan::Node * curr = build_filter_node;
    while (curr)
    {
        if (const auto * limit = typeid_cast<const LimitStep *>(curr->step.get()))
        {
            limit_value = limit->getLimit();
            break;
        }
        /// Only traverse down linear chains (single child) to find the limit.
        if (curr->children.size() != 1)
            break;
        curr = curr->children.front();
    }

    const auto * merge_tree_step = getMergeTreeStep(build_filter_node);
    if (!merge_tree_step)
        return std::nullopt;

    /// Helper to strip table aliases (e.g., "__table1.id" -> "id") so we can look up
    /// the physical column statistics in the storage engine.
    auto stripAlias = [](const String & name)
    {
        size_t dot_pos = name.find_last_of('.');
        return (dot_pos == String::npos) ? name : name.substr(dot_pos + 1);
    };

    String physical_name = stripAlias(key_column_name);
    auto estimator = merge_tree_step->getConditionSelectivityEstimator(Names{physical_name});
    if (!estimator)
        return std::nullopt;

    auto profile = estimator->estimateRelationProfile();

    /// --- Determining 'n' (Estimated Number of Distinct Values) ---

    /// Priority 1: Trust the Optimizer's row estimate first.
    /// This value is usually preferred because it accounts for filter selectivity (WHERE clauses).
    UInt64 n = profile.rows;

    /// Apply the hard LIMIT from the plan if it's smaller than the estimated rows.
    if (limit_value && n > *limit_value)
    {
        n = *limit_value;
    }

    /// Priority 2: Refine using specific Column Statistics (NDV).
    /// If the column has hyperloglog/uniq sketches, this is more accurate than raw row counts.
    auto it = profile.column_stats.find(physical_name);
    if (it != profile.column_stats.end() && it->second.num_distinct_values > 0)
    {
        n = std::min(n, it->second.num_distinct_values);
    }

    /// Priority 3: Storage Fallback (The Safety Net).
    /// If 'n' is 0, it means the estimator is uninitialized (common with new tables or missing stats).
    /// Should fallback to the raw storage count, BUT re-apply the LIMIT check should also be done.
    /// This prevents a "LIMIT 10" query on a 1M row table from being treated as 1M rows.
    if (n == 0)
    {
        n = merge_tree_step->getParts().getRowsCountAllParts();
        if (limit_value && n > *limit_value)
        {
            n = *limit_value;
        }
    }

    /// Calculate the final total rows estimate to return in the struct.
    UInt64 final_total_rows = profile.rows;
    if (limit_value && final_total_rows > *limit_value)
        final_total_rows = *limit_value;

    return JoinKeyStats{.ndv = n, .total_rows = final_total_rows};
}

/**
 * Statically estimates the saturation of the Bloom filter to decide if it should be skipped.
 *
 * Rationale:
 * A Bloom filter becomes ineffective when it is too saturated (too many bits are set to 1).
 * If the saturation is high, the False Positive Probability (FPP) approaches 1.0, meaning
 * the filter will pass almost every row from the probe side. In such cases, building and
 * checking the filter is pure overhead (hashing cost + cache misses) with zero selectivity gain.
 *
 * Math:
 * We estimate the expected fraction of bits set to 1 (saturation) using the standard approximation:
 *
 * P(saturation) = 1 - exp(-k * n / m)
 *
 * Where:
 * n = Estimated Number of Distinct Values (NDV) from the build side statistics.
 * m = Total size of the Bloom filter in bits (join_runtime_bloom_filter_bytes * 8).
 * k = Number of hash functions (join_runtime_bloom_filter_hash_functions).
 *
 * Alignment with Runtime:
 * This static check aligns with the dynamic runtime check controlled by
 * `join_runtime_bloom_filter_max_ratio_of_set_bits`. Both checks measure bit saturation.
 * We use saturation here (instead of the theoretical FPR) to ensure that if the planner
 * decides to build a filter, the runtime won't immediately discard it for being "too full".
 */
static bool shouldDisableRuntimeFilter(
    const std::optional<JoinKeyStats> & build_stats,
    const QueryPlanOptimizationSettings & optimization_settings,
    size_t build_side_row_count = 0) /// Fallback row count from the plan step if stats are missing.
{
    // If the threshold is 1.0 (or higher), the user has explicitly disabled this optimization.
    if (optimization_settings.join_runtime_filter_build_saturation_threshold >= 1.0)
        return false;

    // Determine 'n' (NDV).
    // Priority:
    // 1. Specific column NDV from statistics (most accurate).
    // 2. Fallback to total row count if NDV is unknown/missing.
    double n = 0;
    if (build_stats && build_stats->ndv > 0)
        n = static_cast<double>(build_stats->ndv);
    else if (build_side_row_count > 0)
        n = static_cast<double>(build_side_row_count);

    // If still have no estimate for n, default to ENABLED (return false) to be safe.
    if (n == 0)
        return false;

    double k = static_cast<double>(optimization_settings.join_runtime_bloom_filter_hash_functions);
    double m = static_cast<double>(optimization_settings.join_runtime_bloom_filter_bytes * 8);

    if (m == 0.0)
        return true; // Filter has no space (0 bytes), so it is effectively fully saturated.

    // Calculate expected saturation: P = 1 - e^(-kn/m)
    double p = 1.0 - std::exp(-k * n / m);

    LOG_DEBUG(getLogger("joinRuntimeFilter"),
        "Saturation Check: n={}, m={}, k={}, p={:.4f}, threshold={:.2f}",
        n, m, k, p, optimization_settings.join_runtime_filter_build_saturation_threshold);

    return p >= optimization_settings.join_runtime_filter_build_saturation_threshold;
}

bool tryAddJoinRuntimeFilter(QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Is this a join step?
    auto * join_step = typeid_cast<JoinStepLogical *>(node.step.get());
    if (!join_step)
        return false;

    /// Joining two sources?
    if (node.children.size() != 2)
        return false;

    /// If right table is already filled and will be used for lookups directly (e.g. StorageJoin) then runtime filter cannot be constructed
    if (typeid_cast<JoinStepLogicalLookup *>(node.children[1]->step.get()))
        return false;

    /// There are cases when either or both joined tables are replaced with const data at optimization time, e.g. when they are (SELECT 1 AS col).
    /// In such cases a header can be empty and all the const data is in the ActionsDAG in the Join step. There is no need (and no way) to build
    /// runtime filter in this scenario.
    if (node.children[0]->step->getOutputHeader()->empty() ||
        node.children[1]->step->getOutputHeader()->empty())
        return false;

    /// Check if join can do runtime filtering on left table
    const auto & join_operator = join_step->getJoinOperator();
    auto & join_algorithms = join_step->getJoinSettings().join_algorithms;
    const bool can_use_runtime_filter =
        (
            (join_operator.kind == JoinKind::Inner && (join_operator.strictness == JoinStrictness::All || join_operator.strictness == JoinStrictness::Any))
            || ((join_operator.kind == JoinKind::Left || join_operator.kind == JoinKind::Right) && join_operator.strictness == JoinStrictness::Semi)
            || ((join_operator.kind == JoinKind::Left || join_operator.kind == JoinKind::Right) && join_operator.strictness == JoinStrictness::Anti)
            || (join_operator.kind == JoinKind::Right && (join_operator.strictness == JoinStrictness::All || join_operator.strictness == JoinStrictness::Any))
        ) &&
        (join_operator.locality == JoinLocality::Unspecified || join_operator.locality == JoinLocality::Local) &&
        std::find_if(join_algorithms.begin(), join_algorithms.end(), supportsRuntimeFilter) != join_algorithms.end();

    if (!can_use_runtime_filter)
        return false;

    /// Sometimes cross join can be represented by inner join without expressions
    if (join_operator.expression.empty())
        return false;

    /// In the case of LEFT ANTI JOIN we need to add a filter that filters out rows
    /// that would have matches in the right table. This means we need to add something like NOT IN filter.
    const bool check_left_does_not_contain = (join_operator.kind == JoinKind::Left && join_operator.strictness == JoinStrictness::Anti);

    QueryPlan::Node * apply_filter_node = node.children[0];
    QueryPlan::Node * build_filter_node = node.children[1];

    ColumnsWithTypeAndName join_keys_probe_side;
    ColumnsWithTypeAndName join_keys_build_side;

    /// Check that there are only equality predicates
    for (const auto & condition : join_operator.expression)
    {
        auto [predicate_op, lhs, rhs] = condition.asBinaryPredicate();
        if (predicate_op != JoinConditionOperator::Equals)
            return false;

        /// For the case of ANTI JOIN (more specifically for check_left_does_not_contain) the hash table in JOIN can have extra rows that can be filtered
        /// out by post-condition. In this case we cannot build set of keys for runtime filter from right-side rows because the set will contain more rows
        /// and thus 'NOT IN set' operation will filter out rows that should not be filtered.
        /// So in this case we check that all JOIN predicates are equality between expr from left columns and expr from right columns, but not something
        /// like "func(left, right) = const"
        if (check_left_does_not_contain &&
            !(lhs.fromLeft() && rhs.fromRight()) &&
            !(lhs.fromRight() && rhs.fromLeft()))
        {
            return false;
        }
    }

    /// Save original number of expression before extracting key DAGs
    const auto total_join_on_predicates_count = join_operator.expression.size();

    {
        /// Extract expressions for calculating join on keys
        auto key_dags = join_step->preCalculateKeys(apply_filter_node->step->getOutputHeader(), build_filter_node->step->getOutputHeader());
        if (key_dags)
        {
            auto get_node_column_with_type_and_name = [](const auto * e) { return ColumnWithTypeAndName(e->result_type, e->result_name); };
            join_keys_probe_side = std::ranges::to<ColumnsWithTypeAndName>(key_dags->first.keys | std::views::transform(get_node_column_with_type_and_name));
            join_keys_build_side = std::ranges::to<ColumnsWithTypeAndName>(key_dags->second.keys | std::views::transform(get_node_column_with_type_and_name));
            if (!isPassthroughActions(key_dags->first.actions_dag))
                makeExpressionNodeOnTopOf(*apply_filter_node, std::move(key_dags->first.actions_dag), nodes, makeDescription("Calculate left join keys"));
            if (!isPassthroughActions(key_dags->second.actions_dag))
                makeExpressionNodeOnTopOf(*build_filter_node, std::move(key_dags->second.actions_dag), nodes, makeDescription("Calculate right join keys"));
        }
    }

    // Skip runtime filters if there are no join keys
    if (join_keys_build_side.empty())
    {
        return false;
    }

    /// When negation will be use for the set of rows in filter, double check that all original predicates were transformed into equality predicates
    /// between left and right side
    if (check_left_does_not_contain &&
        (join_keys_build_side.size() != total_join_on_predicates_count ||
        join_keys_probe_side.size() != total_join_on_predicates_count))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Original predicate count {} does not match the number of JOIN ON keys, left: {}, right: {}",
            total_join_on_predicates_count, join_keys_probe_side.size(), join_keys_build_side.size());
    }

    const String filter_name_prefix = fmt::format("{}_runtime_filter_{}", check_left_does_not_contain ? "_exclusion_" : "", thread_local_rng());

    {
        /// Pass all columns on probe side
        ActionsDAG filter_dag(apply_filter_node->step->getOutputHeader()->getColumnsWithTypeAndName(), false);

        String filter_column_name;
        ActionsDAG::NodeRawConstPtrs all_filter_conditions;
        size_t build_side_row_count = 0;
        std::optional<size_t> top_limit;
        {
            QueryPlan::Node * lnode = build_filter_node;
            while (lnode)
            {
                if (const auto * limit = typeid_cast<const LimitStep *>(lnode->step.get()))
                {
                    top_limit = limit->getLimit();
                    break;
                }
                if (lnode->children.size() != 1) break;
                lnode = lnode->children.front();
            }
        }

        if (const auto * merge_tree_step = getMergeTreeStep(build_filter_node))
        {
            build_side_row_count = merge_tree_step->getParts().getRowsCountAllParts();
            if (top_limit && build_side_row_count > *top_limit)
                build_side_row_count = *top_limit;
        }

        for (size_t i = 0; i < join_keys_build_side.size(); ++i)
        {
            const String filter_name = filter_name_prefix + "_" + toString(i);
            const auto & join_key_build_side = join_keys_build_side[i];
            const auto & join_key_probe_side = join_keys_probe_side[i];
            const auto build_key_name = join_key_build_side.name;

            auto build_stats = getJoinKeyStats(build_filter_node, build_key_name);

            // If stats exist, perform the check
            if (build_stats)
            {
                if (shouldDisableRuntimeFilter(build_stats, optimization_settings, build_stats->ndv))
                {
                    LOG_DEBUG(getLogger("joinRuntimeFilter"), "Saturation high (n={}), skipping key {}", build_stats->ndv, build_key_name);
                    continue;
                }
            }

            // Determine effective n
            // Priority 1: NDV from column stats (if > 0)
            // Priority 2: Total rows from the plan step (fallback)
            size_t effective_n = build_side_row_count;
            if (build_stats && build_stats->ndv > 0)
                effective_n = static_cast<size_t>(build_stats->ndv);

            if (shouldDisableRuntimeFilter(build_stats, optimization_settings, effective_n))
            {
                LOG_DEBUG(getLogger("joinRuntimeFilter"),
                    "Saturation Check: Disabling filter for '{}' (n={})",
                    build_key_name, effective_n);
                continue;
            }

            DataTypePtr common_type;
            if (!join_key_build_side.type->equals(*join_key_probe_side.type))
            {
                try
                {
                    common_type = getLeastSupertype(DataTypes{join_key_build_side.type, join_key_probe_side.type});
                }
                catch (Exception & ex)
                {
                    ex.addMessage("JOIN cannot infer common type in ON section for keys. Left key '{}' type {}. Right key '{}' type {}",
                        join_key_probe_side.name, join_key_probe_side.type->getName(),
                        join_key_build_side.name, join_key_build_side.type->getName());
                    throw;
                }
            }
            else
            {
                common_type = join_key_build_side.type;
            }

            LOG_TRACE(getLogger("joinRuntimeFilter"), "Runtime filter '{}' will be built from `{}` and applied to `{}`",
                filter_name, join_key_build_side.name, join_key_probe_side.name);

            /// Add filter lookup to the probe subtree
            all_filter_conditions.push_back(&createRuntimeFilterCondition(filter_dag, filter_name, join_key_probe_side, common_type));

            /// Add building filter to the build subtree of join
            {
                QueryPlan::Node * new_build_filter_node = &nodes.emplace_back();
                new_build_filter_node->step = std::make_unique<BuildRuntimeFilterStep>(
                    build_filter_node->step->getOutputHeader(),
                    join_key_build_side.name,
                    common_type,
                    filter_name,
                    optimization_settings.join_runtime_filter_exact_values_limit,
                    optimization_settings.join_runtime_bloom_filter_bytes,
                    optimization_settings.join_runtime_bloom_filter_hash_functions,
                    optimization_settings.join_runtime_filter_pass_ratio_threshold_for_disabling,
                    optimization_settings.join_runtime_filter_blocks_to_skip_before_reenabling,
                    optimization_settings.join_runtime_bloom_filter_max_ratio_of_set_bits,
                    /*allow_to_use_not_exact_filter_=*/!check_left_does_not_contain);

                new_build_filter_node->step->setStepDescription(fmt::format("Build runtime join filter on {}", join_key_build_side.name), 200);
                new_build_filter_node->children = {build_filter_node};
                build_filter_node = new_build_filter_node;
            }
        }

        if (all_filter_conditions.empty())
        {
            LOG_DEBUG(getLogger("joinRuntimeFilter"),
                "All runtime filters disabled due to high saturation. Skipping FilterStep creation.");
            return false;
        }

        if (all_filter_conditions.size() == 1)
        {
            filter_dag.addOrReplaceInOutputs(*all_filter_conditions.front());
            filter_column_name = all_filter_conditions.front()->result_name;
        }
        else if (all_filter_conditions.size() > 1)
        {
            FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

            const auto & combined_filter_condition = filter_dag.addFunction(func_builder_and, std::move(all_filter_conditions), {});
            filter_dag.addOrReplaceInOutputs(combined_filter_condition);
            filter_column_name = combined_filter_condition.result_name;
        }

        QueryPlan::Node * new_apply_filter_node = &nodes.emplace_back();
        new_apply_filter_node->step = std::make_unique<FilterStep>(apply_filter_node->step->getOutputHeader(), std::move(filter_dag), filter_column_name, true);
        new_apply_filter_node->step->setStepDescription("Apply runtime join filter");
        new_apply_filter_node->children = {apply_filter_node};

        apply_filter_node = new_apply_filter_node;
    }

    node.children = {apply_filter_node, build_filter_node};

    /// Remove algorithms that are not compatible with runtime filters
    std::erase_if(join_algorithms, [](auto join_algorithm) { return !supportsRuntimeFilter(join_algorithm); });

    return true;
}

}

}
