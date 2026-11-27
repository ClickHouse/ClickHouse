#include <memory>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Settings.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/tuple.h>
#include <DataTypes/getLeastSupertype.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

/// For ANTI JOIN exclusion filters, rows with any NULL key can never match in the join (since NULL = NULL is false in SQL)
/// and must always pass the runtime filter. This wraps the filter condition with OR isNull(key1) OR isNull(key2) OR ...
const ActionsDAG::Node * addNullBypassForAntiJoin(
    ActionsDAG & dag,
    const ActionsDAG::Node * filter_condition,
    const ColumnsWithTypeAndName & keys)
{
    ActionsDAG::NodeRawConstPtrs or_conditions;
    or_conditions.push_back(filter_condition);

    auto is_null_func = FunctionFactory::instance().get("isNull", Context::getGlobalContextInstance());
    for (const auto & key : keys)
    {
        if (isNullableOrLowCardinalityNullable(key.type))
        {
            const auto * key_node = &dag.findInOutputs(key.name);
            or_conditions.push_back(&dag.addFunction(is_null_func, {key_node}, {}));
        }
    }

    if (or_conditions.size() == 1)
        return filter_condition;

    FunctionOverloadResolverPtr or_func = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());
    return &dag.addFunction(or_func, std::move(or_conditions), {});
}

/// Build a `tuple(key1, key2, ...)` node in the given DAG, casting each key to the corresponding common type if needed.
const ActionsDAG::Node & addTupleOfKeys(
    ActionsDAG & dag,
    const ColumnsWithTypeAndName & keys,
    const DataTypes & common_types,
    const FunctionOverloadResolverPtr & tuple_func)
{
    ActionsDAG::NodeRawConstPtrs key_nodes;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        const auto * key_node = &dag.findInOutputs(keys[i].name);
        if (!keys[i].type->equals(*common_types[i]))
            key_node = &dag.addCast(*key_node, common_types[i], {}, nullptr);
        key_nodes.push_back(key_node);
    }
    return dag.addFunction(tuple_func, key_nodes, {});
}

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

    /// Compute common types for each key pair
    DataTypes common_types;
    common_types.reserve(join_keys_build_side.size());
    for (size_t i = 0; i < join_keys_build_side.size(); ++i)
    {
        const auto & join_key_build_side = join_keys_build_side[i];
        const auto & join_key_probe_side = join_keys_probe_side[i];

        if (!join_key_build_side.type->equals(*join_key_probe_side.type))
        {
            try
            {
                common_types.push_back(getLeastSupertype(DataTypes{join_key_build_side.type, join_key_probe_side.type}));
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
            common_types.push_back(join_key_build_side.type);
        }
    }

    /// For LEFT ANTI JOIN with multiple keys, per-column NOT IN filters combined with AND are incorrect:
    /// NOT_IN(a, set_a) AND NOT_IN(b, set_b) would incorrectly drop rows where one key is in its per-column set
    /// but the full tuple has no match in the right table.
    /// Instead, wrap all keys into a single Tuple and build one NOT IN filter on the tuple for exact tuple membership check.
    const bool use_tuple_filter = check_left_does_not_contain && join_keys_build_side.size() > 1;

    /// Filter that will be applied on the probe side
    ActionsDAG filter_dag(apply_filter_node->step->getOutputHeader()->getColumnsWithTypeAndName(), false);
    String filter_column_name;

    if (use_tuple_filter)
    {
        const String filter_name = filter_name_prefix + "_0";
        auto tuple_type = std::make_shared<DataTypeTuple>(common_types);
        FunctionOverloadResolverPtr tuple_func = std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());

        /// Build side: compute tuple(key1, key2, ...) and build a single runtime filter on the tuple column.
        /// The tuple column is a temporary column that must be stripped before the join step sees the right-side header.
        {
            /// Remember the original header before adding the tuple column
            auto original_build_header = build_filter_node->step->getOutputHeader();

            ActionsDAG build_tuple_dag(build_filter_node->step->getOutputHeader()->getColumnsWithTypeAndName(), false);
            const auto & tuple_node = addTupleOfKeys(build_tuple_dag, join_keys_build_side, common_types, tuple_func);
            const String tuple_column_name = tuple_node.result_name;
            build_tuple_dag.addOrReplaceInOutputs(tuple_node);

            makeExpressionNodeOnTopOf(*build_filter_node, std::move(build_tuple_dag), nodes, makeDescription("Calculate right join key tuple"));

            LOG_TRACE(getLogger("joinRuntimeFilter"), "Runtime filter '{}' will be built from tuple of right keys and applied to tuple of left keys", filter_name);

            QueryPlan::Node * new_build_filter_node = &nodes.emplace_back();
            new_build_filter_node->step = std::make_unique<BuildRuntimeFilterStep>(
                build_filter_node->step->getOutputHeader(),
                tuple_column_name,
                tuple_type,
                filter_name,
                optimization_settings.join_runtime_filter_exact_values_limit,
                optimization_settings.join_runtime_bloom_filter_bytes,
                optimization_settings.join_runtime_bloom_filter_hash_functions,
                optimization_settings.join_runtime_filter_pass_ratio_threshold_for_disabling,
                optimization_settings.join_runtime_filter_blocks_to_skip_before_reenabling,
                optimization_settings.join_runtime_bloom_filter_max_ratio_of_set_bits,
                /*allow_to_use_not_exact_filter_=*/false);
            new_build_filter_node->step->setStepDescription("Build runtime join filter on key tuple", 200);
            new_build_filter_node->children = {build_filter_node};
            build_filter_node = new_build_filter_node;

            /// Strip the temporary tuple column so the join step sees only the original columns
            ActionsDAG strip_tuple_dag(build_filter_node->step->getOutputHeader()->getColumnsWithTypeAndName(), false);
            strip_tuple_dag.removeUnusedActions(original_build_header->getNames(), /*allow_remove_inputs=*/false);
            makeExpressionNodeOnTopOf(*build_filter_node, std::move(strip_tuple_dag), nodes, makeDescription("Remove temporary tuple column"));
        }

        /// Apply side: compute tuple(key1, key2, ...) and apply the filter
        {
            const auto & tuple_node = addTupleOfKeys(filter_dag, join_keys_probe_side, common_types, tuple_func);

            /// Build __applyFilter(filter_name, tuple_node) condition directly,
            /// since the tuple node is freshly created and not yet in the DAG outputs
            const auto & filter_name_node = filter_dag.addColumn(
                ColumnWithTypeAndName(
                    DataTypeString().createColumnConst(0, filter_name),
                    std::make_shared<DataTypeString>(),
                    filter_name));
            auto filter_function = FunctionFactory::instance().get("__applyFilter", /*query_context*/nullptr);
            const auto & condition = filter_dag.addFunction(filter_function, {&filter_name_node, &tuple_node}, {});

            const auto * final_condition = addNullBypassForAntiJoin(filter_dag, &condition, join_keys_probe_side);
            filter_dag.addOrReplaceInOutputs(*final_condition);

            filter_column_name = final_condition->result_name;
        }
    }
    else
    {
        /// Standard per-column runtime filters (for INNER, SEMI, RIGHT, and single-key ANTI joins)
        ActionsDAG::NodeRawConstPtrs all_filter_conditions;
        for (size_t i = 0; i < join_keys_build_side.size(); ++i)
        {
            const String filter_name = filter_name_prefix + "_" + toString(i);

            const auto & join_key_build_side = join_keys_build_side[i];
            const auto & join_key_probe_side = join_keys_probe_side[i];
            const auto & common_type = common_types[i];

            LOG_TRACE(getLogger("joinRuntimeFilter"), "Runtime filter '{}' will be built from `{}` and applied to `{}`",
                filter_name, join_key_build_side.name, join_key_probe_side.name);

            /// Add filter lookup to the probe subtree
            const auto & filter_condition = createRuntimeFilterCondition(filter_dag, filter_name, join_key_probe_side, common_type);
            all_filter_conditions.push_back(check_left_does_not_contain
                ? addNullBypassForAntiJoin(filter_dag, &filter_condition, {join_key_probe_side})
                : &filter_condition);

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
    }

    QueryPlan::Node * new_apply_filter_node = &nodes.emplace_back();
    new_apply_filter_node->step = std::make_unique<FilterStep>(
        apply_filter_node->step->getOutputHeader(), std::move(filter_dag), filter_column_name, true);
    new_apply_filter_node->step->setStepDescription("Apply runtime join filter");
    new_apply_filter_node->children = {apply_filter_node};
    apply_filter_node = new_apply_filter_node;

    node.children = {apply_filter_node, build_filter_node};

    /// Remove algorithms that are not compatible with runtime filters
    std::erase_if(join_algorithms, [](auto join_algorithm) { return !supportsRuntimeFilter(join_algorithm); });

    return true;
}

}

}
