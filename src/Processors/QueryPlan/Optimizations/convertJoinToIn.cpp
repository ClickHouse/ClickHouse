#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Core/Block.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeSet.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/tuple.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/misc.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/CommonSubplanStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/ObjectFilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageMerge.h>

#include <unordered_set>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

struct InConversion
{
    ActionsDAG dag;
    std::shared_ptr<FutureSetFromSubquery> set;
};

struct NamePair
{
    std::string_view lhs_name;
    std::string_view rhs_name;
};

using NamePairs = std::vector<NamePair>;

static InConversion buildInConversion(
    const SharedHeader & lhs_input_header,
    const NamePairs & name_pairs,
    std::unique_ptr<QueryPlan> in_source,
    FutureSetSettings set_settings)
{
    ActionsDAG lhs_dag(lhs_input_header->getColumnsWithTypeAndName());
    std::unordered_map<std::string_view, const ActionsDAG::Node *> lhs_outputs;
    for (const auto & output : lhs_dag.getOutputs())
        lhs_outputs.emplace(output->result_name, output);

    ActionsDAG rhs_dag(in_source->getCurrentHeader()->getColumnsWithTypeAndName());
    std::unordered_map<std::string_view, const ActionsDAG::Node *> rhs_outputs;
    for (const auto & output : rhs_dag.getOutputs())
        rhs_outputs.emplace(output->result_name, output);

    rhs_dag.getOutputs().clear();

    std::vector<const ActionsDAG::Node *> left_columns;
    for (const auto & name_pair : name_pairs)
    {
        auto it = lhs_outputs.find(name_pair.lhs_name);
        if (it == lhs_outputs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find left key {} in JOIN step", name_pair.lhs_name);
        left_columns.push_back(it->second);

        auto jt = rhs_outputs.find(name_pair.rhs_name);
        if (jt == rhs_outputs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find right key {} in JOIN step", name_pair.lhs_name);
        rhs_dag.getOutputs().push_back(jt->second);
    }

    auto rhs_expression = std::make_unique<ExpressionStep>(in_source->getCurrentHeader(), std::move(rhs_dag));
    rhs_expression->setStepDescription("JOIN keys");
    in_source->addStep(std::move(rhs_expression));

    /// left parameter of IN function
    FunctionOverloadResolverPtr func_tuple_builder =
        std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());
    const ActionsDAG::Node * in_lhs_arg = left_columns.size() == 1 ?
        left_columns.front() :
        &lhs_dag.addFunction(func_tuple_builder, std::move(left_columns), {});

    auto get_random_hash = []()
    {
        auto uuid = UUIDHelpers::generateV4();
        return FutureSet::Hash(UUIDHelpers::getLowBytes(uuid), UUIDHelpers::getHighBytes(uuid));
    };

    /// right parameter of IN function
    auto future_set = std::make_shared<FutureSetFromSubquery>(
        get_random_hash(), nullptr, std::move(in_source), nullptr, nullptr, std::move(set_settings));

    ColumnConst::Ptr set_col = ColumnConst::create(ColumnSet::create(1, future_set), 0);
    /// The set is filled from the right side at execution time, so it is not a constant across queries.
    const ActionsDAG::Node * in_rhs_arg = &lhs_dag.addColumn(
        std::move(set_col), std::make_shared<DataTypeSet>(), "set column", /*is_deterministic_constant=*/false);

    /// IN function
    auto func_in = FunctionFactory::instance().get("in", nullptr);
    const auto & in_node = lhs_dag.addFunction(func_in, {in_lhs_arg, in_rhs_arg}, "");
    lhs_dag.getOutputs().insert(lhs_dag.getOutputs().begin(), &in_node);

    return {std::move(lhs_dag), std::move(future_set)};
}

static void remapNodes(ActionsDAG::NodeRawConstPtrs & keys, const ActionsDAG::NodeMapping & node_map)
{
    for (const auto *& key : keys)
    {
        if (auto it = node_map.find(key); it != node_map.end())
            key = it->second;
    }
}

static ActionsDAG cloneSubDAGWithHeader(const SharedHeader & stream_header, ActionsDAG second_dag)
{
    ActionsDAG dag(stream_header->getColumnsWithTypeAndName());

    auto outputs = second_dag.getOutputs();

    ActionsDAG::NodeMapping node_map;
    dag.mergeInplace(std::move(second_dag), node_map, true);
    remapNodes(outputs, node_map);

    dag.getOutputs() = outputs;

    return dag;
}

static bool hasCommonSubplanNodes(const QueryPlan::Node & root)
{
    std::vector<const QueryPlan::Node *> stack{&root};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (typeid_cast<const CommonSubplanStep *>(node->step.get())
            || typeid_cast<const CommonSubplanReferenceStep *>(node->step.get()))
            return true;

        stack.insert(stack.end(), node->children.begin(), node->children.end());
    }
    return false;
}

/// What the source of the set created by the conversion needs before it can run on its own.
/// Index analysis may build that set synchronously, before the outer plan materializes its CTEs
/// and builds its sets, so the source has to declare these and build them first.
struct SetSourcePrerequisites
{
    /// Unbuilt materialized CTEs read anywhere below the source, each after the CTEs its own plan reads.
    std::vector<MaterializedCTEPtr> ctes;
    std::unordered_set<const MaterializedCTE *> seen_ctes;
    /// Unbuilt sets that the right side or its keys read and do not own; they are built for the outer plan.
    PreparedSets::Subqueries sets;
    std::unordered_set<const FutureSetFromSubquery *> seen_sets;
    /// A dependency that the source cannot build itself: a `GLOBAL IN` set, a CTE that another plan has
    /// claimed but not run yet, a set whose source has moved. The new set could then only be built by
    /// the deferred build, but some readers evaluate the new filter while planning (`ReadFromMerge`
    /// selecting tables, `system.tables` filtering databases), so the join has to stay.
    bool unsatisfiable = false;

    void addCTE(MaterializedCTEPtr cte)
    {
        if (!cte || cte->isBuilt() || !seen_ctes.insert(cte.get()).second)
            return;

        /// Another plan has claimed the CTE but not run it yet: a synchronous build here could
        /// neither materialize it nor wait for it.
        if (cte->is_materialization_planned.load())
        {
            unsatisfiable = true;
            return;
        }

        /// A CTE can read other CTEs. Its plan relies on an outer gate for them, the same way as
        /// the source does, so collect them first: `addTo` puts their steps outside of this one.
        if (cte->plan && cte->plan->getRootNode())
            collectFromPlan(*cte->plan->getRootNode());

        ctes.push_back(std::move(cte));
    }

    /// Collect the CTEs read below `root`, following set sources held outside the node tree.
    void collectFromPlan(const QueryPlan::Node & root)
    {
        std::vector<const QueryPlan::Node *> stack{&root};
        std::unordered_set<const QueryPlan::Node *> visited;
        const auto add_plan = [&](const QueryPlan * plan)
        {
            if (plan && plan->getRootNode())
                stack.push_back(plan->getRootNode());
        };

        while (!stack.empty())
        {
            const auto * node = stack.back();
            stack.pop_back();

            if (!visited.insert(node).second)
                continue;

            /// `ReadFromMerge` creates its child plans when asked for them, so read the CTEs from
            /// the query tree it plans them from; its child plans leave these CTEs to the outer gate.
            if (const auto * merge = typeid_cast<const ReadFromMerge *>(node->step.get()))
            {
                for (const auto & cte : collectMaterializedCTEsFromQueryTree(merge->getQueryInfo().query_tree))
                    addCTE(cte);
            }
            else if (const auto * read = typeid_cast<const ReadFromMemoryStorageStep *>(node->step.get()))
            {
                if (const auto * storage = typeid_cast<const StorageMemory *>(read->getStorage().get()))
                    addCTE(storage->getMaterializedCTE());
            }

            /// A deferred set can itself read a CTE. Its source is held outside the node tree;
            /// a set that is already built no longer needs that source or its dependencies.
            if (const auto * creating_sets = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()))
            {
                for (const auto & set : creating_sets->getSets())
                    if (!set->get())
                        add_plan(set->getQueryPlan());
            }
            else if (const auto * local = typeid_cast<const ReadFromLocalParallelReplicaStep *>(node->step.get()))
                add_plan(local->getQueryPlan());

            stack.insert(stack.end(), node->children.begin(), node->children.end());
        }
    }

    /// Sets owned by a `DelayedCreatingSetsStep` inside the source are built by the source itself.
    std::unordered_set<const FutureSet *> owned_sets;

    /// Collect the unbuilt sets that `dag` reads and the source does not own: an `IN` subquery in an
    /// `ON` key, or an outer filter pushed into the right side. They are built for the outer plan.
    void collectFromDAG(const ActionsDAG & dag)
    {
        /// A `GLOBAL IN` set gets its external table attached by `ReadFromRemote` later, so it must not
        /// be built in place (see `VirtualColumnUtils::buildSetsForDAGExcludingGlobalIn`).
        std::unordered_set<const ActionsDAG::Node *> global_in_sets;
        for (const auto & node : dag.getNodes())
            if (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base
                && functionIsGlobalInOperator(node.function_base->getName()) && node.children.size() >= 2)
                global_in_sets.insert(node.children[1]);

        for (const auto & node : dag.getNodes())
        {
            if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
                continue;

            const auto * column_set = checkAndGetColumn<const ColumnSet>(&node.column->getDataColumn());
            if (!column_set)
                continue;

            auto future_set = column_set->getData();
            if (!future_set || future_set->get() || owned_sets.contains(future_set.get()))
                continue;

            auto subquery_set = std::dynamic_pointer_cast<FutureSetFromSubquery>(future_set);
            /// Without a source plan the set is built elsewhere and cannot be built from here.
            if (global_in_sets.contains(&node) || !subquery_set || !subquery_set->getQueryPlan())
            {
                unsatisfiable = true;
                continue;
            }

            if (seen_sets.insert(subquery_set.get()).second)
            {
                collectFromPlan(*subquery_set->getQueryPlan()->getRootNode());
                sets.push_back(std::move(subquery_set));
            }
        }
    }

    /// Collect the unbuilt sets read by the steps below `root`. Sets reach a plan only through the
    /// expressions of these steps; the sources of nested sets are plans of their own, which own theirs.
    /// These are all the steps that hold an `ActionsDAG` at this point: the physical join steps that
    /// carry expressions too are made by `optimizeJoinLogical` in the second pass, after this one.
    void collectFromSteps(const QueryPlan::Node & root)
    {
        std::vector<const ActionsDAG *> dags;
        std::vector<const QueryPlan::Node *> stack{&root};
        while (!stack.empty())
        {
            const auto * node = stack.back();
            stack.pop_back();
            const auto * step = node->step.get();

            if (const auto * creating_sets = typeid_cast<const DelayedCreatingSetsStep *>(step))
                for (const auto & set : creating_sets->getSets())
                    owned_sets.insert(set.get());
            else if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
                dags.push_back(&expression->getExpression());
            else if (const auto * filter = typeid_cast<const FilterStep *>(step))
                dags.push_back(&filter->getExpression());
            else if (const auto * join = typeid_cast<const JoinStepLogical *>(step))
                dags.push_back(&join->getActionsDAG());
            else if (const auto * having = typeid_cast<const TotalsHavingStep *>(step))
                dags.push_back(having->getActions());
            else if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(step))
                dags.push_back(array_join->getElementFilter() ? &*array_join->getElementFilter() : nullptr);
            else if (const auto * limit_range = typeid_cast<const LimitRangeStep *>(step))
                dags.push_back(&limit_range->getConditions());
            else if (const auto * object_filter = typeid_cast<const ObjectFilterStep *>(step))
                dags.push_back(&object_filter->getExpression());
            else if (const auto * source = dynamic_cast<const SourceStepWithFilterBase *>(step))
            {
                dags.push_back(source->getFilterActionsDAG().get());
                if (const auto & prewhere = source->getPrewhereInfo())
                    dags.push_back(&prewhere->prewhere_actions);
                if (const auto & row_level = source->getRowLevelFilter())
                    dags.push_back(&row_level->actions);
            }

            stack.insert(stack.end(), node->children.begin(), node->children.end());
        }

        /// After the walk, when all the sets that the source owns are known.
        for (const auto * dag : dags)
            if (dag)
                collectFromDAG(*dag);
    }

    /// Declare the prerequisites on the extracted source. Both steps stay only on a synchronous
    /// build of the new set: `makePlansForSets` strips them on the deferred path, where the outer
    /// plan's gates own the CTE and set writers.
    void addTo(QueryPlan & source, const SizeLimits & network_transfer_limits)
    {
        if (!sets.empty())
        {
            auto step = std::make_unique<DelayedCreatingSetsStep>(
                source.getCurrentHeader(), std::move(sets), network_transfer_limits, nullptr);
            step->setInplaceBuildOnly();
            step->setStepDescription("Build sets read by JOIN right side");
            source.addStep(std::move(step));
        }

        /// One step per CTE, a dependency outside of the CTEs that read it, so that its gate also
        /// holds back their writers. This is the order in which `Planner` stacks the levels.
        for (auto it = ctes.rbegin(); it != ctes.rend(); ++it)
        {
            auto step = std::make_unique<DelayedMaterializingCTEsStep>(source.getCurrentHeader(), VectorWithMemoryTracking<MaterializedCTEPtr>{*it});
            step->setStepDescription("Materialize CTE read by JOIN right side");
            source.addStep(std::move(step));
        }
        ctes.clear();
    }
};

size_t tryConvertJoinToIn(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    auto & parent = parent_node->step;

    if (parent_node->children.size() != 2)
        return 0;

    auto * join = typeid_cast<JoinStepLogical *>(parent.get());
    if (!join)
        return 0;

    /// The set created here uses `transform_null_in = false` and transfer limits, which the
    /// serialized set record does not carry; a distributed-plan worker would rebuild the set
    /// with its task settings and could get a different membership policy. Keep the join.
    if (settings.make_distributed_plan)
        return 0;

    /// Let's support only hash algorithm, because full sorting join may be more memory efficient than IN.
    const auto & join_algorithms = join->getJoinSettings().join_algorithms;
    if (!TableJoin::isEnabledAlgorithm(join_algorithms, JoinAlgorithm::HASH))
        return 0;

    const auto & join_operator = join->getJoinOperator();

    /// Let's allow Strictness::All with a wrong result for now.
    if (join_operator.strictness != JoinStrictness::Any && join_operator.strictness != JoinStrictness::All)
        return 0;

    /// TODO: support left in the future
    if (!isInner(join_operator.kind)/*&& !isLeft(join_operator.kind) && !isRight(join_operator.kind)*/)
        return 0;

    /// Do not support many condition for now.
    if (join_operator.expression.empty())
        return 0;

    /// Only equality expressions are supported.
    std::vector<std::pair<JoinActionRef, JoinActionRef>> key_pairs;
    for (const auto & predicate : join_operator.expression)
    {
        auto [op, lhs, rhs] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals)
            return 0;

        if (lhs.fromLeft() && rhs.fromRight())
            key_pairs.emplace_back(lhs, rhs);
        else if (lhs.fromRight() && rhs.fromLeft())
            key_pairs.emplace_back(rhs, lhs);
        else
            return 0;
    }

    bool build_set_from_left_part = false;

    auto join_output_actions = join->getOutputActions();
    /// Check output columns come from one side.
    if (!isInnerOrLeft(join_operator.kind) || std::ranges::any_of(join_output_actions, &JoinActionRef::fromRight))
        return 0;

    /// Check input and output type match
    if (!join->typeChangingSides().empty())
        return 0;

    /// A `CommonSubplanReferenceStep` holds a raw pointer to a node that must still hold a
    /// `CommonSubplanStep` when the second pass resolves it. The rewrite below installs new steps at
    /// both input node addresses and splices the right input into the IN set's own plan.
    if (hasCommonSubplanNodes(*parent_node->children.at(0))
        || hasCommonSubplanNodes(*parent_node->children.at(1)))
        return 0;

    // {
    //     WriteBufferFromOwnString buf;
    //     IQueryPlanStep::FormatSettings s{.out=buf, .write_header=true};
    //     join->describeActions(s);
    //     std::cerr << buf.stringView() << std::endl;
    // }

    if (build_set_from_left_part)
    {
        for (auto & p : key_pairs)
            std::swap(p.first, p.second);
    }

    NamePairs name_pairs;
    for (auto & [lhs, rhs] : key_pairs)
        name_pairs.emplace_back(lhs.getColumnName(), rhs.getColumnName());

    auto left_pre_join_actions = JoinExpressionActions::getSubDAG(key_pairs | std::views::transform([](const auto & key_pair) { return key_pair.first; }));
    auto right_pre_join_actions = JoinExpressionActions::getSubDAG(key_pairs | std::views::transform([](const auto & key_pair) { return key_pair.second; }));

    /// The right side and keys move into the source of the new set, which index analysis may build
    /// synchronously, before the outer plan materializes the CTEs and builds the sets they read.
    /// Unlike a set planned by `Planner`, this source carries no such prerequisites yet.
    SetSourcePrerequisites prerequisites;
    const auto & right_root = *parent_node->children.at(1);
    prerequisites.collectFromPlan(right_root);
    prerequisites.collectFromSteps(right_root);
    prerequisites.collectFromDAG(right_pre_join_actions);
    if (prerequisites.unsatisfiable)
        return 0;

    /// Decline the conversion if the rewritten plan could not resolve some
    /// input of `join_output_actions`: `mergeInplace` with
    /// `remove_dangling_inputs = true` would drop the unmatched INPUT (e.g.
    /// source `L.col` whose only forwarded form is `arrayJoin(L.col)`),
    /// causing a `NOT_FOUND_COLUMN_IN_BLOCK` exception at execution. The
    /// check must happen before any plan mutation below. Compare against the
    /// real post-expression header (`ActionsDAG::updateHeader`: DAG outputs
    /// plus input columns the DAG does not consume), counting occurrences
    /// per name, because both `updateHeader` and `mergeInplace` match
    /// duplicate names by multiplicity.
    ///
    /// NB: `ActionsDAG::hasArrayJoin()` on the key sub-DAGs cannot be used to
    /// gate this — an `arrayJoin` in a JOIN ON key is not represented as an
    /// `ARRAY_JOIN` DAG node here, so `hasArrayJoin()` returns false and the
    /// dangling input would slip through. The header-resolution check below is
    /// what actually detects the consumed column.
    {
        auto join_output_actions_subdag = JoinExpressionActions::getSubDAG(join_output_actions);
        const auto & left_input_header = parent_node->children.at(0)->step->getOutputHeader();
        auto post_left_header = left_pre_join_actions.updateHeader(*left_input_header);

        std::unordered_map<std::string_view, size_t> forwarded_columns;
        for (const auto & column : post_left_header)
            ++forwarded_columns[column.name];

        for (const auto * input : join_output_actions_subdag.getInputs())
        {
            auto it = forwarded_columns.find(input->result_name);
            if (it == forwarded_columns.end() || it->second == 0)
                return 0;
            --it->second;
        }
    }

    auto * lhs_in_node = parent_node->children.at(0);
    makeExpressionNodeOnTopOf(*lhs_in_node, std::move(left_pre_join_actions), nodes, makeDescription("Calculate join left keys"));
    auto * rhs_in_node = parent_node->children.at(1);
    makeExpressionNodeOnTopOf(*rhs_in_node, std::move(right_pre_join_actions), nodes, makeDescription("Calculate join right keys"));
    parent_node->children.pop_back();

    auto set_settings = settings.set_settings;
    /// Join equality does not match Nulls.
    /// In case we support NullSafeEquals, we should set transform_null_in = true.
    /// But it would require proper support for sets with multiple keys.
    set_settings.transform_null_in = false;
    set_settings.size_limits = settings.network_transfer_limits;

    auto in_source = std::make_unique<QueryPlan>(QueryPlan::extractSubplan(rhs_in_node, nodes));
    prerequisites.addTo(*in_source, settings.network_transfer_limits);

    auto in_conversion = buildInConversion(
        lhs_in_node->step->getOutputHeader(),
        name_pairs,
        std::move(in_source),
        std::move(set_settings));

    {
        auto filter_name = in_conversion.dag.getOutputs().front()->result_name;
        const auto & header = lhs_in_node->step->getOutputHeader();
        auto step = std::make_unique<FilterStep>(header, std::move(in_conversion.dag), filter_name, true);
        lhs_in_node = &nodes.emplace_back(QueryPlan::Node{std::move(step), {lhs_in_node}});
    }

    auto output_header = lhs_in_node->step->getOutputHeader();
    auto creating_sets_step = std::make_unique<DelayedCreatingSetsStep>(
        output_header,
        PreparedSets::Subqueries{std::move(in_conversion.set)},
        settings.network_transfer_limits,
        nullptr);

    auto join_output_actions_dag = cloneSubDAGWithHeader(output_header, JoinExpressionActions::getSubDAG(join_output_actions));

    /// Materialize constant columns in the join output actions DAG.
    /// JoinStepLogical materializes the `__join_result_dummy` constant column in its output header,
    /// but the replacement ExpressionStep does not, causing a block structure mismatch.
    for (auto & output_node : join_output_actions_dag.getOutputs())
        if (output_node->column)
            output_node = &join_output_actions_dag.materializeNode(*output_node, /*materialize_sparse=*/ false);

    creating_sets_step->setStepDescription("Create sets after JOIN -> IN optimization");
    parent = std::move(creating_sets_step);
    parent_node->children = {lhs_in_node};

    makeExpressionNodeOnTopOf(*parent_node, std::move(join_output_actions_dag), nodes, makeDescription("Join output actions"));

    /// JoinLogical is replaced to [Expression(left_pre_join_actions), Expression(IN), DelayedCreatingSets, Expression(join_output_actions)]
    return 5;
}

}
