#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Columns/ColumnConst.h>
#include <Common/FieldAccurateComparison.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/UnionStep.h>

#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/NullSource.h>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Core/Settings.h>
#include <Storages/StorageDummy.h>
#include <Storages/VirtualColumnUtils.h>
#include <Planner/PlannerExpressionAnalysis.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/ProjectionsDescription.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool force_optimize_projection;
    extern const SettingsString preferred_optimize_projection_name;
}
}

namespace DB::QueryPlanOptimizations
{

/// ---- BEGIN: Predicate-implication helpers (shared logic with optimizeUseNormalProjection.cpp) ----

/// Extract AND-connected conjuncts from an AST expression tree.
static void extractConjunctsFromAST(const ASTPtr & expr, std::vector<ASTPtr> & result)
{
    if (const auto * func = expr->as<ASTFunction>(); func && func->name == "and" && func->arguments)
    {
        for (const auto & child : func->arguments->children)
            extractConjunctsFromAST(child, result);
    }
    else
    {
        result.push_back(expr);
    }
}

/// Strip a leading analyzer table qualifier (e.g. `__table1.`) from a column name.
static std::string_view stripTableQualifier(std::string_view name)
{
    static constexpr std::string_view prefix = "__table";
    if (!name.starts_with(prefix))
        return name;

    size_t pos = prefix.size();
    while (pos < name.size() && isdigit(static_cast<unsigned char>(name[pos])))
        ++pos;

    if (pos > prefix.size() && pos < name.size() && name[pos] == '.')
        return name.substr(pos + 1);

    return name;
}

/// Structurally compare a query-filter DAG node against a projection-WHERE AST conjunct.
static bool matchDAGNodeToAST(const ActionsDAG::Node * node, const ASTPtr & ast)
{
    while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();

    if (const auto * func = ast->as<ASTFunction>())
    {
        if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
            return false;
        if (node->function_base->getName() != func->name)
            return false;

        const ASTs empty;
        const auto & ast_args = func->arguments ? func->arguments->children : empty;
        if (node->children.size() != ast_args.size())
            return false;

        for (size_t i = 0; i < ast_args.size(); ++i)
            if (!matchDAGNodeToAST(node->children[i], ast_args[i]))
                return false;

        return true;
    }

    if (const auto * ident = ast->as<ASTIdentifier>())
    {
        if (node->type != ActionsDAG::ActionType::INPUT)
            return false;
        return stripTableQualifier(node->result_name) == ident->name();
    }

    if (const auto * literal = ast->as<ASTLiteral>())
    {
        if (node->type != ActionsDAG::ActionType::COLUMN || !node->column)
            return false;
        const auto * const_col = typeid_cast<const ColumnConst *>(node->column.get());
        if (!const_col)
            return false;
        return accurateEquals(const_col->getField(), literal->value);
    }

    return false;
}

static bool containsAliases(const ASTPtr & expr)
{
    if (!expr)
        return false;
    if (!expr->tryGetAlias().empty())
        return true;
    for (const auto & child : expr->children)
        if (containsAliases(child))
            return true;
    return false;
}

static bool containsNonDeterministicFunctions(const ASTPtr & expr, ContextPtr context)
{
    if (!expr)
        return false;
    if (const auto * func = expr->as<ASTFunction>())
    {
        auto resolver = FunctionFactory::instance().tryGet(func->name, context);
        if (resolver)
        {
            if (!resolver->isDeterministic() || !resolver->isDeterministicInScopeOfQuery())
                return true;
        }
        else
            return true;
    }
    for (const auto & child : expr->children)
        if (containsNonDeterministicFunctions(child, context))
            return true;
    return false;
}

/// Check whether a query's WHERE condition logically implies a projection's WHERE condition.
static bool doesQueryFilterImplyProjectionWhere(
    const ActionsDAG::Node * query_filter_node,
    const ASTPtr & projection_where,
    const ASTPtr & projection_query_ast,
    ContextPtr context)
{
    if (!projection_where)
        return true;
    if (!query_filter_node)
        return false;

    if (containsAliases(projection_where))
        return false;
    if (containsNonDeterministicFunctions(projection_where, context))
        return false;

    if (projection_query_ast)
    {
        if (const auto * projection_select = projection_query_ast->as<ASTSelectQuery>())
        {
            if (projection_select->with())
                return false;
            if (projection_select->select() && containsAliases(projection_select->select()))
                return false;
        }
    }

    std::vector<ASTPtr> proj_conjuncts;
    extractConjunctsFromAST(projection_where, proj_conjuncts);

    const auto * filter_root = query_filter_node;
    while (filter_root->type == ActionsDAG::ActionType::ALIAS && !filter_root->children.empty())
        filter_root = filter_root->children.front();

    auto query_atoms = ActionsDAG::extractConjunctionAtoms(filter_root);

    for (const auto & proj_conj : proj_conjuncts)
    {
        bool found = std::any_of(
            query_atoms.begin(),
            query_atoms.end(),
            [&](const auto * atom) { return matchDAGNodeToAST(atom, proj_conj); });
        if (!found)
            return false;
    }

    return true;
}

/// Build a residual query filter by removing conjuncts already covered by the projection's WHERE.
/// Returns nullptr if all query conjuncts are covered (no residual filter needed).
static const ActionsDAG::Node * buildResidualFilterNode(
    const ActionsDAG::Node * query_filter_node,
    const ASTPtr & projection_where,
    ActionsDAG & dag)
{
    if (!query_filter_node || !projection_where)
        return query_filter_node;

    const auto * filter_root = query_filter_node;
    while (filter_root->type == ActionsDAG::ActionType::ALIAS && !filter_root->children.empty())
        filter_root = filter_root->children.front();

    auto query_atoms = ActionsDAG::extractConjunctionAtoms(filter_root);

    std::vector<ASTPtr> proj_conjuncts;
    extractConjunctsFromAST(projection_where, proj_conjuncts);

    /// Keep only query atoms that do NOT match any projection-WHERE conjunct.
    ActionsDAG::NodeRawConstPtrs residual_atoms;
    for (const auto * atom : query_atoms)
    {
        bool matched = std::any_of(
            proj_conjuncts.begin(),
            proj_conjuncts.end(),
            [&](const ASTPtr & proj_conj) { return matchDAGNodeToAST(atom, proj_conj); });
        if (!matched)
            residual_atoms.push_back(atom);
    }

    if (residual_atoms.size() == query_atoms.size())
        return query_filter_node; /// Nothing was stripped

    if (residual_atoms.empty())
        return nullptr; /// All conjuncts matched — no residual filter

    if (residual_atoms.size() == 1)
        return residual_atoms.front();

    /// Combine remaining atoms with AND.
    FunctionOverloadResolverPtr func_builder_and =
        std::make_unique<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionAnd>());

    return &dag.addFunction(func_builder_and, std::move(residual_atoms), {});
}

/// ---- END: Predicate-implication helpers ----

using DAGIndex = std::unordered_map<std::string_view, const ActionsDAG::Node *>;
static DAGIndex buildDAGIndex(const ActionsDAG & dag)
{
    DAGIndex index;
    for (const auto * output : dag.getOutputs())
        index.emplace(output->result_name, output);

    return index;
}

/// Required analysis info from aggregate projection.
struct AggregateProjectionInfo
{
    std::optional<ActionsDAG> before_aggregation;
    Names keys;
    AggregateDescriptions aggregates;

    /// A context copy from interpreter which was used for analysis.
    /// Just in case it is used by some function.
    ContextPtr context;
};

/// Get required info from aggregate projection.
/// Ideally, this should be pre-calculated and stored inside ProjectionDescription.
static AggregateProjectionInfo getAggregatingProjectionInfo(
    const ProjectionDescription & projection,
    const ContextPtr & context,
    const StorageMetadataPtr & metadata_snapshot,
    const Block & key_virtual_columns)
{
    /// This is a bad approach.
    /// We'd better have a separate interpreter for projections.
    /// Now it's not obvious we didn't miss anything here.
    ///
    /// Setting ignoreASTOptimizations is used because some of them are invalid for projections.
    /// Example: 'SELECT min(c0), max(c0), count() GROUP BY -c0' for minmax_count projection can be rewritten to
    /// 'SELECT min(c0), max(c0), count() GROUP BY c0' which is incorrect cause we store a column '-c0' in projection.
    InterpreterSelectQuery interpreter(
        projection.query_ast,
        context,
        Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(metadata_snapshot->getSampleBlockWithSubcolumns()))),
        SelectQueryOptions{QueryProcessingStage::WithMergeableState}.ignoreASTOptimizations().ignoreSettingConstraints());

    const auto & analysis_result = interpreter.getAnalysisResult();
    const auto & query_analyzer = interpreter.getQueryAnalyzer();

    AggregateProjectionInfo info;
    info.context = interpreter.getContext();
    info.before_aggregation = analysis_result.before_aggregation->dag.clone();
    info.keys = query_analyzer->aggregationKeys().getNames();
    info.aggregates = query_analyzer->aggregates();

    /// Add part/partition virtual columns to projection aggregation keys.
    /// We can do it because projection is stored for every part separately.
    for (const auto & virt_column : key_virtual_columns)
    {
        const auto * input = &info.before_aggregation->addInput(virt_column.name, virt_column.type);
        info.before_aggregation->getOutputs().push_back(input);
        info.keys.push_back(virt_column.name);
    }

    return info;
}

struct AggregateFunctionMatch
{
    const AggregateDescription * description = nullptr;
    DataTypes argument_types;
};

using AggregateFunctionMatches = std::vector<AggregateFunctionMatch>;

/// Here we try to match aggregate functions from the query to
/// aggregate functions from projection.
static std::optional<AggregateFunctionMatches> matchAggregateFunctions(
    const AggregateProjectionInfo & info,
    const AggregateDescriptions & aggregates,
    const MatchedTrees::Matches & matches,
    const DAGIndex & query_index,
    const DAGIndex & proj_index)
{
    AggregateFunctionMatches res;

    /// Index (projection agg function name) -> pos
    std::unordered_map<std::string, std::vector<size_t>> projection_aggregate_functions;
    for (size_t i = 0; i < info.aggregates.size(); ++i)
        projection_aggregate_functions[info.aggregates[i].function->getName()].push_back(i);

    for (const auto & aggregate : aggregates)
    {
        /// Get a list of candidates by name first.
        auto it = projection_aggregate_functions.find(aggregate.function->getName());
        if (it == projection_aggregate_functions.end())
            return {};

        size_t num_args = aggregate.argument_names.size();

        DataTypes argument_types;
        argument_types.reserve(num_args);

        auto & candidates = it->second;
        bool found_match = false;

        for (size_t idx : candidates)
        {
            argument_types.clear();
            const auto & candidate = info.aggregates[idx];

            /// In some cases it's possible only to check that states are equal,
            /// e.g. for quantile(0.3)(...) and quantile(0.5)(...).
            ///
            /// Note we already checked that aggregate function names are equal,
            /// so that functions sum(...) and sumIf(...) with equal states will
            /// not match.
            if (!candidate.function->getStateType()->equals(*aggregate.function->getStateType()))
                continue;

            /// This is a special case for the function count().
            /// We can assume that 'count(expr) == count()' if expr is not nullable,
            /// which can be verified by simply casting to `AggregateFunctionCount *`.
            if (typeid_cast<const AggregateFunctionCount *>(aggregate.function.get())
                && typeid_cast<const AggregateFunctionCount *>(candidate.function.get()))
            {
                /// we can ignore arguments for count()
                found_match = true;
                res.push_back({&candidate, DataTypes()});
                break;
            }

            /// Now, function names and types matched.
            /// Next, match arguments from DAGs.

            if (num_args != candidate.argument_names.size())
                continue;

            size_t next_arg = 0;
            while (next_arg < num_args)
            {
                const auto & query_name = aggregate.argument_names[next_arg];
                const auto & proj_name = candidate.argument_names[next_arg];

                auto jt = query_index.find(query_name);
                auto kt = proj_index.find(proj_name);

                /// This should not happen ideally.
                if (jt == query_index.end() || kt == proj_index.end())
                    break;

                const auto * query_node = jt->second;
                const auto * proj_node = kt->second;

                auto mt = matches.find(query_node);
                if (mt == matches.end())
                    break;

                const auto & node_match = mt->second;
                if (node_match.node != proj_node || node_match.monotonicity)
                    break;

                argument_types.push_back(query_node->result_type);
                ++next_arg;
            }

            if (next_arg < aggregate.argument_names.size())
                continue;

            found_match = true;
            res.push_back({&candidate, std::move(argument_types)});
            break;
        }

        if (!found_match)
            return {};
    }

    return res;
}

static void appendAggregateFunctions(
    ActionsDAG & proj_dag,
    const AggregateDescriptions & aggregates,
    const AggregateFunctionMatches & matched_aggregates)
{
    std::unordered_map<const AggregateDescription *, const ActionsDAG::Node *> inputs;

    /// Just add all the aggregates to dag inputs.
    auto & proj_dag_outputs =  proj_dag.getOutputs();
    size_t num_aggregates = aggregates.size();
    for (size_t i = 0; i < num_aggregates; ++i)
    {
        const auto & aggregate = aggregates[i];
        const auto & match = matched_aggregates[i];
        auto type = std::make_shared<DataTypeAggregateFunction>(aggregate.function, match.argument_types, aggregate.parameters);

        auto & input = inputs[match.description];
        if (!input)
            input = &proj_dag.addInput(match.description->column_name, type);

        const auto * node = input;

        if (!DataTypeAggregateFunction::strictEquals(type, node->result_type))
            /// Cast to aggregate types specified in query if it's not
            /// strictly the same as the one specified in projection. This
            /// is required to generate correct results during finalization.
            node = &proj_dag.addCast(*node, type, aggregate.column_name, nullptr);
        else if (node->result_name != aggregate.column_name)
            node = &proj_dag.addAlias(*node, aggregate.column_name);

        proj_dag_outputs.push_back(node);
    }
}

static std::optional<ActionsDAG> analyzeAggregateProjection(
    const AggregateProjectionInfo & info,
    const QueryDAG & query,
    const DAGIndex & query_index,
    const Names & keys,
    const AggregateDescriptions & aggregates,
    size_t max_set_size_for_match)
{
    auto proj_index = buildDAGIndex(*info.before_aggregation);

    MatchedTrees::Matches matches = matchTrees(
        info.before_aggregation->getOutputs(), *query.dag, false /* check_monotonicity */, max_set_size_for_match);
    auto matched_aggregates = matchAggregateFunctions(info, aggregates, matches, query_index, proj_index);
    if (!matched_aggregates)
        return {};

    ActionsDAG::NodeRawConstPtrs query_key_nodes;
    std::unordered_set<const ActionsDAG::Node *> proj_key_nodes;

    {
        /// Just, filling the set above.

        for (const auto & key : info.keys)
        {
            auto it = proj_index.find(key);
            /// This should not happen ideally.
            if (it == proj_index.end())
                return {};

            proj_key_nodes.insert(it->second);
        }

        query_key_nodes.reserve(keys.size() + 1);

        /// We need to add filter column to keys set.
        /// It should be computable from projection keys.
        /// It will be removed in FilterStep.
        if (query.filter_node)
            query_key_nodes.push_back(query.filter_node);

        for (const auto & key : keys)
        {
            auto it = query_index.find(key);
            /// This should not happen ideally.
            if (it == query_index.end())
                return {};

            query_key_nodes.push_back(it->second);
        }
    }

    /// Here we want to match query keys with projection keys.
    /// Query key can be any expression depending on projection keys.
    auto new_inputs = resolveMatchedInputs(matches, proj_key_nodes, query_key_nodes);
    if (!new_inputs)
        return {};

    auto proj_dag = ActionsDAG::foldActionsByProjection(*new_inputs, query_key_nodes);
    appendAggregateFunctions(proj_dag, aggregates, *matched_aggregates);
    return proj_dag;
}


struct AggregateProjectionCandidate : public ProjectionCandidate
{
    AggregateProjectionInfo info;

    /// Actions which need to be applied to columns from projection
    /// in order to get all the columns required for aggregation.
    ActionsDAG dag;

    /// Whether this candidate's DAG includes a filter output (first output).
    /// False when a filtered projection's WHERE fully covers the query filter
    /// (residual is empty), so the DAG has no filter column.
    bool has_filter = false;
};

struct MinMaxProjectionCandidate
{
    AggregateProjectionCandidate candidate;
    Block block;
};

struct AggregateProjectionCandidates
{
    std::vector<AggregateProjectionCandidate> real;
    std::optional<MinMaxProjectionCandidate> minmax_projection;

    /// This flag means that DAG for projection candidate should be used in FilterStep.
    bool has_filter = false;

    /// If not empty, try to find exact ranges from parts to speed up trivial count queries.
    String only_count_column;
};

static AggregateProjectionCandidates getAggregateProjectionCandidates(
    QueryPlan::Node & node,
    AggregatingStep & aggregating,
    ReadFromMergeTree & reading,
    const PartitionIdToMaxBlockPtr & max_added_blocks,
    bool allow_implicit_projections,
    size_t max_set_size_for_match)
{
    const auto & keys = aggregating.getParams().keys;
    const auto & aggregates = aggregating.getParams().aggregates;
    const auto metadata = reading.getStorageMetadata();
    Block key_virtual_columns = reading.getMergeTreeData().getHeaderWithVirtualsForFilter(metadata);

    AggregateProjectionCandidates candidates;

    ContextPtr context = reading.getContext();

    const auto & projections = metadata->projections;
    std::vector<const ProjectionDescription *> agg_projections;

    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Aggregate)
            agg_projections.push_back(&projection);

    bool can_use_minmax_projection = allow_implicit_projections
        && metadata->minmax_count_projection
        && !reading.getMutationsSnapshot()->hasLightweightDeletedMask();

    if (!can_use_minmax_projection && agg_projections.empty())
        return candidates;

    QueryDAG dag;
    if (!dag.build(*node.children.front()))
        return candidates;

    auto query_index = buildDAGIndex(*dag.dag);

    candidates.has_filter = dag.filter_node;
    /// We can't use minmax projection if filter has non-deterministic functions.
    if (dag.filter_node && !VirtualColumnUtils::isDeterministicInScopeOfQuery(dag.filter_node))
        can_use_minmax_projection = false;

    if (can_use_minmax_projection)
    {
        const auto * projection = &*(metadata->minmax_count_projection);
        auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
        if (auto proj_dag = analyzeAggregateProjection(info, dag, query_index, keys, aggregates, max_set_size_for_match))
        {
            AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(*proj_dag)};
            candidate.has_filter = (dag.filter_node != nullptr);

            auto block = reading.getMergeTreeData().getMinMaxCountProjectionBlock(
                metadata,
                candidate.dag.getRequiredColumnsNames(),
                (dag.filter_node ? &*dag.dag : nullptr),
                reading.getParts(),
                max_added_blocks.get(),
                context);

            // minmax_count_projection cannot be used when there is no data to process, because
            // it will produce incorrect result during constant aggregation.
            // See https://github.com/ClickHouse/ClickHouse/issues/36728
            if (!block.empty())
            {
                MinMaxProjectionCandidate minmax;
                minmax.candidate = std::move(candidate);
                minmax.block = std::move(block);
                minmax.candidate.projection = projection;
                candidates.minmax_projection.emplace(std::move(minmax));
            }
        }
        else
        {
            /// Trivial count optimization only applies after @can_use_minmax_projection.
            if (keys.empty() && aggregates.size() == 1 && typeid_cast<const AggregateFunctionCount *>(aggregates[0].function.get()))
                candidates.only_count_column = aggregates[0].column_name;
        }
    }

    if (!candidates.minmax_projection)
    {
        auto it = std::find_if(
            agg_projections.begin(),
            agg_projections.end(),
            [&](const auto * projection)
            { return projection->name == context->getSettingsRef()[Setting::preferred_optimize_projection_name].value; });

        if (it != agg_projections.end())
        {
            const ProjectionDescription * preferred_projection = *it;
            agg_projections.clear();
            agg_projections.push_back(preferred_projection);
        }

        candidates.real.reserve(agg_projections.size());
        for (const auto * projection : agg_projections)
        {
            /// Skip projections whose WHERE condition is not implied by the query's filter.
            if (projection->where_clause_ast)
            {
                if (!doesQueryFilterImplyProjectionWhere(dag.filter_node, projection->where_clause_ast, projection->query_ast, context))
                    continue;
            }

            /// When the projection has a WHERE, strip the implied conjuncts from the query
            /// filter so that analyzeAggregateProjection does not require the filter column
            /// to be computable from projection keys (it is already satisfied by the projection).
            const auto * original_filter = dag.filter_node;
            if (projection->where_clause_ast && dag.filter_node && dag.dag)
                dag.filter_node = buildResidualFilterNode(original_filter, projection->where_clause_ast, *dag.dag);

            auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
            if (auto proj_dag = analyzeAggregateProjection(info, dag, query_index, keys, aggregates, max_set_size_for_match))
            {
                AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(*proj_dag)};
                candidate.projection = projection;
                candidate.has_filter = (dag.filter_node != nullptr);
                candidates.real.emplace_back(std::move(candidate));
            }

            dag.filter_node = original_filter;
        }
    }

    return candidates;
}

static AggregateProjectionCandidates getAggregateProjectionCandidates(
    QueryPlan::Node & node, DistinctStep & distinct, ReadFromMergeTree & reading, size_t max_set_size_for_match)
{
    const auto metadata = reading.getStorageMetadata();
    Block key_virtual_columns = reading.getMergeTreeData().getHeaderWithVirtualsForFilter(metadata);

    ContextPtr context = reading.getContext();

    const auto & projections = metadata->projections;
    std::vector<const ProjectionDescription *> agg_projections;

    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Aggregate)
            agg_projections.push_back(&projection);

    AggregateProjectionCandidates candidates;

    if (agg_projections.empty())
        return candidates;

    QueryDAG dag;
    if (!dag.build(*node.children.front()))
        return candidates;

    auto query_index = buildDAGIndex(*dag.dag);
    candidates.has_filter = dag.filter_node;

    const auto & keys = distinct.getColumnNames();

    /// Prefer the user specified projection if any.
    auto it = std::find_if(
        agg_projections.begin(),
        agg_projections.end(),
        [&](const auto * projection)
        { return projection->name == context->getSettingsRef()[Setting::preferred_optimize_projection_name].value; });

    if (it != agg_projections.end())
    {
        const ProjectionDescription * preferred_projection = *it;
        agg_projections.clear();
        agg_projections.push_back(preferred_projection);
    }

    AggregateDescriptions aggregates; // Empty for DISTINCT
    candidates.real.reserve(agg_projections.size());

    /// Only select the projection where distinct columns are a subset of projection columns.
    for (const auto * projection : agg_projections)
    {
        /// Skip projections whose WHERE condition is not implied by the query's filter.
        if (projection->where_clause_ast)
        {
            if (!doesQueryFilterImplyProjectionWhere(dag.filter_node, projection->where_clause_ast, projection->query_ast, context))
                continue;
        }

        /// Strip implied conjuncts from the query filter (see aggregation path above).
        const auto * original_filter = dag.filter_node;
        if (projection->where_clause_ast && dag.filter_node && dag.dag)
            dag.filter_node = buildResidualFilterNode(original_filter, projection->where_clause_ast, *dag.dag);

        auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
        if (auto proj_dag = analyzeAggregateProjection(info, dag, query_index, keys, aggregates, max_set_size_for_match))
        {
            AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(*proj_dag)};
            candidate.projection = projection;
            candidate.has_filter = (dag.filter_node != nullptr);
            candidates.real.emplace_back(std::move(candidate));
        }

        dag.filter_node = original_filter;
    }

    return candidates;
}

static QueryPlan::Node * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * /*reading*/ _ = typeid_cast<ReadFromMergeTree *>(step))
        return &node;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front());

    return nullptr;
}

/// Pseudo projection name used to indicate exact count optimization
static constexpr const char * EXACT_COUNT_PROJECTION_NAME = "_exact_count_projection";

std::optional<String> optimizeUseAggregateProjections(
    QueryPlan::Node & node,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    if (node.children.size() != 1)
        return {};

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());

    auto * distinct = typeid_cast<DistinctStep *>(node.step.get());

    /// In the event there is DISTINCT but no GROUP BY, we still want to use aggregate projections.
    if (!aggregating && !distinct)
        return {};

    if (aggregating && !aggregating->canUseProjection())
        return {};

    QueryPlan::Node * reading_node = findReadingStep(*node.children.front());
    if (!reading_node)
        return {};

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());
    if (!reading)
        return {};

    if (!canUseProjectionForReadingStep(reading))
        return {};

    PartitionIdToMaxBlockPtr max_added_blocks = getMaxAddedBlocks(reading);

    const size_t max_set_size_for_match = optimization_settings.max_set_size_for_projection_match;
    auto candidates
        = (distinct ? getAggregateProjectionCandidates(node, *distinct, *reading, max_set_size_for_match)
                    : getAggregateProjectionCandidates(
                          node,
                          *aggregating,
                          *reading,
                          max_added_blocks,
                          optimization_settings.optimize_use_implicit_projections,
                          max_set_size_for_match));

    auto logger = getLogger("optimizeUseAggregateProjections");
    const auto & query_info = reading->getQueryInfo();
    const auto metadata = reading->getStorageMetadata();
    ContextPtr context = reading->getContext();
    AggregateProjectionCandidate * best_candidate = nullptr;

    /// Stores row count from exact ranges of parts.
    size_t exact_count = 0;
    ReadFromMergeTree::AnalysisResultPtr parent_reading_select_result;
    ReadFromMergeTree::AnalysisResultPtr inexact_ranges_select_result;
    if (candidates.minmax_projection)
    {
        best_candidate = &candidates.minmax_projection->candidate;
    }
    else if (!candidates.real.empty() || !candidates.only_count_column.empty())
    {
        parent_reading_select_result = reading->getAnalyzedResult();
        bool find_exact_ranges = !candidates.only_count_column.empty();
        if (!parent_reading_select_result || (!parent_reading_select_result->has_exact_ranges && find_exact_ranges))
            parent_reading_select_result = reading->selectRangesToRead(find_exact_ranges);

        bool force_optimize_projection = context->getSettingsRef()[Setting::force_optimize_projection];

        if (!force_optimize_projection)
        {
            /// /// Nothing to read. Ignore projections.
            if (parent_reading_select_result->parts_with_ranges.empty())
                return {};
        }

        /// Try to identify ranges that can be exactly counted using primary key analysis.
        if (!candidates.only_count_column.empty())
        {
            /// Copy parent analysis result to isolate modifications during exact count optimization. This result will
            /// store the remaining inexact ranges (not covered by exact analysis), to be used for for normal reading
            /// and count calculation.
            inexact_ranges_select_result = std::make_shared<ReadFromMergeTree::AnalysisResult>(*parent_reading_select_result);
            auto & parent_parts_with_ranges = inexact_ranges_select_result->parts_with_ranges;
            size_t exact_ranges_count = 0;
            size_t exact_ranges_parts = 0;
            size_t exact_ranges_marks = 0;

            for (auto & part_with_ranges : parent_parts_with_ranges)
            {
                MarkRanges new_ranges;
                auto & ranges = part_with_ranges.ranges;
                const auto & exact_ranges = part_with_ranges.exact_ranges;
                if (exact_ranges.empty())
                    continue;

                size_t i = 0;
                size_t len = exact_ranges.size();
                exact_ranges_count += len;
                exact_ranges_parts += 1;
                exact_ranges_marks += exact_ranges.getNumberOfMarks();
                for (auto & range : ranges)
                {
                    while (i < len && exact_ranges[i].begin < range.end)
                    {
                        chassert(exact_ranges[i].begin >= range.begin);
                        chassert(exact_ranges[i].end <= range.end);

                        /// Found some marks which are not exact
                        if (range.begin < exact_ranges[i].begin)
                            new_ranges.emplace_back(range.begin, exact_ranges[i].begin);

                        range.begin = exact_ranges[i].end;
                        inexact_ranges_select_result->selected_marks -= exact_ranges[i].end - exact_ranges[i].begin;
                        size_t range_rows = part_with_ranges.data_part->index_granularity->getRowsCountInRange(exact_ranges[i]);
                        inexact_ranges_select_result->selected_rows -= range_rows;
                        exact_count += range_rows;
                        ++i;
                    }

                    /// Current range still contains some marks which are not exact
                    if (range.begin < range.end)
                        new_ranges.emplace_back(range);
                }
                chassert(i == len);

                ssize_t range_diff = new_ranges.size() - part_with_ranges.ranges.size();
                inexact_ranges_select_result->selected_ranges += range_diff;
                part_with_ranges.ranges = std::move(new_ranges);
            }

            auto & stat = inexact_ranges_select_result->projection_stats.emplace_back();
            stat.name = EXACT_COUNT_PROJECTION_NAME;
            for (const auto & index_stat : inexact_ranges_select_result->index_stats)
            {
                if (index_stat.type == ReadFromMergeTree::IndexType::PrimaryKey)
                {
                    stat.condition = index_stat.condition;
                    stat.search_algorithm = index_stat.search_algorithm;
                }
            }

            stat.selected_parts = exact_ranges_parts;
            stat.selected_marks = exact_ranges_marks;
            stat.selected_ranges = exact_ranges_count;
            stat.selected_rows = exact_count;
            stat.filtered_parts
                = std::erase_if(parent_parts_with_ranges, [&](const auto & part_with_ranges) { return part_with_ranges.ranges.empty(); });

            stat.description = fmt::format(
                "Exact count optimization is applied: {} ranges with exact row count {}. Filtered parts: {}",
                stat.selected_ranges,
                stat.selected_rows,
                stat.filtered_parts);

            LOG_DEBUG(logger, "{}", stat.description);

            inexact_ranges_select_result->selected_parts = parent_parts_with_ranges.size();
            /// The original result may have exceeded_row_limits set because the full table scan
            /// was over the limit.  After subtracting exact ranges the remaining rows are fewer,
            /// so clear the flag — the reduced result will be re-checked during execution.
            inexact_ranges_select_result->exceeded_row_limits = false;
            if (parent_parts_with_ranges.empty())
            {
                chassert(inexact_ranges_select_result->selected_marks == 0);
                chassert(inexact_ranges_select_result->selected_rows == 0);
                chassert(inexact_ranges_select_result->selected_ranges == 0);
            }
        }

        auto empty_mutations_snapshot = reading->getMutationsSnapshot()->cloneEmpty();

        /// If there are remaining parts to read, attempt to select the best candidate.
        if (!parent_reading_select_result->parts_with_ranges.empty() || force_optimize_projection)
        {
            for (auto & candidate : candidates.real)
            {
                auto required_column_names = candidate.dag.getRequiredColumnsNames();

                auto projection_query_info = query_info;
                projection_query_info.prewhere_info = nullptr;
                projection_query_info.row_level_filter = nullptr;
                /// `candidate.dag` is the projection-rewrite DAG. Its first output is a real `WHERE` /
                /// `PREWHERE` filter predicate only when this candidate has a (residual) filter
                /// (`candidate.has_filter`); otherwise — either the query has no filter, or the projection's
                /// own `WHERE` fully covers it — the first output is a projection key column. Part selection
                /// in `MergeTreeDataSelectExecutor::estimateNumMarksToRead` treats
                /// `filter_actions_dag->getOutputs().front()` as a filter predicate for primary-key and
                /// skip-index analysis, so installing the rewrite DAG as the pruning filter when there is no
                /// real filter would make it prune on a bare key column (e.g. since #89222 a numeric key
                /// column is read as `key != 0`), which is wrong. See #89222.
                projection_query_info.filter_actions_dag = candidate.has_filter
                    ? std::make_unique<ActionsDAG>(candidate.dag.clone())
                    : nullptr;

                MergeTreeDataSelectExecutor reader(reading->getMergeTreeData(), candidate.projection);
                bool analyzed = analyzeProjectionCandidate(
                    candidate,
                    reader,
                    empty_mutations_snapshot,
                    required_column_names,
                    metadata,
                    *parent_reading_select_result,
                    projection_query_info,
                    context);

                if (!analyzed)
                    continue;

                auto & stat = parent_reading_select_result->projection_stats.emplace_back();
                stat.name = candidate.projection->name;
                for (const auto & index_stat : candidate.merge_tree_projection_select_result_ptr->index_stats)
                {
                    if (index_stat.type == ReadFromMergeTree::IndexType::PrimaryKey)
                    {
                        stat.condition = index_stat.condition;
                        stat.search_algorithm = index_stat.search_algorithm;
                    }
                }
                stat.selected_parts = candidate.selected_parts;
                stat.selected_marks = candidate.selected_marks;
                stat.selected_ranges = candidate.selected_ranges;
                stat.selected_rows = candidate.selected_rows;
                stat.filtered_parts = candidate.filtered_parts;
                candidate.stat = &stat;

                size_t parent_reading_marks = parent_reading_select_result->selected_marks;
                if (candidate.sum_marks > parent_reading_marks)
                {
                    stat.description = fmt::format(
                        "Projection {} is usable but requires reading {} marks, which is not better than the original table with {} marks",
                        candidate.projection->name,
                        candidate.sum_marks,
                        parent_reading_marks);

                    LOG_DEBUG(logger, "{}", stat.description);
                    continue;
                }

                if (best_candidate == nullptr || best_candidate->sum_marks > candidate.sum_marks)
                    best_candidate = &candidate;
            }
        }

        /// No suitable projection found, and exact count optimization is not used.
        if (!best_candidate && exact_count == 0)
            return {};
    }
    else
    {
        return {};
    }

    /// Identify projections selected as the best candidates and update their stat descriptions with appropriate logging
    if (best_candidate)
    {
        for (const auto & candidate : candidates.real)
        {
            chassert(parent_reading_select_result);

            if (&candidate == best_candidate)
            {
                chassert(candidate.stat);
                chassert(candidate.stat->description.empty());
                candidate.stat->description = fmt::format(
                    "Projection {} is selected as the best with {} marks to read, while the original table requires scanning {} marks",
                    candidate.projection->name,
                    candidate.sum_marks,
                    parent_reading_select_result->selected_marks);
                LOG_DEBUG(logger, "{}", candidate.stat->description);
            }
            else if (candidate.stat && candidate.stat->description.empty())
            {
                candidate.stat->description = fmt::format(
                    "Projection {} is usable but requires reading {} marks, which is less efficient than projection {} with {} marks",
                    candidate.projection->name,
                    candidate.sum_marks,
                    best_candidate->projection->name,
                    best_candidate->sum_marks);
                LOG_DEBUG(logger, "{}", candidate.stat->description);
            }
        }
    }

    QueryPlanStepPtr projection_reading;
    bool has_parent_parts = false;
    String selected_projection_name;
    if (best_candidate)
        selected_projection_name = best_candidate->projection->name;

    bool is_parallel_reading_on_remote_replicas = reading->isParallelReadingEnabled()
        && !optimization_settings.is_parallel_replicas_initiator_with_projection_support;
    /// Add reading from projection step.
    if (candidates.minmax_projection)
    {
        /// The min-max projection optimization modifies ReadFromMergeTree to ReadFromPreparedSource.
        /// -------------------------------------------------------------------------------------------------
        ///  ReadFromMergeTree  ---is replaced by--->       ReadFromPreparedSource (_minmax_count_projection)
        /// -------------------------------------------------------------------------------------------------
        /// When parallel replicas is enabled, only the initiator should read the min-max projection to avoid data duplication.
        Pipe pipe;
        if (!is_parallel_reading_on_remote_replicas)
            pipe = Pipe(
                std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(candidates.minmax_projection->block))));
        else
            pipe = Pipe(std::make_shared<NullSource>(std::make_shared<const Block>(candidates.minmax_projection->block.cloneEmpty())));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        has_parent_parts = false;
    }
    else if (best_candidate == nullptr)
    {
        /// Exact count optimization.
        chassert(exact_count > 0);
        chassert(inexact_ranges_select_result);

        auto agg_count = std::make_shared<AggregateFunctionCount>(DataTypes{});

        std::vector<char> state(agg_count->sizeOfData());
        AggregateDataPtr place = state.data();
        agg_count->create(place);
        SCOPE_EXIT_MEMORY_SAFE(agg_count->destroy(place));
        AggregateFunctionCount::set(place, exact_count);

        auto column = ColumnAggregateFunction::create(agg_count);
        column->insertFrom(place);

        Block block_with_count{
            {std::move(column),
             std::make_shared<DataTypeAggregateFunction>(agg_count, DataTypes{}, Array{}),
             candidates.only_count_column}};

        /// The exact count optimization may modify the `ReadFromMergeTree` to use `ReadFromPreparedSource`.
        /// ---------------------------if parent parts have been partially filtered------------------------
        ///                                             AggregatingProjection
        ///  ReadFromMergeTree  ---is replaced by--->       ReadFromMergeTree
        ///                                                 ReadFromPreparedSource (_exact_count_projection)
        /// ------------------------------------------------------------------------------------------------
        /// When parallel replicas is enabled, only the initiator should read the exact count projection to avoid data duplication.
        Pipe pipe;
        if (!is_parallel_reading_on_remote_replicas)
            pipe = Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(block_with_count))));
        else
            pipe = Pipe(std::make_shared<NullSource>(std::make_shared<const Block>(block_with_count.cloneEmpty())));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));

        selected_projection_name = EXACT_COUNT_PROJECTION_NAME;
        has_parent_parts = !inexact_ranges_select_result->parts_with_ranges.empty();
        if (has_parent_parts)
            reading->setAnalyzedResult(std::move(inexact_ranges_select_result));
    }
    else
    {
        chassert(parent_reading_select_result);
        auto storage_snapshot = reading->getStorageSnapshot();
        auto proj_snapshot = std::make_shared<StorageSnapshot>(storage_snapshot->storage, best_candidate->projection->metadata);
        auto projection_query_info = query_info;
        projection_query_info.prewhere_info = nullptr;
        projection_query_info.row_level_filter = nullptr;
        projection_query_info.filter_actions_dag = nullptr;

        MergeTreeDataSelectExecutor reader(reading->getMergeTreeData(), best_candidate->projection);
        projection_reading = reader.readFromParts(
            /* parts = */ {},
            reading->getMutationsSnapshot()->cloneEmpty(),
            best_candidate->dag.getRequiredColumnsNames(),
            proj_snapshot,
            projection_query_info,
            context,
            reading->getMaxBlockSize(),
            reading->getNumStreams(),
            max_added_blocks,
            best_candidate->merge_tree_projection_select_result_ptr,
            reading->isParallelReadingEnabled(),
            reading->getParallelReadingExtension());

        /// Filter out parts in parent_ranges that overlap with those already read by the best candidate projection
        filterPartsByProjection(*parent_reading_select_result, best_candidate->parent_parts);
        has_parent_parts = !parent_reading_select_result->parts_with_ranges.empty();

        /// Only the initiator should read the projection to avoid potential data duplication.
        bool should_skip_projection_reading_on_remote_replicas = is_parallel_reading_on_remote_replicas && has_parent_parts;
        if (!projection_reading || should_skip_projection_reading_on_remote_replicas)
        {
            Pipe pipe(std::make_shared<NullSource>(std::make_shared<const Block>(
                proj_snapshot->getSampleBlockForColumns(best_candidate->dag.getRequiredColumnsNames()))));
            projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        }

        if (has_parent_parts && optimization_settings.is_parallel_replicas_initiator_with_projection_support)
            fallbackToLocalProjectionReading(projection_reading);
    }

    if (!query_info.is_internal && context->hasQueryContext())
    {
        context->getQueryContext()->addQueryAccessInfo(Context::QualifiedProjectionName
        {
            .storage_id = reading->getMergeTreeData().getStorageID(),
            .projection_name = selected_projection_name,
        });
    }

    projection_reading->setStepDescription(selected_projection_name, optimization_settings.max_step_description_length);
    auto & projection_reading_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(projection_reading)});

    /// Root node of optimized child plan using @projection_name
    QueryPlan::Node * aggregate_projection_node = nullptr;

    if (best_candidate)
    {
        aggregate_projection_node = &nodes.emplace_back();

        if (candidates.has_filter && best_candidate->has_filter)
        {
            const auto & result_name = best_candidate->dag.getOutputs().front()->result_name;
            aggregate_projection_node->step = std::make_unique<FilterStep>(
                projection_reading_node.step->getOutputHeader(),
                std::move(best_candidate->dag),
                result_name,
                true);
        }
        else
            aggregate_projection_node->step
                = std::make_unique<ExpressionStep>(projection_reading_node.step->getOutputHeader(), std::move(best_candidate->dag));

        aggregate_projection_node->children.push_back(&projection_reading_node);
    }
    else /// trivial count optimization
    {
        aggregate_projection_node = &projection_reading_node;
    }

    const auto & projection_header = aggregate_projection_node->step->getOutputHeader();

    QueryPlan::Node * source_node = aggregate_projection_node;

    if (aggregating)
    {
        if (has_parent_parts)
            node.step = aggregating->convertToAggregatingProjection(projection_header);
        else
            aggregating->requestOnlyMergeForAggregateProjection(projection_header);
    }
    else
    {
        /// For DISTINCT, handle potential type mismatches between the projection
        /// output and the expected types. This can happen when removeTrivialWrappers
        /// strips materialize/identity from the query DAG, causing the query to match
        /// a projection whose column types differ in LowCardinality wrapping.
        const auto & expected_header = node.step->getOutputHeader();
        if (blocksHaveEqualStructure(*projection_header, *expected_header))
        {
            if (!has_parent_parts)
                node.step->updateInputHeader(projection_header);
        }
        else
        {
            auto converting = ActionsDAG::makeConvertingActions(
                projection_header->getColumnsWithTypeAndName(),
                expected_header->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                context);
            auto & converting_node = nodes.emplace_back();
            converting_node.step = std::make_unique<ExpressionStep>(
                projection_header, std::move(converting));
            converting_node.children.push_back(aggregate_projection_node);
            source_node = &converting_node;
        }
    }

    if (has_parent_parts)
    {
        if (aggregating)
        {
            node.children.push_back(source_node);
        }
        else
        {
            /// Some parts have no projection data. DistinctStep must see rows from both readings
            /// to return the correct set of distinct values; union them into its single input.
            auto * main_node = node.children.front();
            SharedHeaders input_headers = {
                main_node->step->getOutputHeader(),
                source_node->step->getOutputHeader(),
            };
            auto & union_node = nodes.emplace_back();
            union_node.step = std::make_unique<UnionStep>(std::move(input_headers));
            union_node.children = {main_node, source_node};
            node.children.front() = &union_node;
        }
    }
    else
        node.children.front() = source_node;

    return selected_projection_name;
}

}
