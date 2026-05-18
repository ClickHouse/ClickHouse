#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/optimizePrewhere.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/SelectQueryInfo.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST.h>
#include <unordered_set>


namespace DB
{

namespace Setting
{
    extern const SettingsString preferred_optimize_projection_name;
    extern const SettingsBool force_optimize_projection;
    extern const SettingsBool optimize_use_projection_filtering;
}

}

namespace DB::QueryPlanOptimizations
{

/// Extract AND-connected conjuncts from an AST expression tree.
/// For example, (a = 1 AND b = 2 AND c = 3) yields {"a = 1", "b = 2", "c = 3"}.
static void extractConjunctsFromAST(const ASTPtr & expr, std::vector<String> & result)
{
    if (const auto * func = expr->as<ASTFunction>(); func && func->name == "and")
    {
        for (const auto & child : func->arguments->children)
            extractConjunctsFromAST(child, result);
    }
    else
    {
        /// Use the canonical column-name representation for comparison.
        result.push_back(expr->getColumnName());
    }
}

/// Recursively check if an AST tree contains any aliases.
/// If the projection WHERE uses aliases, textual conjunct matching becomes unsafe
/// because the same expression may have different canonical representations when aliased.
static bool containsAliases(const ASTPtr & expr)
{
    if (!expr)
        return false;

    if (!expr->tryGetAlias().empty())
        return true;

    for (const auto & child : expr->children)
    {
        if (containsAliases(child))
            return true;
    }

    return false;
}

/// Recursively check if an AST tree contains calls to non-deterministic functions
/// by querying function metadata via FunctionFactory.
/// Non-deterministic predicates (rand(), now(), nowInBlock(), etc.) evaluate differently
/// at materialization time vs query time, so textual conjunct matching is unsound for them.
static bool containsNonDeterministicFunctions(const ASTPtr & expr, ContextPtr context)
{
    if (!expr)
        return false;

    if (const auto * func = expr->as<ASTFunction>())
    {
        /// Use FunctionFactory metadata instead of a hardcoded blacklist.
        /// This automatically covers all current and future non-deterministic functions.
        /// IFunctionOverloadResolver provides isDeterministic() and isDeterministicInScopeOfQuery()
        /// without needing to resolve argument types.
        auto resolver = FunctionFactory::instance().tryGet(func->name, context);
        if (resolver)
        {
            if (!resolver->isDeterministic() || !resolver->isDeterministicInScopeOfQuery())
                return true;
        }
        else
        {
            /// Unknown function — conservatively treat as non-deterministic.
            return true;
        }
    }

    for (const auto & child : expr->children)
    {
        if (containsNonDeterministicFunctions(child, context))
            return true;
    }

    return false;
}

/// Check whether a query's WHERE condition logically implies a projection's WHERE condition.
/// Uses CNF conjunct containment: every conjunct of the projection's WHERE must appear
/// (as a textually identical sub-expression) among the conjuncts of the query's WHERE.
///
/// Both sides are compared using canonical column-name representations:
/// - Projection WHERE conjuncts use ASTPtr::getColumnName() (canonical AST serialization)
/// - Query filter conjuncts use ActionsDAG::Node::result_name (set by the analyzer)
/// These produce identical strings for semantically equivalent expressions.
///
/// Safety guards:
/// 1. Reject if projection WHERE contains aliases (may differ from analyzer representation)
/// 2. Reject if projection WHERE contains non-deterministic functions (unsafe for implication)
/// 3. Reject if projection has a non-empty WITH clause (aliases referenced by identifier in WHERE
///    won't be detected by `containsAliases` and could cause false-positive matches)
///
/// This is a conservative check — it may reject some valid cases (e.g., range implications),
/// but it is safe: it will never incorrectly accept a query that doesn't match the projection.
static bool doesQueryFilterImplyProjectionWhere(
    const ActionsDAG::Node * query_filter_node,
    const ASTPtr & projection_where,
    const ASTPtr & projection_query_ast,
    ContextPtr context)
{
    if (!projection_where)
        return true; /// No projection filter = always applicable

    if (!query_filter_node)
        return false; /// Projection has filter but query doesn't

    /// Safety guard 1: reject if projection WHERE contains any aliases.
    /// Aliases could cause the canonical column-name to differ from the analyzer's
    /// result_name, leading to false positive implication matches.
    if (containsAliases(projection_where))
        return false;

    /// Safety guard 2: reject if projection WHERE contains non-deterministic functions.
    /// Such expressions evaluate differently at materialization time vs query time,
    /// so textual equality does not imply semantic equivalence.
    if (containsNonDeterministicFunctions(projection_where, context))
        return false;

    /// Safety guard 3: reject if the projection has a non-empty WITH clause.
    /// An identifier in projection WHERE could resolve to a CTE/alias defined in WITH,
    /// in which case textual conjunct matching against the query's WHERE (where the same
    /// identifier refers to a table column) would produce false-positive implications.
    if (projection_query_ast)
    {
        if (const auto * projection_select = projection_query_ast->as<ASTSelectQuery>())
        {
            if (projection_select->with())
                return false;
        }
    }

    /// Extract projection's WHERE conjuncts from the AST.
    std::vector<String> proj_conjuncts;
    extractConjunctsFromAST(projection_where, proj_conjuncts);

    /// Extract query's filter conjuncts using ClickHouse's built-in DAG utility.
    /// This is the same function used by the filter pushdown optimizer.
    auto query_atoms = ActionsDAG::extractConjunctionAtoms(query_filter_node);

    /// Build a set of query conjunct names for O(1) lookup.
    std::unordered_set<String> query_conjunct_names;
    query_conjunct_names.reserve(query_atoms.size());
    for (const auto * atom : query_atoms)
        query_conjunct_names.insert(atom->result_name);

    /// Check that every projection conjunct has an exact match in the query conjuncts.
    for (const auto & proj_conj : proj_conjuncts)
    {
        if (!query_conjunct_names.contains(proj_conj))
            return false;
    }

    return true;
}

/// Normal projection analysis result in case it can be applied.
/// For now, it is empty.
/// Normal projection can be used only if it contains all required source columns.
/// It would not be hard to support pre-computed expressions and filtration.
struct NormalProjectionCandidate : public ProjectionCandidate
{
};

static std::optional<ActionsDAG> makeMaterializingDAG(const Block & proj_header, const Block & main_header)
{
    /// Materialize constants in case we don't have it in output header.
    /// This may happen e.g. if we have PREWHERE.

    size_t num_columns = main_header.columns();
    /// This is a error; will have block structure mismatch later.
    if (proj_header.columns() != num_columns)
        return {};

    std::vector<size_t> const_positions;
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto col_proj = proj_header.getByPosition(i).column;
        auto col_main = main_header.getByPosition(i).column;
        bool is_proj_const = col_proj && isColumnConst(*col_proj);
        bool is_main_proj = col_main && isColumnConst(*col_main);
        if (is_proj_const && !is_main_proj)
            const_positions.push_back(i);
    }

    if (const_positions.empty())
        return {};

    ActionsDAG dag;
    auto & outputs = dag.getOutputs();
    for (const auto & col : proj_header.getColumnsWithTypeAndName())
        outputs.push_back(&dag.addInput(col));

    for (auto pos : const_positions)
    {
        auto & output = outputs[pos];
        output = &dag.materializeNode(*output);
    }

    return dag;
}

std::optional<String> optimizeUseNormalProjections(
    Stack & stack,
    QueryPlan::Nodes & nodes,
    const QueryPlanOptimizationSettings & optimization_settings,
    bool is_parallel_replicas_initiator_with_projection_support,
    size_t max_step_description_length)
{
    const auto & frame = stack.back();

    auto * reading = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!reading)
        return {};

    if (!canUseProjectionForReadingStep(reading))
        return {};

    auto iter = stack.rbegin();
    while (std::next(iter) != stack.rend())
    {
        iter = std::next(iter);

        if (!typeid_cast<FilterStep *>(iter->node->step.get()) && !typeid_cast<ExpressionStep *>(iter->node->step.get()))
            break;
    }

    /// Dangling query plan node. This might be generated by StorageMerge.
    if (iter->node->step.get() == reading)
        return {};

    const auto metadata = reading->getStorageMetadata();
    const auto & projections = metadata->projections;

    std::vector<const ProjectionDescription *> normal_projections;
    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Normal)
            normal_projections.push_back(&projection);

    if (normal_projections.empty())
        return {};

    ContextPtr context = reading->getContext();
    auto it = std::find_if(
        normal_projections.begin(),
        normal_projections.end(),
        [&](const auto * projection)
        { return projection->name == context->getSettingsRef()[Setting::preferred_optimize_projection_name].value; });

    if (it != normal_projections.end())
    {
        const ProjectionDescription * preferred_projection = *it;
        normal_projections.clear();
        normal_projections.push_back(preferred_projection);
    }

    Names required_columns = reading->getAllColumnNames();

    /// If `with_parent_part_offset` is true and the required columns include `_part_offset`,
    /// we need to remap it to `_parent_part_offset`. This ensures that the projection's
    /// ActionsDAG reads from the correct column and generates `_part_offset` in the output.
    bool with_parent_part_offset = std::any_of(
        normal_projections.begin(), normal_projections.end(), [](const auto & projection) { return projection->with_parent_part_offset; });
    bool need_parent_part_offset = false;
    if (with_parent_part_offset)
    {
        /// required_columns contains unique column names
        for (auto & name : required_columns)
        {
            if (name == "_part_offset")
            {
                name = "_parent_part_offset";
                need_parent_part_offset = true;
                break;
            }
        }
    }

    QueryDAG query;
    {
        auto & child = iter->node->children[iter->next_child - 1];
        if (!query.build(*child))
            return {};

        if (need_parent_part_offset)
        {
            ActionsDAG rename_dag;
            const auto * node = &rename_dag.addInput("_parent_part_offset", std::make_shared<DataTypeUInt64>());
            node = &rename_dag.addAlias(*node, "_part_offset");
            rename_dag.getOutputs() = {node};

            if (query.dag)
                query.dag = ActionsDAG::merge(std::move(rename_dag), *std::move(query.dag));
            else
                query.dag = std::move(rename_dag);
        }

        if (query.dag)
            query.dag->removeUnusedActions();
    }

    bool force_optimize_projection = context->getSettingsRef()[Setting::force_optimize_projection];

    if (!force_optimize_projection)
    {
        /// /// Skip normal projection analysis if the query has no filter condition
        if (!query.dag || !query.filter_node)
            return {};
    }

    std::list<NormalProjectionCandidate> candidates;
    NormalProjectionCandidate * best_candidate = nullptr;

    const auto & query_info = reading->getQueryInfo();
    auto parent_reading_select_result = reading->getAnalyzedResult();
    if (!parent_reading_select_result)
        parent_reading_select_result = reading->selectRangesToRead();

    /// parent parts (non-projection result) exceeded limits
    /// construct a base AnalysisResult with all parts to analyze projection candidates
    if (!parent_reading_select_result->isUsable())
    {
        const auto & parts = reading->getParts();

        parent_reading_select_result = std::make_shared<ReadFromMergeTree::AnalysisResult>();
        parent_reading_select_result->parts_with_ranges = parts;
        parent_reading_select_result->selected_parts = parts.size();
        parent_reading_select_result->exceeded_row_limits = true;

        size_t total_marks = 0;
        size_t total_rows = 0;
        for (const auto & part : parts)
        {
            total_marks += part.data_part->getMarksCount();
            total_rows += part.data_part->rows_count;
        }
        parent_reading_select_result->selected_marks = total_marks;
        parent_reading_select_result->selected_rows = total_rows;
        parent_reading_select_result->selected_ranges = parts.size();
    }

    if (!force_optimize_projection)
    {
        /// /// Nothing to read. Ignore projections.
        if (parent_reading_select_result->parts_with_ranges.empty())
            return {};
    }

    PartitionIdToMaxBlockPtr max_added_blocks = getMaxAddedBlocks(reading);

    auto logger = getLogger("optimizeUseNormalProjections");

    auto has_all_required_columns = [&](const ProjectionDescription * projection)
    {
        for (const auto & col : required_columns)
        {
            if (!projection->sample_block.findColumnOrSubcolumnByName(col) && !projection->metadata->virtuals.has(col))
                return false;
        }

        return true;
    };

    bool optimize_use_projection_filtering = context->getSettingsRef()[Setting::optimize_use_projection_filtering];
    auto projection_query_info = query_info;
    projection_query_info.prewhere_info = nullptr;
    projection_query_info.row_level_filter = nullptr;
    if (query.dag)
        projection_query_info.filter_actions_dag = std::make_unique<ActionsDAG>(query.dag->clone());
    auto empty_mutations_snapshot = reading->getMutationsSnapshot()->cloneEmpty();
    for (const auto * projection : normal_projections)
    {
        /// Skip projections whose WHERE condition is not implied by the query's filter (Issue #74234).
        /// A projection with WHERE stores only a subset of rows, so we can only use it
        /// if the query's filter guarantees it won't need rows outside that subset.
        if (projection->where_clause_ast)
        {
            if (!doesQueryFilterImplyProjectionWhere(query.filter_node, projection->where_clause_ast, projection->query_ast, context))
            {
                LOG_DEBUG(logger,
                    "Projection {} skipped: query WHERE does not imply projection WHERE",
                    projection->name);
                continue;
            }
        }

        if (!has_all_required_columns(projection))
        {
            /// Check if projection can be used to filter parts or building projection index filters
            if (query.filter_node && optimize_use_projection_filtering)
            {
                MergeTreeDataSelectExecutor reader(reading->getMergeTreeData(), projection);
                filterPartsAndCollectProjectionCandidates(
                    *reading,
                    *projection,
                    reader,
                    empty_mutations_snapshot,
                    *parent_reading_select_result,
                    projection_query_info,
                    query.filter_node,
                    context);
            }

            continue;
        }

        auto & candidate = candidates.emplace_back();
        candidate.projection = projection;

        MergeTreeDataSelectExecutor reader(reading->getMergeTreeData(), projection);
        bool analyzed = analyzeProjectionCandidate(
            candidate,
            reader,
            empty_mutations_snapshot,
            required_columns,
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

        /// Consider projections with equal read cost only if:
        /// - `force_optimize_projection` is enabled, or
        /// - the parent reading's `selected_marks` becomes zero
        if (candidate.sum_marks > parent_reading_marks
            || (candidate.sum_marks == parent_reading_marks && parent_reading_marks > 0 && !force_optimize_projection))
        {
            stat.description = fmt::format(
                "Projection {} is usable but requires reading {} marks, which is not better than the original table with {} marks",
                candidate.projection->name,
                candidate.sum_marks,
                parent_reading_marks);

            LOG_DEBUG(logger, "{}", stat.description);
            continue;
        }

        if (best_candidate == nullptr || candidate.sum_marks < best_candidate->sum_marks)
            best_candidate = &candidate;

        /// All parts has been filtered out; no need to analyze further projections.
        if (parent_reading_marks == 0)
            break;
    }

    if (!best_candidate)
        return {};

    /// Identify projections selected as the best candidates and update their stat descriptions with appropriate logging
    for (const auto & candidate : candidates)
    {
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

    auto storage_snapshot = reading->getStorageSnapshot();
    auto proj_snapshot = std::make_shared<StorageSnapshot>(storage_snapshot->storage, best_candidate->projection->metadata);

    /// Enables PREWHERE on projections to improve read efficiency and leverage query condition cache.
    if (query.dag && query.filter_node)
    {
        projection_query_info.prewhere_info = std::make_shared<PrewhereInfo>();
        query.dag = QueryPlanOptimizations::splitAndFillPrewhereInfo(
            projection_query_info.prewhere_info,
            true, /// Always remove since this filter node is generated for projection reading
            std::move(*query.dag),
            query.filter_node->result_name,
            {query.filter_node},
            {query.filter_node});
    }

    MergeTreeDataSelectExecutor reader(reading->getMergeTreeData(), best_candidate->projection);
    auto projection_reading = reader.readFromParts(
        /*parts=*/{},
        reading->getMutationsSnapshot()->cloneEmpty(),
        required_columns,
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

    /// Only the initiator should read the projection to avoid potential data duplication.
    bool has_parent_parts = !parent_reading_select_result->parts_with_ranges.empty();
    bool should_skip_projection_reading_on_remote_replicas = reading->isParallelReadingEnabled() && !is_parallel_replicas_initiator_with_projection_support
        && has_parent_parts;
    if (!projection_reading || should_skip_projection_reading_on_remote_replicas)
    {
        Pipe pipe(std::make_shared<NullSource>(std::make_shared<const Block>(proj_snapshot->getSampleBlockForColumns(required_columns))));
        if (projection_query_info.prewhere_info)
        {
            auto filter_actions = std::make_shared<ExpressionActions>(std::move(projection_query_info.prewhere_info->prewhere_actions));
            pipe.addSimpleTransform(
                [&](const SharedHeader & header)
                {
                    return std::make_shared<FilterTransform>(
                        header,
                        filter_actions,
                        projection_query_info.prewhere_info->prewhere_column_name,
                        projection_query_info.prewhere_info->remove_prewhere_column);
                });
        }
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
    }

    if (has_parent_parts && is_parallel_replicas_initiator_with_projection_support)
        fallbackToLocalProjectionReading(projection_reading);

    if (!query_info.is_internal && context->hasQueryContext())
    {
        context->getQueryContext()->addQueryAccessInfo(Context::QualifiedProjectionName
        {
            .storage_id = reading->getMergeTreeData().getStorageID(),
            .projection_name = best_candidate->projection->name,
        });
    }

    projection_reading->setStepDescription(best_candidate->projection->name, max_step_description_length);

    auto & projection_reading_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(projection_reading)});
    auto * next_node = &projection_reading_node;

    if (query.dag)
    {
        auto & expr_or_filter_node = nodes.emplace_back();
        expr_or_filter_node.step = std::make_unique<ExpressionStep>(projection_reading_node.step->getOutputHeader(), std::move(*query.dag));
        expr_or_filter_node.children.push_back(&projection_reading_node);
        next_node = &expr_or_filter_node;
    }

    if (parent_reading_select_result->parts_with_ranges.empty())
    {
        /// All parts are taken from projection
        iter->node->children[iter->next_child - 1] = next_node;
    }
    else
    {
        const auto & main_stream = iter->node->children[iter->next_child - 1]->step->getOutputHeader();
        const auto * proj_stream = &next_node->step->getOutputHeader();

        if (auto materializing = makeMaterializingDAG(**proj_stream, *main_stream))
        {
            auto converting = std::make_unique<ExpressionStep>(*proj_stream, std::move(*materializing));
            proj_stream = &converting->getOutputHeader();
            auto & expr_node = nodes.emplace_back();
            expr_node.step = std::move(converting);
            expr_node.children.push_back(next_node);
            next_node = &expr_node;
        }

        /// Verify headers are compatible before creating the Union.
        /// If they differ (e.g., different columns due to different query DAGs being applied),
        /// skip this optimization to avoid "Block structure mismatch" errors.
        if (!blocksHaveEqualStructure(*main_stream, **proj_stream))
            return {};

        auto & union_node = nodes.emplace_back();
        SharedHeaders input_headers = {main_stream, *proj_stream};
        union_node.step = std::make_unique<UnionStep>(std::move(input_headers));
        union_node.children = {iter->node->children[iter->next_child - 1], next_node};
        iter->node->children[iter->next_child - 1] = &union_node;
    }

    /// Now the projection is used, re-do optimizeReadInOrder
    if (optimization_settings.read_in_order && typeid_cast<SortingStep *>(iter->node->step.get()))
        optimizeReadInOrder(*iter->node, nodes, optimization_settings);

    /// Here we remove last steps from stack to be able to optimize again.
    stack.resize(iter.base() - stack.begin());
    return best_candidate->projection->name;
}

}
