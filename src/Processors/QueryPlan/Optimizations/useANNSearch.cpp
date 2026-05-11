#include <Columns/ColumnConst.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/ANNSearchUtils.h>
#include <Storages/MergeTree/MergeTreeIndexANN.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/StorageMergeTree.h>
#include "config.h"
#if USE_DISKANN
#    include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#    include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>
#endif

namespace DB::QueryPlanOptimizations
{

/// First-pass identifier for the table-level ANN (DiskANN) index search shape.
///
/// The recognised shape is:
///     SELECT [...]
///     FROM tab [, ...]
///     WHERE [...]           -- optional
///     ORDER BY L2Distance(vec, reference) ASC   -- or cosineDistance / dotProduct
///     LIMIT N
/// where `vec` is an `Array(Float32)` column that is covered by a `TYPE ann(...)`
/// secondary index in the table metadata.
///
/// On match, the identified parameters (column, distance function, limit, reference
/// vector) are attached to the underlying ReadFromMergeTree step through
/// `setANNSearchParameters`. The plan structure is not modified here; the second
/// pass does the DAG rewrite that substitutes the `_distance` virtual column for
/// the distance function call.
size_t tryUseANNSearch(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & settings)
{
    QueryPlan::Node * node = parent_node;

    /// First pass never mutates the plan.
    constexpr size_t no_layers_updated = 0;

    bool additional_filters_present = false;

    /// Expected plan shape (FilterStep is optional):
    ///     LimitStep -> SortingStep -> ExpressionStep -> [FilterStep ->] ReadFromMergeTree
    auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
    if (!limit_step)
        return no_layers_updated;

    if (node->children.size() != 1)
        return no_layers_updated;
    node = node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(node->step.get());
    if (!sorting_step)
        return no_layers_updated;

    if (node->children.size() != 1)
        return no_layers_updated;
    node = node->children.front();
    auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (!expression_step)
        return no_layers_updated;

    if (node->children.size() != 1)
        return no_layers_updated;
    node = node->children.front();
    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_mergetree_step)
    {
        /// FilterStep directly above ReadFromMergeTree.
        auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
        if (!filter_step)
            return no_layers_updated;
        if (node->children.size() != 1)
            return no_layers_updated;
        node = node->children.front();
        read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
        if (!read_from_mergetree_step)
            return no_layers_updated;
        additional_filters_present = true;
    }

    if (const auto & prewhere_info = read_from_mergetree_step->getPrewhereInfo())
    {
        (void)prewhere_info;
        additional_filters_present = true;
    }

    /// Respect user intent for brute-force (exact) search.
    if (additional_filters_present && settings.vector_search_filter_strategy == VectorSearchFilterStrategy::PREFILTER)
        return no_layers_updated;

    /// Extract N (user LIMIT).
    size_t n = limit_step->getLimitForSorting();
    if (n > settings.max_limit_for_vector_search_queries)
        return no_layers_updated;

    /// Only full sorting qualifies; partial / merge variants mean a different operator above.
    if (sorting_step->getType() != SortingStep::Type::Full)
        return no_layers_updated;

    const auto & sort_description = sorting_step->getSortDescription();
    if (sort_description.size() != 1)
        return no_layers_updated;
    const String & sort_column = sort_description.front().column_name;

    /// Locate the node in the expression DAG that produces the ORDER BY key.
    ActionsDAG & expression = expression_step->getExpression();
    const ActionsDAG::Node * sort_column_node = expression.tryFindInOutputs(sort_column);
    if (sort_column_node == nullptr || sort_column_node->type != ActionsDAG::ActionType::FUNCTION)
        return no_layers_updated;

    /// Extract distance_function.
    const String & function_name = sort_column_node->function_base->getName();
    String distance_function;
    if (function_name == "L2Distance" || function_name == "cosineDistance" || function_name == "dotProduct")
        distance_function = function_name;
    else
        return no_layers_updated;

    /// L2Distance / cosineDistance: smaller is better, so expect ASC.
    /// dotProduct: larger is better, so expect DESC.
    const int sort_direction = sort_description.front().direction;
    if ((distance_function == "L2Distance" || distance_function == "cosineDistance") && sort_direction != 1)
        return no_layers_updated;
    if (distance_function == "dotProduct" && sort_direction != -1)
        return no_layers_updated;

    /// Walk the FUNCTION children to recover the search column name and the reference vector literal.
    const ActionsDAG::NodeRawConstPtrs & sort_column_node_children = sort_column_node->children;
    std::vector<Float64> reference_vector;
    String search_column;

    for (const auto * child : sort_column_node_children)
    {
        if (child->type == ActionsDAG::ActionType::ALIAS)
        {
            const auto * search_column_node = child->children.at(0);
            if (search_column_node->type == ActionsDAG::ActionType::INPUT)
                search_column = search_column_node->result_name;
        }
        else if (child->type == ActionsDAG::ActionType::INPUT)
        {
            search_column = child->result_name;
            if (search_column.contains('.'))
                search_column = search_column.substr(search_column.find('.') + 1);
        }
        else if (child->type == ActionsDAG::ActionType::COLUMN)
        {
            /// Is it an Array(Float32) / Array(Float64) / Array(BFloat16) constant?
            const DataTypePtr & data_type = child->result_type;
            const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
            if (data_type_array == nullptr)
                continue;
            DataTypePtr nested = data_type_array->getNestedType();
            const auto * nested_f64 = typeid_cast<const DataTypeFloat64 *>(nested.get());
            const auto * nested_f32 = typeid_cast<const DataTypeFloat32 *>(nested.get());
            const auto * nested_bf16 = typeid_cast<const DataTypeBFloat16 *>(nested.get());
            if (nested_f64 == nullptr && nested_f32 == nullptr && nested_bf16 == nullptr)
                continue;

            const ColumnPtr & column = child->column;
            const auto * literal_column = typeid_cast<const ColumnConst *>(column.get());
            if (!literal_column || literal_column->size() != 1)
                continue;
            Field field;
            literal_column->get(0, field);
            if (field.getType() != Field::Types::Array)
                continue;
            Array field_array = field.safeGet<Array>();
            for (const auto & v : field_array)
            {
                if (v.getType() != Field::Types::Float64)
                    return no_layers_updated;
                reference_vector.push_back(v.safeGet<Float64>());
            }
        }
    }

    if (search_column.empty() || reference_vector.empty())
        return no_layers_updated;

    /// A single-column `ann` secondary index on `search_column` is required.
    const auto & indexes = read_from_mergetree_step->getStorageMetadata()->getSecondaryIndices();
    bool has_ann_index = false;
    for (const auto & index : indexes)
    {
        if (index.type != "ann")
            continue;
        chassert(index.expression);
        auto required_columns = index.expression->getRequiredColumns();
        if (required_columns.size() == 1 && required_columns[0] == search_column)
        {
            has_ann_index = true;
            break;
        }
    }

    if (!has_ann_index)
        return no_layers_updated;

    /// The query distance function must match the index metric, otherwise routing the query
    /// through the ANN index would silently return semantically wrong results (e.g. an L2
    /// graph cannot answer a cosine query). `dotProduct` has no corresponding metric in the
    /// DDL today and is therefore always rejected. On mismatch we fall back to a full scan,
    /// which is consistent with how other secondary indexes behave when they cannot help.
    /// `metric` IDs are the values from `METRIC_TO_ID` in `MergeTreeIndexANN.cpp` — kept as
    /// plain integers here to avoid pulling in the `USE_DISKANN`-gated `DiskANNMetric` enum.
    constexpr UInt8 ann_metric_l2 = 0;
    constexpr UInt8 ann_metric_cosine = 1;
    ANNIndexDefinition ann_definition;
    if (!extractANNDefinitionFromMetadata(*read_from_mergetree_step->getStorageMetadata(), ann_definition))
        return no_layers_updated;
    const bool metric_matches_distance_function =
        (distance_function == "L2Distance"     && ann_definition.shape.metric == ann_metric_l2) ||
        (distance_function == "cosineDistance" && ann_definition.shape.metric == ann_metric_cosine);
    if (!metric_matches_distance_function)
        return no_layers_updated;

    /// Stash parameters on the read step; the second pass picks them up.
    ANNSearchParameters params;
    params.column = search_column;
    params.distance_function = distance_function;
    params.limit = n;
    params.reference_vector = std::move(reference_vector);
    params.rescoring_factor = 1;
    params.additional_filters_present = additional_filters_present;
    params.force_brute_force = settings.ann_search_force_brute_force;
    params.search_list_size = settings.ann_search_list_size;
    params.beam_width = settings.ann_beam_width;

#if USE_DISKANN
    /// Borrow a searcher from the first active group so that the unindexed-parts dispatch can
    /// reach the index's stateless distance kernel via `IANNIndexSearcher::computeDistances`.
    /// The kernel is determined by `(metric, dim)` and is independent of which group provides
    /// the searcher; any active group will do. If no group is active yet (fresh table or build
    /// hasn't started), the field stays null and the reader silently falls back to the SQL
    /// distance function.
    if (auto manager = read_from_mergetree_step->getMergeTreeData().getANNIndexManager())
    {
        if (auto snapshot = manager->getActiveSnapshot(); snapshot && !snapshot->groups.empty())
            params.metric_kernel = snapshot->groups.front()->getSearcher();
    }
#endif

    LOG_INFO(
        getLogger("ANNSearchOptimizer"),
        "Table-level ANN search matched: column={}, distance_function={}, k={}, dim={}, "
        "additional_filters_present={}, force_brute_force={}, has_metric_kernel={}",
        params.column,
        params.distance_function,
        params.limit,
        params.reference_vector.size(),
        params.additional_filters_present,
        params.force_brute_force,
        params.metric_kernel != nullptr);

    read_from_mergetree_step->setANNSearchParameters(std::make_optional<ANNSearchParameters>(std::move(params)));

    return no_layers_updated;
}

/// Second pass for the table-level ANN index path.
///
/// The first pass tagged ReadFromMergeTree with `ANNSearchParameters`. By the time this runs,
/// `selectRangesToRead` (invoked from inside ReadFromMergeTree) has already populated
/// `parts_with_ranges[*].read_hints.ann_search_results`.
///
/// Unlike the vector-similarity path, the ANN path does not support a rescoring fallback: the
/// rewrite is unconditional. Indexed parts carry `_distance` pre-computed in the read hints;
/// unindexed parts compute `_distance` from the vector column at row-read time (the reader
/// picks the code path based on the `ann_search_results` tri-state). In both cases the output
/// of the ReadFromMergeTree step has `_distance` instead of the vector column, and that is what
/// the rewritten ExpressionStep consumes.
bool optimizeANNSearchSecondPass(QueryPlan::Node & /*root*/, Stack & stack, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & /*settings*/)
{
    /// Expected plan shape (same as the first pass):
    ///     LimitStep -> SortingStep -> ExpressionStep -> [FilterStep | ExpressionStep(PREWHERE) ->] ReadFromMergeTree
    const auto & frame = stack.back();

    if (frame.node->children.size() != 1)
        return false;

    QueryPlan::Node * node = frame.node;

    auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
    if (!limit_step)
        return false;

    if (node->children.size() != 1)
        return false;
    node = node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(node->step.get());
    if (!sorting_step)
        return false;

    if (node->children.size() != 1)
        return false;
    node = node->children.front();
    auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (!expression_step)
        return false;

    if (node->children.size() != 1)
        return false;

    auto * expression_node = node;
    node = node->children.front();

    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());

    FilterStep * filter_step = nullptr;
    ExpressionStep * prewhere_expression_step = nullptr;
    QueryPlan::Node * filter_or_prewhere_node = nullptr;
    if (!read_from_mergetree_step)
    {
        filter_step = typeid_cast<FilterStep *>(node->step.get());
        prewhere_expression_step = typeid_cast<ExpressionStep *>(node->step.get());
        if (!filter_step && !prewhere_expression_step)
            return false;

        if (node->children.size() != 1)
            return false;

        filter_or_prewhere_node = node;
        node = node->children.front();

        read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
        if (!read_from_mergetree_step)
            return false;
    }

    /// The first pass must have stamped parameters on this step.
    auto ann_search_parameters = read_from_mergetree_step->getANNSearchParameters();
    if (!ann_search_parameters.has_value())
        return false;

    /// The rewrite only makes sense when index analysis + execution happen on the same node.
    if (read_from_mergetree_step->isParallelReadingFromReplicas())
        return false;

    /// Explicit PREWHERE conflicts with the ANN read path (the range reader owns the filter).
    if (const auto & prewhere_info = read_from_mergetree_step->getPrewhereInfo())
    {
        (void)prewhere_info;
        return false;
    }

    if (sorting_step->getType() != SortingStep::Type::Full)
        return false;

    const auto & sort_description = sorting_step->getSortDescription();
    if (sort_description.size() != 1)
        return false;
    const String & sort_column = sort_description.front().column_name;

    ActionsDAG & expression = expression_step->getExpression();

    /// If the SELECT clause references the vector column itself, the rewrite cannot substitute
    /// `_distance` for it. Nothing meaningful to do.
    const auto & search_column = ann_search_parameters.value().column;
    for (const auto & output : expression.getOutputs())
    {
        if (output->result_name == search_column
            || (output->type == ActionsDAG::ActionType::ALIAS && output->children.at(0)->result_name == search_column)
            || (output->result_name.contains('.') && output->result_name.ends_with("." + search_column)))
        {
            return false;
        }
    }

    /// Remove the vector column from the read list, add `_distance` as a virtual column.
    read_from_mergetree_step->replaceVectorColumnWithDistanceColumn(search_column);

    /// The ANN indexed path matches hits by (block_number, block_offset), so the range reader
    /// needs both virtual columns; make sure they are part of the read list before the Pool
    /// turns this into per-part column sets.
    read_from_mergetree_step->ensureBlockNumberAndOffsetColumns();

    /// Preserve the result type of the original distance function so the top-level
    /// expression still emits the same type (e.g. `cosineDistance` may return Float32 or
    /// Float64 depending on input). `_distance` itself is always Float32; insert a CAST
    /// when the original type is wider/different.
    const ActionsDAG::Node * sort_column_node = expression.tryFindInOutputs(sort_column);
    const bool need_cast = !WhichDataType(sort_column_node->result_type).isFloat32();
    const auto result_type = sort_column_node->result_type;

    /// Replace `<distance_function>(vec, ref)` with an INPUT on `_distance` (+ optional CAST) + ALIAS.
    expression.removeUnusedResult(sort_column);
    expression.removeUnusedActions();
    const auto * distance_node = &expression.addInput("_distance", std::make_shared<DataTypeFloat32>());
    if (need_cast)
        distance_node = &expression.addCast(*distance_node, result_type, "_CAST_distance", nullptr);

    const auto * new_output = &expression.addAlias(*distance_node, sort_column);
    expression.getOutputs().push_back(new_output);

    /// If a Filter or PREWHERE step sits between the rewritten expression and the read step,
    /// remove the vector column from its DAG as well — it no longer flows through.
    if (filter_or_prewhere_node)
    {
        ActionsDAG & filter_expression = prewhere_expression_step
            ? prewhere_expression_step->getExpression()
            : filter_step->getExpression();
        String output_result_to_delete;
        for (const auto * output_node : filter_expression.getOutputs())
        {
            if (output_node->type == ActionsDAG::ActionType::ALIAS && output_node->children.at(0)->result_name == search_column)
            {
                output_result_to_delete = output_node->result_name;
                break;
            }
        }
        if (output_result_to_delete.empty())
            output_result_to_delete = search_column; /// old analyzer fallback
        filter_expression.removeUnusedResult(output_result_to_delete);
        filter_expression.removeUnusedActions();

        QueryPlanStepPtr new_step;
        if (prewhere_expression_step)
            new_step = std::make_unique<ExpressionStep>(read_from_mergetree_step->getOutputHeader(), std::move(filter_expression));
        else
            new_step = std::make_unique<FilterStep>(read_from_mergetree_step->getOutputHeader(), std::move(filter_expression), filter_step->getFilterColumnName(), filter_step->removesFilterColumn());
        new_step->setStepDescription(*filter_or_prewhere_node->step);
        filter_or_prewhere_node->step = std::move(new_step);
    }

    auto new_step = std::make_unique<ExpressionStep>(
        filter_or_prewhere_node ? filter_or_prewhere_node->step.get()->getOutputHeader() : read_from_mergetree_step->getOutputHeader(),
        std::move(expression));
    new_step->setStepDescription(*expression_node->step);
    expression_node->step = std::move(new_step);

    sorting_step->updateInputHeader(expression_node->step->getOutputHeader());

    return true;
}

}
