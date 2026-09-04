#include <Columns/ColumnConst.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}
}

namespace DB::QueryPlanOptimizations
{

/// Vector search queries have this form:
///     SELECT [...]
///     FROM tab, [...]
///     WHERE [...]      -- optional
///     ORDER BY distance_function(vec, reference_vec), [...]
///     LIMIT N
/// where
/// - distance_function is function 'L2Distance', 'cosineDistance', or 'dotProduct',
/// - vec is a column of tab (*),
/// - reference_vec is a literal of type Array(Float32 / Float64 / BFloat16 / (U)Int8 / (U)Int16 / (U)Int32 / (U)Int64)
///
/// This function extracts distance_function, reference_vec, and N from the query plan without rewriting it.
/// The extracted values are then passed to ReadFromMergeTree which can then use the vector similarity index
/// to speed up the search.
///
/// (*) Vector search only makes sense if a vector similarity index exists on vec. In the scope of this
///     function, we check that the table has a vector similarity index built on vec or an expression based
///     on vec. Other checks are left to query runtime, ReadFromMergeTree specifically.
size_t tryUseVectorSearchWithVectorIndexFirstPass(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & settings)
{
    QueryPlan::Node * node = parent_node;

    /// In the first pass, we do not modify the plan
    constexpr size_t no_layers_updated = 0;

    bool additional_filters_present = false; /// WHERE or PREWHERE

    /// Expect this query plan:
    /// LimitStep
    ///    ^
    ///    |
    /// SortingStep
    ///    ^
    ///    |
    /// ExpressionStep
    ///    ^
    ///    |
    /// (optional: FilterStep)
    ///    ^
    ///    |
    /// ReadFromMergeTree

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
    FilterStep * filter_step = nullptr;
    if (!read_from_mergetree_step)
    {
        /// Do we have a FilterStep on top of ReadFromMergeTree?
        filter_step = typeid_cast<FilterStep *>(node->step.get());
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
        additional_filters_present = true;

    if (additional_filters_present && settings.vector_search_filter_strategy == VectorSearchFilterStrategy::PREFILTER)
        return no_layers_updated; /// user explicitly wanted exact (brute-force) vector search

    /// Extract N
    size_t n = limit_step->getLimitForSorting();

    /// LIMIT ... WITH TIES can return more rows than n. The vector search optimization
    /// bounds the ANN search to exactly n candidates, so rows tied with the n-th row
    /// are never retrieved. Skip the optimization and fall back to brute force.
    if (limit_step->withTies())
        return no_layers_updated;

    /// Check that the LIMIT specified by the user isn't too big - otherwise the cost of vector search outweighs the benefit.
    if (n > settings.max_limit_for_vector_search_queries)
        return no_layers_updated;

    /// Not 100% sure but other sort types are likely not what we want
    SortingStep::Type sorting_step_type = sorting_step->getType();
    if (sorting_step_type != SortingStep::Type::Full)
        return no_layers_updated;

    /// Read ORDER BY clause
    const auto & sort_description = sorting_step->getSortDescription();
    if (sort_description.size() > 1)
        return no_layers_updated;
    const String & sort_column = sort_description.front().column_name;

    /// The ActionDAG of the ExpressionStep underneath SortingStep may have arbitrary output nodes (e.g. stuff
    /// in the SELECT clause). Find the output node which corresponds to the first ORDER BY clause.
    ActionsDAG & expression = expression_step->getExpression();
    const ActionsDAG::Node * sort_column_node = expression.tryFindInOutputs(sort_column);
    if (sort_column_node == nullptr || sort_column_node->type != ActionsDAG::ActionType::FUNCTION)
        return no_layers_updated;

    /// Extract distance_function
    const String & function_name = sort_column_node->function_base->getName();
    String distance_function;
    if (function_name == "L2Distance" || function_name == "cosineDistance" || function_name == "dotProduct")
        distance_function = function_name;
    else
        return no_layers_updated;

    /// Validate sort direction:
    /// - L2Distance and cosineDistance require ascending sort order (smaller means more similar)
    /// - dotProduct requires descending sort order (larger means more similar)
    const int sort_direction = sort_description.front().direction;
    if ((distance_function == "L2Distance" || distance_function == "cosineDistance") && sort_direction != 1)
        return no_layers_updated;
    if (distance_function == "dotProduct" && sort_direction != -1)
        return no_layers_updated;

    /// Extract stuff from the ORDER BY clause. It is expected to look like this: ORDER BY cosineDistance(vec1, [1.0, 2.0 ...])
    /// - The search column is 'vec1'.
    /// - The reference vector is [1.0, 2.0, ...].
    const ActionsDAG::NodeRawConstPtrs & sort_column_node_children = sort_column_node->children;
    VectorWithMemoryTracking<Float64> reference_vector;
    String search_column;

    for (const auto * child : sort_column_node_children)
    {
        if (child->type == ActionsDAG::ActionType::ALIAS) /// the analyzer
        {
            const auto * search_column_node = child->children.at(0);
            if (search_column_node->type == ActionsDAG::ActionType::INPUT)
                search_column = search_column_node->result_name;
        }
        else if (child->type == ActionsDAG::ActionType::INPUT) /// old analyzer
        {
            search_column = child->result_name;
            if (search_column.contains('.'))
                search_column = search_column.substr(search_column.find('.') + 1); /// admittedly fragile but hey, it's the old path ...
        }
        else if (child->type == ActionsDAG::ActionType::COLUMN)
        {
            /// Is it an Array(Float32), Array(Float64), Array(BFloat16), Array((U)Int8/16/32/64) column?
            const DataTypePtr & data_type = child->result_type;
            const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
            if (data_type_array == nullptr)
                continue;
            WhichDataType which_data_type_array_nested(data_type_array->getNestedType());
            if (!which_data_type_array_nested.isFloat() && !which_data_type_array_nested.isNativeInteger())
                continue;

            /// Read value from column
            Field field = child->column->getField();
            Field::Types::Which field_type = field.getType();
            if (field_type != Field::Types::Array)
                continue;
            Array field_array = field.safeGet<Array>();
            for (const auto & field_array_value : field_array)
            {
                Field::Types::Which field_array_value_type = field_array_value.getType();
                if (field_array_value_type != Field::Types::Float64 && field_array_value_type != Field::Types::UInt64
                    && field_array_value_type != Field::Types::Int64)
                    return no_layers_updated;
                Float64 float64 = applyVisitor(FieldVisitorConvertToNumber<Float64>(), field_array_value);
                reference_vector.push_back(float64);
            }
        }
    }

    if (search_column.empty() || reference_vector.empty())
        return no_layers_updated;

    /// Check if a vector similarity index exists on top of the search column.
    /// Multi-column indexes cannot be used
    const auto & indexes = read_from_mergetree_step->getStorageMetadata()->getSecondaryIndices();
    bool has_vector_similarity_index = false;
    for (const auto & index : indexes)
    {
        if (index.type != "vector_similarity")
            continue;

        chassert(index.expression);
        auto required_columns = index.expression->getRequiredColumns();
        if (required_columns.size() == 1 && required_columns[0] == search_column)
        {
            has_vector_similarity_index = true;
            break;
        }
    }

    if (!has_vector_similarity_index)
        return no_layers_updated;

    /// The `_distance` column is an internal virtual column populated by the vector search optimization.
    /// It must not be referenced directly in queries.
    if (read_from_mergetree_step->isVectorColumnReplaced())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "The `_distance` column is an internal virtual column of vector search and cannot be referenced directly in queries. "
            "Use the distance function (e.g. `L2Distance`, `cosineDistance`) in ORDER BY instead");

    /// All set for 2nd pass
    auto vector_search_parameters = std::make_optional<VectorSearchParameters>(search_column, distance_function, n, reference_vector, additional_filters_present, true);
    read_from_mergetree_step->setVectorSearchParameters(std::move(vector_search_parameters));

    return no_layers_updated;
}

namespace
{

/// Does the node refer to the vector column? Both analyzers are handled, same as in the first pass.
bool isSearchColumnNode(const ActionsDAG::Node * node, const String & search_column)
{
    if (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.at(0);

    if (node->type != ActionsDAG::ActionType::INPUT)
        return false;

    String name = node->result_name;
    if (name.contains('.')) /// old analyzer qualifies the name
        name = name.substr(name.find('.') + 1);

    return name == search_column;
}

/// Read a reference vector out of a COLUMN node, or return nullopt if the node is not an array literal.
std::optional<VectorWithMemoryTracking<Float64>> tryGetReferenceVector(const ActionsDAG::Node * node)
{
    if (node->type != ActionsDAG::ActionType::COLUMN || !node->column)
        return std::nullopt;

    const auto * data_type_array = typeid_cast<const DataTypeArray *>(node->result_type.get());
    if (!data_type_array)
        return std::nullopt;

    Field field = node->column->getField();
    if (field.getType() != Field::Types::Array)
        return std::nullopt;

    VectorWithMemoryTracking<Float64> reference_vector;
    for (const auto & element : field.safeGet<Array>())
    {
        if (element.getType() != Field::Types::Float64)
            return std::nullopt;
        reference_vector.push_back(element.safeGet<Float64>());
    }

    if (reference_vector.empty())
        return std::nullopt;

    return reference_vector;
}

/// True if `dag` feeds `search_column` into a function, as opposed to merely carrying it through to
/// its outputs.
///
/// This distinguishes a filter above the read that only passes the vector column along - which the
/// planner does routinely so the ORDER BY above can use it, and whose pass-through alias
/// replaceVectorColumnWithDistanceColumn() removes - from one that actually computes with it, such
/// as `WHERE length(vec) = 2`. Only the latter breaks once the column leaves the read list.
bool searchColumnFeedsIntoFunction(const ActionsDAG & dag, const String & search_column)
{
    std::unordered_map<const ActionsDAG::Node *, bool> reaches_search_column;

    auto reaches = [&](const ActionsDAG::Node * node, auto & self) -> bool
    {
        auto [it, inserted] = reaches_search_column.try_emplace(node, false);
        if (!inserted)
            return it->second;

        /// isSearchColumnNode also unwraps an ALIAS and strips the qualification the old analyzer
        /// adds, so `tab.vec` is recognised as well as `vec`.
        bool result = isSearchColumnNode(node, search_column);
        if (!result)
        {
            for (const auto * child : node->children)
            {
                if (self(child, self))
                {
                    result = true;
                    break;
                }
            }
        }

        /// try_emplace may have rehashed while recursing, so look the slot up again.
        reaches_search_column[node] = result;
        return result;
    };

    for (const auto & node : dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION && node.type != ActionsDAG::ActionType::ARRAY_JOIN)
            continue;
        for (const auto * child : node.children)
            if (reaches(child, reaches))
                return true;
    }

    return false;
}

/// Find the distance-function nodes in `dag` that the `_distance` virtual column can stand in for.
///
/// A node qualifies only if it computes exactly the same thing the vector index already returned,
/// i.e. the same distance function over the same vector column and the *same* reference vector as
/// the ORDER BY. A query may well filter on a different reference vector than it sorts by, and
/// `_distance` only holds the distances belonging to the ORDER BY, so substituting it there would
/// silently return wrong results.
///
/// Returns an empty result if the vector column is reachable in any other way, because then the
/// column still has to be read and there is nothing to gain.
ActionsDAG::NodeRawConstPtrs findDistanceNodesReplaceableByDistanceColumn(
    const ActionsDAG & dag, const VectorSearchParameters & parameters)
{
    ActionsDAG::NodeRawConstPtrs distance_nodes;
    std::unordered_set<const ActionsDAG::Node *> vector_column_users;

    for (const auto & node : dag.getNodes())
    {
        for (const auto * child : node.children)
        {
            if (isSearchColumnNode(child, parameters.column))
                vector_column_users.insert(&node);
        }
    }

    for (const auto * node : vector_column_users)
    {
        if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
            return {};

        if (node->function_base->getName() != parameters.distance_function)
            return {};

        if (node->children.size() != 2)
            return {};

        /// One child is the vector column, the other must be a literal equal to the ORDER BY reference vector.
        const auto * reference_child = isSearchColumnNode(node->children.at(0), parameters.column)
            ? node->children.at(1)
            : node->children.at(0);

        auto reference_vector = tryGetReferenceVector(reference_child);
        if (!reference_vector.has_value())
            return {};

        if (reference_vector->size() != parameters.reference_vector.size()
            || !std::equal(reference_vector->begin(), reference_vector->end(), parameters.reference_vector.begin()))
            return {};

        distance_nodes.push_back(node);
    }

    return distance_nodes;
}

/// Rewrite `distance_nodes` (interior nodes of `dag`) to read the `_distance` virtual column instead
/// of recomputing the distance. Each node keeps its name and result type, so its parents are
/// unaffected: it simply becomes an alias of the value the vector index handed us for free.
void replaceDistanceNodesWithDistanceColumn(
    ActionsDAG & dag,
    const ActionsDAG::NodeRawConstPtrs & distance_nodes,
    const VectorSearchParameters & parameters,
    const ContextPtr & context)
{
    const auto * distance_input = &dag.addInput("_distance", std::make_shared<DataTypeFloat32>());

    /// usearch returns L2 *squared* to avoid repeated sqrt computations.
    const auto * distance_node = distance_input;
    if (parameters.distance_function == "L2Distance")
    {
        auto sqrt_function = FunctionFactory::instance().get("sqrt", context);
        distance_node = &dag.addFunction(sqrt_function, {distance_node}, {});
    }

    for (const auto * node_to_replace : distance_nodes)
    {
        /// `_distance` is always Float32 while the distance function may return Float64 (bug #85514),
        /// so cast to the type the node already had rather than changing it under its parents.
        const auto * replacement = distance_node;
        if (!replacement->result_type->equals(*node_to_replace->result_type))
            replacement = &dag.addCast(*replacement, node_to_replace->result_type, "_CAST_distance_prewhere", context);

        /// Mutating nodes in place is how the other query plan optimizations rewire a DAG
        /// (see filterPushDown.cpp and mergeFilterIntoJoinCondition.cpp).
        auto * mutable_node = const_cast<ActionsDAG::Node *>(node_to_replace);
        mutable_node->type = ActionsDAG::ActionType::ALIAS;
        mutable_node->children = {replacement};
        mutable_node->function_base = nullptr;
        mutable_node->function = nullptr;
        mutable_node->is_function_compiled = false;
    }

    /// The vector column is usually also an output of the PREWHERE actions, passed through so that
    /// the ORDER BY above can compute the distance from it. Swap that pass-through for `_distance`:
    /// only the outputs of these actions survive into the step's header, so `_distance` has to be
    /// among them for the ORDER BY (rewritten the same way) to find it. Dropping the vector column
    /// has to be explicit as well, since outputs are roots that removeUnusedActions() would keep
    /// alive, and not reading that column is the point of this optimization.
    auto & outputs = dag.getOutputs();
    std::erase_if(outputs, [&](const auto * output) { return isSearchColumnNode(output, parameters.column); });

    if (std::ranges::find(outputs, distance_input) == outputs.end())
        outputs.push_back(distance_input);

    dag.removeUnusedActions();
}

}

bool optimizeVectorSearchWithVectorIndexSecondPass(QueryPlan::Node & /*root*/, Stack & stack, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & settings)
{
    /// QueryPlan::Node * node = parent_node;

    /// Expect this query plan:
    /// LimitStep
    ///    ^
    ///    |
    /// SortingStep
    ///    ^
    ///    |
    /// ExpressionStep
    ///    ^
    ///    |
    /// (FilterStep, optional) Or (ExpressionStep, if prewhere optimization)
    ///    ^
    ///    |
    /// ReadFromMergeTree
    ///
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
        /// Do we have a FilterStep Or ExpressionStep (PREWHERE) on top of ReadFromMergeTree?
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

    /// Check if first pass has indicated vector index usage
    auto vector_search_parameters = read_from_mergetree_step->getVectorSearchParameters();
    if (!vector_search_parameters.has_value())
        return false;

    /// The `_distance` column is an internal virtual column populated by the vector search optimization.
    /// It must not be referenced directly in queries.
    if (read_from_mergetree_step->isVectorColumnReplaced())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "The `_distance` column is an internal virtual column of vector search and cannot be referenced directly in queries. "
            "Use the distance function (e.g. `L2Distance`, `cosineDistance`) in ORDER BY instead");

    /// The optimization is only possible if the index-analyis and query execution
    /// are both executed on the same node.
    if (read_from_mergetree_step->isParallelReadingFromReplicas())
        return false;

    /// An explicit PREWHERE generally disables the optimization. The PREWHERE optimization
    /// is slightly at odds with vector search optimizations. There are two optimizations in vector
    /// search -
    /// 1. Lookup the vector index and shortlist a handful of granules containing neighbours.
    /// 2. Apply the candidate-row filter from the vector index before distance
    ///    computation for rescoring queries, or use `_distance` from the index
    ///    for non-rescoring queries.
    /// Thus, explicit or implicit PREWHERE after above two optimizations does not bring additional benefit. Also,
    /// the PREWHERE filter implementation conflicts with the vector-search candidate-row filter. If explicit PREWHERE
    /// is requested, we turn the vector-search optimization off. If there is a WHERE clause and even with
    /// optimize_move_to_prewhere = 1, we retain vector-search optimization and disable the implicit PREWHERE
    /// optimization. (check optimizePrewhere.cpp)
    ///
    /// The one exception is a PREWHERE that only reaches the vector column through the very distance
    /// function the ORDER BY sorts by, e.g.
    ///     PREWHERE cosineDistance(vec, reference) < 0.5 ORDER BY cosineDistance(vec, reference)
    /// There the filter recomputes a distance the vector index already returned, so the whole (heavy)
    /// vector column gets read only to derive a number we were handed for free. Such a PREWHERE can be
    /// rewritten onto the `_distance` virtual column, which keeps the optimization and drops the vector
    /// column from the read list. Any other use of the vector column keeps the bail-out below.
    ActionsDAG::NodeRawConstPtrs prewhere_distance_nodes;
    if (const auto & prewhere_info = read_from_mergetree_step->getPrewhereInfo())
    {
        if (settings.vector_search_with_rescoring)
            return false;

        /// FINAL may add PK-overlapping ranges after vector index analysis, so the vector row hints
        /// describe only the pre-FINAL candidates. The rescoring row filter is disabled under FINAL
        /// for exactly this reason (see apply_row_filter_for_rescoring below), but the rewrite keeps
        /// the hints active, so an explicit PREWHERE under FINAL has to keep the bail-out.
        if (read_from_mergetree_step->isQueryWithFinal())
            return false;

        /// The rewrite drops the vector column from the read list, so a step above the read that
        /// computes something from it would reference a column that is no longer produced, e.g.
        ///     PREWHERE cosineDistance(vec, reference) < 0.5 WHERE length(vec) = 2
        /// Merely carrying the column through to the outputs is fine and common - the planner keeps
        /// it available for the ORDER BY above - and replaceVectorColumnWithDistanceColumn() drops
        /// that pass-through alias. So a filter such as `WHERE id % 2 = 0` stays eligible, and only
        /// a filter that feeds the column into a function bails out.
        if (filter_step || prewhere_expression_step)
        {
            const ActionsDAG & dag_above_read
                = prewhere_expression_step ? prewhere_expression_step->getExpression() : filter_step->getExpression();
            if (searchColumnFeedsIntoFunction(dag_above_read, vector_search_parameters.value().column))
                return false;
        }

        prewhere_distance_nodes
            = findDistanceNodesReplaceableByDistanceColumn(prewhere_info->prewhere_actions, vector_search_parameters.value());
        if (prewhere_distance_nodes.empty())
            return false;
    }

    /// Not 100% sure but other sort types are likely not what we want
    SortingStep::Type sorting_step_type = sorting_step->getType();
    if (sorting_step_type != SortingStep::Type::Full)
        return false;

    /// Read ORDER BY clause
    const auto & sort_description = sorting_step->getSortDescription();
    if (sort_description.size() > 1)
        return false;
    const String & sort_column = sort_description.front().column_name;

    /// The Usearch index calculates and returns (at index granule level) the row ID(s) + corresponding distances for the top-N most similar
    /// matches to the given reference vector. This creates a mismatch to the granule-based interface of skip indexes in ClickHouse.
    /// To bridge this gap, MergeTreeVectorSimilarityIndex historically extrapolated the result from USearch to granule level. This caused
    /// vector search queries to slow down as ClickHouse subsequently loaded the returned granules from disk and applied the distance
    /// function to _all_ contained rows (e.g. 8191 out of 8192 rows). This is maximally silly but we decided to give this mode the fancy
    /// name "rescoring mode" and turn a weakness into a strength (in terms of feature completeness).
    ///
    /// A more natural way (called "optimized plan" below) goes like this: We rewrite the query plan and
    /// - remove the vector_column from the read list in ReadFromMergeTreeStep,
    /// - remove the L2Distance(...) function OUTPUT node from the expressions ActionsDAG,
    /// - adds back the L2Distance(...) as ALIAS to a "_distance" INPUT node.
    /// "_distance" node is a virtual column.
    /// The row IDs + distances returned from Usearch are bundled as RangesInDataPartHints and reach the MergeTreeRangeReader.
    /// MergeTreeRangeReader::executeActionsForReadHints() is the key - it creates and populates a filter that is True only for the exact
    /// row IDs/part offsets returned by vector search and the routine populates a virtual column named _distance for distance corresponding
    /// to the exact Row ID. The filter is then applied on the columns in the read list.

    ActionsDAG & expression = expression_step->getExpression();

    bool optimize_plan = !settings.vector_search_with_rescoring;
    /// FINAL may add PK-overlapping ranges after vector index analysis. In that case,
    /// vector row hints only describe the original candidates and must not filter
    /// rows added for the final merge.
    bool apply_row_filter_for_rescoring = settings.vector_search_with_rescoring && !read_from_mergetree_step->isQueryWithFinal();
    if (optimize_plan)
    {
        auto search_column = vector_search_parameters.value().column;
        for (const auto & output : expression.getOutputs())
        {
            /// If the SELECT clause contains the vector column (rare situation), skip the optimization.
            /// Multiple forms of analyzer nodes to handle.
            if (output->result_name == search_column ||
                (output->type == ActionsDAG::ActionType::ALIAS && output->children.at(0)->result_name == search_column) ||
                (output->result_name.contains('.') && output->result_name.ends_with("." + search_column)))
            {
                optimize_plan = false;
                break;
            }
        }

        if (optimize_plan)
        {
            auto analyzed_result = read_from_mergetree_step->getAnalyzedResult();
            analyzed_result = analyzed_result ? analyzed_result : read_from_mergetree_step->selectRangesToRead();

            /// Only if full parts were candidates and vector index was used to fetch
            /// distances, we can proceed with the optimization.
            for (const auto & part_with_ranges : analyzed_result->parts_with_ranges)
            {
                if (!part_with_ranges.ranges.empty())
                {
                    if (!part_with_ranges.read_hints.vector_search_results.has_value() ||
                        !part_with_ranges.read_hints.vector_search_results.value().distances.has_value())
                    {
                        optimize_plan = false;
                        break;
                    }
                }
            }
        }

        if (optimize_plan)
        {
            /// Rewrite an explicit PREWHERE onto `_distance` before the read list changes: the header is
            /// recomputed from both, so they have to be swapped together. `_distance` is filled in
            /// MergeTreeRangeReader::startReadingChain(), which runs before the prewhere actions.
            PrewhereInfoPtr new_prewhere_info;
            if (!prewhere_distance_nodes.empty())
            {
                new_prewhere_info = std::make_shared<PrewhereInfo>(read_from_mergetree_step->getPrewhereInfo()->clone());
                auto nodes_to_replace = findDistanceNodesReplaceableByDistanceColumn(
                    new_prewhere_info->prewhere_actions, vector_search_parameters.value());
                replaceDistanceNodesWithDistanceColumn(
                    new_prewhere_info->prewhere_actions,
                    nodes_to_replace,
                    vector_search_parameters.value(),
                    read_from_mergetree_step->getContext());
            }

            /// Remove the physical vector column from ReadFromMergeTreeStep, add virtual "_distance" column
            read_from_mergetree_step->replaceVectorColumnWithDistanceColumn(search_column, new_prewhere_info);

            /// Bug #85514: cosineDistance/L2Distance can have return types Float64 or Float32, depending on the
            /// input types but the "_distance" column is always of type Float32. Add a CAST if needed.
            ///
            /// The sort column node will be removed first from the DAG, hence remember the datatype of final result
            const ActionsDAG::Node * sort_column_node = expression.tryFindInOutputs(sort_column); /// "cosine/L2Distance(..., ...)"
            const auto result_type = sort_column_node->result_type;

            /// Now replace the "cosineDistance(vec, [1.0, 2.0...])" node in the DAG by the "_distance" node
            expression.removeUnusedResult(sort_column); /// Removes the OUTPUT cosineDistance(...) FUNCTION Node
            expression.removeUnusedActions(); /// Removes the vector column INPUT node (it is no longer needed)
            const auto * distance_node = &expression.addInput("_distance",std::make_shared<DataTypeFloat32>());

            const bool need_sqrt = vector_search_parameters->distance_function == "L2Distance";
            if (need_sqrt) /// usearch returns L2 squared distance to save repeated sqrt computations.
            {
                auto sqrt_function = FunctionFactory::instance().get("sqrt", read_from_mergetree_step->getContext());
                distance_node = &expression.addFunction(sqrt_function, {distance_node}, {});
            }

            if (!distance_node->result_type->equals(*result_type))
                distance_node = &expression.addCast(*distance_node, result_type, "_CAST_distance", nullptr);

            const auto * new_output = &expression.addAlias(*distance_node, sort_column);
            expression.getOutputs().push_back(new_output);

            /// Need to do same removal of the vector column from the Filter step
            if (filter_or_prewhere_node)
            {
                ActionsDAG & filter_expression = prewhere_expression_step ? prewhere_expression_step->getExpression() : filter_step->getExpression();
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
                    output_result_to_delete = search_column; /// old analyzer
                filter_expression.removeUnusedResult(output_result_to_delete);
                filter_expression.removeUnusedActions();

                /// Update the node with new Step
                QueryPlanStepPtr new_step;
                if (prewhere_expression_step)
                    new_step = std::make_unique<ExpressionStep>(read_from_mergetree_step->getOutputHeader(), std::move(filter_expression));
                else
                    new_step = std::make_unique<FilterStep>(read_from_mergetree_step->getOutputHeader(), std::move(filter_expression), filter_step->getFilterColumnName(), filter_step->removesFilterColumn());
                new_step->setStepDescription(*filter_or_prewhere_node->step);
               filter_or_prewhere_node->step = std::move(new_step);
            }
        }

        /// Update the node with new Step
        auto new_step = std::make_unique<ExpressionStep>(
            filter_or_prewhere_node ? filter_or_prewhere_node->step.get()->getOutputHeader() : read_from_mergetree_step->getOutputHeader(), std::move(expression));
        new_step->setStepDescription(*expression_node->step);
        expression_node->step = std::move(new_step);

        /// The SortingStep's input header must reflect the new ExpressionStep output header
        /// (which now has _distance consumed and L2Distance(...) produced via ALIAS).
        sorting_step->updateInputHeader(expression_node->step->getOutputHeader());
    }

    if (apply_row_filter_for_rescoring)
    {
        auto analyzed_result = read_from_mergetree_step->getAnalyzedResult();
        analyzed_result = analyzed_result ? analyzed_result : read_from_mergetree_step->selectRangesToRead();

        bool can_apply_row_filter = analyzed_result != nullptr;
        if (can_apply_row_filter)
        {
            for (const auto & part_with_ranges : analyzed_result->parts_with_ranges)
            {
                if (!part_with_ranges.ranges.empty() && !part_with_ranges.read_hints.vector_search_results.has_value())
                {
                    can_apply_row_filter = false;
                    break;
                }
            }
        }

        if (can_apply_row_filter)
        {
            for (auto & part_with_ranges : analyzed_result->parts_with_ranges)
            {
                if (!part_with_ranges.ranges.empty())
                    part_with_ranges.read_hints.use_vector_search_result_filter = true;
            }
        }
        else
        {
            apply_row_filter_for_rescoring = false;
        }
    }

    const bool vector_optimization_applied = optimize_plan || apply_row_filter_for_rescoring;

    /// Both vector-search optimizations narrow each granule to the candidate rows returned by the
    /// vector index before the WHERE/PREWHERE filter runs. The query condition cache key encodes
    /// only the filter predicate, so a granule whose candidates all fail the filter would be
    /// recorded as "the predicate matches nothing" and a later ordinary query with the same
    /// predicate would skip it and lose rows. Same reasoning as the SAMPLE exclusion in
    /// ReadFromMergeTree::initializePipeline. Reading and index analysis are already excluded for
    /// vector search (MergeTreeDataSelectExecutor::filterPartsByQueryConditionCache and
    /// ReadFromMergeTree::selectRangesToRead); this covers the remaining write paths.
    if (vector_optimization_applied)
        read_from_mergetree_step->disableQueryConditionCache();

    if (!vector_optimization_applied && settings.optimize_prewhere && filter_step)
        optimizePrewhere(*filter_or_prewhere_node, settings.remove_unused_columns, false);

    return vector_optimization_applied;
}

}
