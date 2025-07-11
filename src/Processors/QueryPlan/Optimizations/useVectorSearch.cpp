#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB::QueryPlanOptimizations
{

/// Vector search queries have this form:
///     SELECT [...]
///     FROM tab, [...]
///     WHERE [...]      -- optional
///     ORDER BY distance_function(vec, reference_vec), [...]
///     LIMIT N
/// where
/// - distance_function is function 'L2Distance' or 'cosineDistance',
/// - vec is a column of tab (*),
/// - reference_vec is a literal of type Array(Float32/Float64)
///
/// This function extracts distance_function, reference_vec, and N from the query plan without rewriting it.
/// The extracted values are then passed to ReadFromMergeTree which can then use the vector similarity index
/// to speed up the search.
///
/// (*) Vector search only makes sense if a vector similarity index exists on vec. In the scope of this
///     function, we don't care. That check is left to query runtime, ReadFromMergeTree specifically.
size_t tryUseVectorSearch(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & settings)
{
    QueryPlan::Node * node = parent_node;

    /// If the optimization pass did not change ReadFromMergeTree & Expression steps in the query plan.
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
    if (function_name == "L2Distance" || function_name == "cosineDistance")
        distance_function = function_name;
    else
        return no_layers_updated;

    /// Extract stuff from the ORDER BY clause. It is expected to look like this: ORDER BY cosineDistance(vec1, [1.0, 2.0 ...])
    /// - The search column is 'vec1'.
    /// - The reference vector is [1.0, 2.0, ...].
    const ActionsDAG::NodeRawConstPtrs & sort_column_node_children = sort_column_node->children;
    std::vector<Float64> reference_vector;
    String search_column;

    for (const auto * child : sort_column_node_children)
    {
        if (child->type == ActionsDAG::ActionType::ALIAS) /// new analyzer
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
            /// Is it an Array(Float32), Array(Float64) or Array(BFloat16) column?
            const DataTypePtr & data_type = child->result_type;
            const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
            if (data_type_array == nullptr)
                continue;
            DataTypePtr data_type_array_nested = data_type_array->getNestedType();
            const auto * data_type_nested_float64 = typeid_cast<const DataTypeFloat64 *>(data_type_array_nested.get());
            const auto * data_type_nested_float32 = typeid_cast<const DataTypeFloat32 *>(data_type_array_nested.get());
            const auto * data_type_nested_bfloat16 = typeid_cast<const DataTypeBFloat16 *>(data_type_array_nested.get());
            if (data_type_nested_float64 == nullptr && data_type_nested_float32 == nullptr && data_type_nested_bfloat16 == nullptr)
                continue;

            /// Read value from column
            const ColumnPtr & column = child->column;
            const auto * literal_column = typeid_cast<const ColumnConst *>(column.get());
            if (!literal_column || literal_column->size() != 1)
                continue;
            Field field;
            literal_column->get(0, field);
            Field::Types::Which field_type = field.getType();
            if (field_type != Field::Types::Array)
                continue;
            Array field_array = field.safeGet<Array>();
            for (const auto & field_array_value : field_array)
            {
                Field::Types::Which field_array_value_type = field_array_value.getType();
                if (field_array_value_type != Field::Types::Float64)
                    return no_layers_updated;
                Float64 float64 = field_array_value.safeGet<Float64>();
                reference_vector.push_back(float64);
            }
        }
    }

    if (search_column.empty() || reference_vector.empty())
        return no_layers_updated;

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
    bool optimize_plan = !settings.vector_search_with_rescoring;
    size_t updated_layers = no_layers_updated;
    if (optimize_plan)
    {
        for (const auto & output : expression.getOutputs())
        {
            /// If the SELECT clause contains the vector column (rare situation), skip the optimization.
            if (output->result_name == search_column ||
                (output->type == ActionsDAG::ActionType::ALIAS && output->children.at(0)->result_name == search_column))
            {
                optimize_plan = false;
                break;
            }
        }

        if (optimize_plan)
        {
            /// Remove the physical vector column from ReadFromMergeTreeStep, add virtual "_distance" column
            read_from_mergetree_step->replaceVectorColumnWithDistanceColumn(search_column);

            /// Now replace the "cosineDistance(vec, [1.0, 2.0...])" node in the DAG by the "_distance" node
            std::cerr << expression.dumpDAG() << std::endl;
            expression.removeUnusedResult(sort_column); /// Removes the OUTPUT cosineDistance(...) FUNCTION Node
            expression.removeUnusedActions(); /// Removes the vector column INPUT node (it is no longer needed)
            const auto * distance_node = &expression.addInput("_distance",std::make_shared<DataTypeFloat32>());
            const auto * new_output = &expression.addAlias(*distance_node, sort_column);
            expression.getOutputs().push_back(new_output);
            std::cerr << expression.dumpDAG() << std::endl;

            updated_layers = 2;
            /// Need to do same removal of the vector column from the Filter step
            if (filter_step)
            {
                auto & filter_expression = filter_step->getExpression();
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
                updated_layers++;
            }
        }
    }

    auto vector_search_parameters = std::make_optional<VectorSearchParameters>(search_column, distance_function, n, reference_vector, additional_filters_present, optimize_plan);
    read_from_mergetree_step->setVectorSearchParameters(std::move(vector_search_parameters));

    return updated_layers;
}

}
