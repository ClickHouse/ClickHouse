#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
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

    /// This optimization pass doesn't change the structure of the query plan.
    constexpr size_t updated_layers = 0;

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
    /// ReadFromMergeTree

    auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
    if (!limit_step)
        return updated_layers;

    if (node->children.size() != 1)
        return updated_layers;
    node = node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(node->step.get());
    if (!sorting_step)
        return updated_layers;

    if (node->children.size() != 1)
        return updated_layers;
    node = node->children.front();
    auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (!expression_step)
        return updated_layers;

    if (node->children.size() != 1)
        return updated_layers;
    node = node->children.front();
    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_mergetree_step)
        return updated_layers;

    /// Extract N
    size_t n = limit_step->getLimitForSorting();

    /// Check that the LIMIT specified by the user isn't too big - otherwise the cost of vector search outweighs the benefit.
    if (n > settings.max_limit_for_ann_queries)
        return updated_layers;

    /// Not 100% sure but other sort types are likely not what we want
    SortingStep::Type sorting_step_type = sorting_step->getType();
    if (sorting_step_type != SortingStep::Type::Full)
        return updated_layers;

    /// Read ORDER BY clause
    const auto & sort_description = sorting_step->getSortDescription();
    if (sort_description.size() > 1)
        return updated_layers;
    const String & sort_column = sort_description.front().column_name;

    /// The ActionDAG of the ExpressionStep underneath SortingStep may have arbitrary output nodes (e.g. stuff
    /// in the SELECT clause). Find the output node which corresponds to the first ORDER BY clause.
    const ActionsDAG & expression = expression_step->getExpression();
    const ActionsDAG::Node * sort_column_node = expression.tryFindInOutputs(sort_column);
    if (sort_column_node == nullptr || sort_column_node->type != ActionsDAG::ActionType::FUNCTION)
        return updated_layers;

    /// Extract distance_function
    const String & function_name = sort_column_node->function_base->getName();
    String distance_function;
    if (function_name == "L2Distance" || function_name == "cosineDistance")
        distance_function = function_name;
    else
        return updated_layers;

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
        }
        else if (child->type == ActionsDAG::ActionType::COLUMN)
        {
            /// Is it an Array(Float32) or Array(Float64) column?
            const DataTypePtr & data_type = child->result_type;
            const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
            if (data_type_array == nullptr)
                continue;
            DataTypePtr data_type_array_nested = data_type_array->getNestedType();
            const auto * data_type_nested_float64 = typeid_cast<const DataTypeFloat64 *>(data_type_array_nested.get());
            const auto * data_type_nested_float32 = typeid_cast<const DataTypeFloat32 *>(data_type_array_nested.get());
            if (data_type_nested_float64 == nullptr && data_type_nested_float32 == nullptr)
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
                    return updated_layers;
                Float64 float64 = field_array_value.safeGet<Float64>();
                reference_vector.push_back(float64);
            }
        }
    }

    if (search_column.empty() || reference_vector.empty())
        return updated_layers;

    auto vector_search_parameters = std::make_optional<VectorSearchParameters>(search_column, distance_function, n, reference_vector);
    read_from_mergetree_step->setVectorSearchParameters(std::move(vector_search_parameters));

    return updated_layers;
}

}
