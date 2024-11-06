#include <memory>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB::QueryPlanOptimizations
{

size_t tryUseVectorSearch(QueryPlan::Node * parent_node, QueryPlan::Nodes & /* nodes*/)
{
    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    if (parent_node->children.size() != 1)
        return 0;
    QueryPlan::Node * child_node = parent_node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(child_node->step.get());
    if (!sorting_step)
        return 0;

    if (child_node->children.size() != 1)
        return 0;
    child_node = child_node->children.front();
    auto * expression_step = typeid_cast<ExpressionStep *>(child_node->step.get());
    if (!expression_step)
        return 0;

    if (child_node->children.size() != 1)
        return 0;
    child_node = child_node->children.front();
    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(child_node->step.get());
    if (!read_from_mergetree_step)
        return 0;

    const auto & sort_description = sorting_step->getSortDescription();

    if (sort_description.size() != 1)
        return 0;

    [[maybe_unused]] ReadFromMergeTree::DistanceFunction distance_function;

    /// lol
    if (sort_description[0].column_name.starts_with("L2Distance"))
        distance_function = ReadFromMergeTree::DistanceFunction::L2Distance;
    else if (sort_description[0].column_name.starts_with("cosineDistance"))
        distance_function = ReadFromMergeTree::DistanceFunction::cosineDistance;
    else
        return 0;

    [[maybe_unused]] size_t limit = sorting_step->getLimit();
    [[maybe_unused]] std::vector<Float64> reference_vector = {0.0, 0.2}; /// TODO

    /// TODO check that ReadFromMergeTree has a vector similarity index

    read_from_mergetree_step->vec_sim_idx_input = std::make_optional<ReadFromMergeTree::VectorSimilarityIndexInput>(
            distance_function, limit, reference_vector);

    /// --- --- ---
    /// alwaysUnknownOrTrue:
    ///
    /// String index_distance_function;
    /// switch (metric_kind)
    /// {
    ///     case unum::usearch::metric_kind_t::l2sq_k: index_distance_function = "L2Distance"; break;
    ///     case unum::usearch::metric_kind_t::cos_k:  index_distance_function = "cosineDistance"; break;
    ///     default: std::unreachable();
    /// }
    /// return vector_similarity_condition.alwaysUnknownOrTrue(index_distance_function);

    return 0;
}

}
