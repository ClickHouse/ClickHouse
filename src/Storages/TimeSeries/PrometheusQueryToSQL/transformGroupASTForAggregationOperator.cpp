#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <algorithm>


namespace DB::PrometheusQueryToSQL
{

ASTPtr transformGroupASTForAggregationOperator(
    const PQT::AggregationOperator * operator_node,
    ASTPtr && group,
    bool drop_metric_name,
    bool & metric_name_dropped)
{
    if (!operator_node->by && !operator_node->without)
    {
        /// No by/without: aggregate all series into one with no tags (group 0).
        metric_name_dropped = true;
        return makeASTFunction("CAST", make_intrusive<ASTLiteral>(0u), make_intrusive<ASTLiteral>("UInt64"));
    }

    if (operator_node->by)
    {
        /// by(labels): keep only the specified labels, removing everything else including __name__.
        std::vector<std::string_view> tags_to_keep{operator_node->labels.begin(), operator_node->labels.end()};
        std::sort(tags_to_keep.begin(), tags_to_keep.end());
        tags_to_keep.erase(std::unique(tags_to_keep.begin(), tags_to_keep.end()), tags_to_keep.end());

        /// The metric name is preserved only if it is explicitly listed in by() and wasn't already dropped.
        if (!metric_name_dropped && !std::binary_search(tags_to_keep.begin(), tags_to_keep.end(), kMetricName))
            metric_name_dropped = true;

        return makeASTFunction(
            "timeSeriesRemoveAllTagsExcept",
            std::move(group),
            make_intrusive<ASTLiteral>(Array{tags_to_keep.begin(), tags_to_keep.end()}));
    }

    chassert(operator_node->without);

    /// without(labels): remove the specified labels and optionally __name__.
    std::vector<std::string_view> tags_to_remove{operator_node->labels.begin(), operator_node->labels.end()};

    if (!metric_name_dropped && drop_metric_name)
    {
        tags_to_remove.push_back(kMetricName);
        metric_name_dropped = true;
    }

    std::sort(tags_to_remove.begin(), tags_to_remove.end());
    tags_to_remove.erase(std::unique(tags_to_remove.begin(), tags_to_remove.end()), tags_to_remove.end());

    if (!metric_name_dropped && std::binary_search(tags_to_remove.begin(), tags_to_remove.end(), kMetricName))
        metric_name_dropped = true;

    return makeASTFunction(
        "timeSeriesRemoveTags",
        std::move(group),
        make_intrusive<ASTLiteral>(Array{tags_to_remove.begin(), tags_to_remove.end()}));
}

}
