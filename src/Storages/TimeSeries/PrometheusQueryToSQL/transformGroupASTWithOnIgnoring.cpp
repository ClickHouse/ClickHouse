#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTWithOnIgnoring.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <algorithm>


namespace DB::PrometheusQueryToSQL
{

ASTPtr transformGroupASTWithOnIgnoring(
    const PQT::BinaryOperator * operator_node,
    ASTPtr && group,
    bool drop_metric_name,
    bool & metric_name_dropped)
{
    /// Group #0 always means a group with no tags.
    if (const auto * literal = group->as<const ASTLiteral>(); literal && literal->value == Field{0u})
    {
        metric_name_dropped = true;
        return std::move(group);
    }

    if (operator_node->on)
    {
        if (operator_node->labels.empty())
        {
            /// on() means we ignore all tags.
            metric_name_dropped = true;
            return make_intrusive<ASTLiteral>(0u);
        }
        else
        {
            /// on(tags) means we ignore all tags except the specified ones.
            /// If the metric name "__name__" is among the tags in on(tags) we don't remove it from the join group.

            /// timeSeriesRemoveAllTagsExcept(group, on_tags)
            std::vector<std::string_view> tags_to_keep = {operator_node->labels.begin(), operator_node->labels.end()};
            std::sort(tags_to_keep.begin(), tags_to_keep.end());
            tags_to_keep.erase(std::unique(tags_to_keep.begin(), tags_to_keep.end()), tags_to_keep.end());

            if (!metric_name_dropped && !std::binary_search(tags_to_keep.begin(), tags_to_keep.end(), kMetricName))
                metric_name_dropped = true;

            return makeASTFunction(
                "timeSeriesRemoveAllTagsExcept",
                std::move(group),
                make_intrusive<ASTLiteral>(Array{tags_to_keep.begin(), tags_to_keep.end()}));
        }
    }

    if (operator_node->ignoring && !operator_node->labels.empty())
    {
        /// ignoring(tags) means we ignore the specified tags, and also the metric name "__name__".

        /// timeSeriesRemoveTags(group, ignoring_tags + ['__name__'])
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
            "timeSeriesRemoveTags", std::move(group), make_intrusive<ASTLiteral>(Array{tags_to_remove.begin(), tags_to_remove.end()}));
    }

    /// Neither on() keyword nor ignoring() keyword are specified,
    /// so we use all the tags except the metric name "__name__".
    if (!metric_name_dropped && drop_metric_name)
    {
        group = makeASTFunction("timeSeriesRemoveTag", std::move(group), make_intrusive<ASTLiteral>(kMetricName));
        metric_name_dropped = true;
    }

    return std::move(group);
}

}
