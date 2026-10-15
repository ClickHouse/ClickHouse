#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <algorithm>


namespace DB::PrometheusQueryToSQL
{

ASTPtr transformGroupASTForAggregationOperator(
    const PrometheusQueryTree::AggregationOperator * operator_node,
    ASTPtr && group,
    bool drop_metric_name,
    bool & metric_name_dropped)
{
    if ((!operator_node->by && !operator_node->without)
        || (operator_node->by && operator_node->labels.empty()))
    {
        /// No grouping labels: aggregate all series into one with no tags (group 0).
        metric_name_dropped = true;
        return timeSeriesNoTagsGroupToAST();
    }

    if (operator_node->by)
    {
        /// by(labels): keep only the specified labels, removing everything else including __name__.
        std::vector<std::string_view> tags_to_keep{operator_node->labels.begin(), operator_node->labels.end()};
        std::sort(tags_to_keep.begin(), tags_to_keep.end());
        tags_to_keep.erase(std::unique(tags_to_keep.begin(), tags_to_keep.end()), tags_to_keep.end());

        /// The metric name is preserved only if it is explicitly listed in by() and wasn't already dropped.
        /// The marker of a dropped metric name is not a grouping key.
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
        tags_to_remove.push_back(kDroppedMetricNameMarker);
        metric_name_dropped = true;
    }

    std::sort(tags_to_remove.begin(), tags_to_remove.end());
    tags_to_remove.erase(std::unique(tags_to_remove.begin(), tags_to_remove.end()), tags_to_remove.end());

    if (!metric_name_dropped && std::binary_search(tags_to_remove.begin(), tags_to_remove.end(), kMetricName))
    {
        tags_to_remove.push_back(kDroppedMetricNameMarker);
        metric_name_dropped = true;
    }

    return makeASTFunction(
        "timeSeriesRemoveTags",
        std::move(group),
        make_intrusive<ASTLiteral>(Array{tags_to_remove.begin(), tags_to_remove.end()}));
}


ASTPtr copyDroppedMetricNameMarkerToBucket(const ASTPtr & bucket_group)
{
    /// timeSeriesCopyTag(<bucket_group>, anyIf(group, isNotNull(timeSeriesExtractTag(group, '')) AND arrayExists(x -> isNotNull(x), values)), '')
    /// A row matches when it has the marker and a non-NULL sample.
    /// Otherwise anyIf returns the group without tags, so nothing is copied.
    return makeASTFunction(
        "timeSeriesCopyTag",
        bucket_group->clone(),
        makeASTFunction(
            "anyIf",
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            makeASTFunction(
                "and",
                makeASTFunction(
                    "isNotNull",
                    makeASTFunction(
                        "timeSeriesExtractTag",
                        make_intrusive<ASTIdentifier>(ColumnNames::Group),
                        make_intrusive<ASTLiteral>(kDroppedMetricNameMarker))),
                makeASTFunction(
                    "arrayExists",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
                    make_intrusive<ASTIdentifier>(ColumnNames::Values)))),
        make_intrusive<ASTLiteral>(kDroppedMetricNameMarker));
}

}
