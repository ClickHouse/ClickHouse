#include <Storages/TimeSeries/PrometheusQueryToSQL/getSortOrder.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionSort.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Returns the order of a PromQL sort function at the root of the query.
    SortOrder getSortOrderOfFunctionSort(const SortFunctionInfo & sort_function, const PrometheusQueryTree::Function * function_node)
    {
        ASTs keys;
        if (sort_function.by_label)
        {
            /// The first argument is the instant vector; the label names follow it. `applyFunctionSort` has checked
            /// that they have the type string, which only a string literal has.
            const auto & arguments = function_node->getArguments();
            for (size_t i = 1; i < arguments.size(); ++i)
            {
                chassert(arguments[i]->node_type == PrometheusQueryTree::NodeType::StringLiteral);
                const auto & label_name = static_cast<const PrometheusQueryTree::StringLiteral *>(arguments[i])->string;

                /// No series has a label with an empty name, so it decides nothing, and `timeSeriesExtractTag` does not accept one.
                if (label_name.empty())
                    continue;

                /// A series without the label is ordered as if the label were empty.
                auto label_value = makeASTFunction("ifNull",
                    makeASTFunction("timeSeriesExtractTag", make_intrusive<ASTIdentifier>(ColumnNames::Group), make_intrusive<ASTLiteral>(label_name)),
                    make_intrusive<ASTLiteral>(String{}));

                /// The label values are compared in natural order, as `sort_by_label` does in Prometheus.
                /// The mode is 1 because the default mode puts `host-1` before `host2`, and Prometheus puts it after.
                keys.emplace_back(makeASTFunction("naturalSortKey", std::move(label_value), make_intrusive<ASTLiteral>(UInt64{1})));
            }
        }
        else
        {
            keys.emplace_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));
        }

        /// Ties are ordered by tags, in the same direction, to make the result deterministic.
        keys.emplace_back(make_intrusive<ASTIdentifier>(ColumnNames::Tags));

        return SortOrder{std::move(keys), sort_function.direction == SortDirection::DESCENDING ? -1 : 1};
    }

    /// Returns the order of `topk` or `bottomk` at the root of the query.
    SortOrder getSortOrderOfLimitAggregation(const PrometheusQueryTree::AggregationOperator * aggregation, const SQLQueryPiece & result)
    {
        ASTs keys;
        if (result.store_method == StoreMethod::VECTOR_GRID && (aggregation->by || aggregation->without))
        {
            bool metric_name_dropped = result.metric_name_dropped;
            /// Group ids follow read order, so order by the bucket tags.
            ASTPtr bucket_group = transformGroupASTForAggregationOperator(
                aggregation,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                /*drop_metric_name=*/true,
                metric_name_dropped);
            keys.emplace_back(makeASTFunction("timeSeriesGroupToTags", std::move(bucket_group)));
        }
        keys.emplace_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        return SortOrder{std::move(keys), aggregation->operator_name == "topk" ? -1 : 1};
    }
}

std::optional<SortOrder> getSortOrder(const SQLQueryPiece & result)
{
    if (result.store_method == StoreMethod::VECTOR_GRID && result.node->node_type == PrometheusQueryTree::NodeType::Function)
    {
        const auto * function_node = static_cast<const PrometheusQueryTree::Function *>(result.node);
        if (auto sort_function = getSortFunctionInfo(function_node->function_name))
            return getSortOrderOfFunctionSort(*sort_function, function_node);
    }

    if (result.node->node_type == PrometheusQueryTree::NodeType::AggregationOperator)
    {
        const auto * aggregation = static_cast<const PrometheusQueryTree::AggregationOperator *>(result.node);
        if (aggregation->operator_name == "topk" || aggregation->operator_name == "bottomk")
            return getSortOrderOfLimitAggregation(aggregation, result);
    }

    if (result.store_method == StoreMethod::VECTOR_GRID)
    {
        /// Other instant vectors are sorted by tags, as range vectors are.
        return SortOrder{{make_intrusive<ASTIdentifier>(ColumnNames::Tags)}, 1};
    }

    return std::nullopt;
}

}
