#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>

#include <Common/typeid_cast.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/makeNoDuplicateSeriesPerStepCheck.h>

#include <algorithm>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Returns true if all series selected by `node` have the same metric name: `node` is an instant or range selector with
    /// an equality matcher on `__name__` (`http_requests_total` or `{__name__="http_requests_total"}`), maybe wrapped in
    /// offset or @ modifiers or a subquery. Other nodes can rename or combine series, so the function returns false for them.
    bool selectsSingleMetricName(const Node * node)
    {
        while (true)
        {
            switch (node->node_type)
            {
                case NodeType::Offset:
                    node = typeid_cast<const PrometheusQueryTree::Offset &>(*node).getExpression();
                    break;
                case NodeType::Subquery:
                    node = typeid_cast<const PrometheusQueryTree::Subquery &>(*node).getExpression();
                    break;
                case NodeType::RangeSelector:
                    node = typeid_cast<const PrometheusQueryTree::RangeSelector &>(*node).getInstantSelector();
                    break;
                case NodeType::InstantSelector:
                {
                    const auto & matchers = typeid_cast<const PrometheusQueryTree::InstantSelector &>(*node).matchers;
                    return std::ranges::any_of(matchers, [](const PrometheusQueryTree::Matcher & matcher)
                        { return (matcher.label_name == kMetricName) && (matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ); });
                }
                default:
                    return false;
            }
        }
    }

    /// Returns true if the series of `query_piece` can't get the same tags when the metric name is dropped.
    /// `query_piece` is the result of a function or an operator (for example `rate(x[5m])`, `abs(x)`, `x * 2`) which keeps
    /// the tags of its only vector argument except `__name__` (and `le` for `histogram_quantile`), so its series have
    /// different tags. If all of them also have the same metric name then they still have different tags without it.
    bool cannotGetDuplicateSeries(const SQLQueryPiece & query_piece)
    {
        const Node * node = query_piece.node;
        if (!node
            || ((node->node_type != NodeType::Function) && (node->node_type != NodeType::UnaryOperator)
                && (node->node_type != NodeType::BinaryOperator)))
            return false;

        const Node * vector_argument = nullptr;
        for (const auto * child : node->children)
        {
            if ((child->result_type == ResultType::INSTANT_VECTOR) || (child->result_type == ResultType::RANGE_VECTOR))
            {
                if (vector_argument)
                    return false;
                vector_argument = child;
            }
        }

        return vector_argument && selectsSingleMetricName(vector_argument);
    }
}


SQLQueryPiece dropMetricName(SQLQueryPiece && query_piece, ConverterContext & context)
{
    if (query_piece.metric_name_dropped)
        return std::move(query_piece);

    switch (query_piece.store_method)
    {
        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::CONST_STRING:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// No metric name.
            query_piece.metric_name_dropped = true;
            return std::move(query_piece);
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// When we remove the metric name `__name__` it's possible that we get the same set of tags (i.e. the same `group`)
            /// on time series which were different before we removed the metric name.
            /// Prometheus evaluates each step independently, so this is allowed only if such series don't have values
            /// at the same step (for example, if a metric was renamed at some point), and then their values are merged into one series.
            /// Otherwise it's an error, we can't have multiple time series with the same set of tags in the same resultset.
            ///
            /// Example:
            ///             tags                           timestamp1        timestamp2
            /// metric1{tag1='value1', tag2='value2'}       value_a           value_b
            /// metric2{tag1='value1', tag2='value2'}       value_c           value_d
            ///                                 ||
            ///                                 \/
            ///             tags                           timestamp1        timestamp2
            /// {tag1='value1', tag2='value2'}              value_a           value_b
            /// {tag1='value1', tag2='value2'}              value_c           value_d
            ///
            /// That's why we need the function timeSeriesThrowDuplicateSeriesIf() to detect such cases and throw an exception.
            ///
            /// If the series can't get the same tags (see `cannotGetDuplicateSeries`) then each `new_group` comes from one series,
            /// so step 1 is a projection without GROUP BY and HAVING.

            /// Step 1:
            /// SELECT timeSeriesRemoveTag(group, '__name__') AS new_group,
            ///        anyForEach(values) AS values
            /// FROM <vector_grid>
            /// GROUP BY new_group
            /// HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(<vector_grid>.values)), new_group) = 0
            ///
            /// or, if the series can't get the same tags:
            /// SELECT timeSeriesRemoveTag(group, '__name__') AS new_group, values
            /// FROM <vector_grid>
            ASTPtr metric_name_removing_query;
            {
                SelectQueryBuilder builder;
                const bool need_duplicate_check = !cannotGetDuplicateSeries(query_piece);

                builder.select_list.push_back(makeASTFunction(
                    "timeSeriesRemoveTag", make_intrusive<ASTIdentifier>(ColumnNames::Group), make_intrusive<ASTLiteral>(kMetricName)));
                builder.select_list.back()->setAlias(ColumnNames::NewGroup);

                if (need_duplicate_check)
                {
                    builder.select_list.push_back(makeNoDuplicateSeriesPerStepValues(make_intrusive<ASTIdentifier>(ColumnNames::Values)));
                    builder.select_list.back()->setAlias(ColumnNames::Values);
                }
                else
                {
                    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
                }

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(query_piece.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                if (need_duplicate_check)
                {
                    builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

                    builder.having = makeNoDuplicateSeriesPerStepCheck(
                        make_intrusive<ASTIdentifier>(Strings{builder.from_table, ColumnNames::Values}),
                        make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
                }

                metric_name_removing_query = builder.getSelectQuery();
            }

            /// Step 2:
            /// SELECT new_group AS group, values
            /// FROM step1
            ASTPtr column_renaming_query;
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
                builder.select_list.back()->setAlias(ColumnNames::Group);

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(metric_name_removing_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                column_renaming_query = builder.getSelectQuery();
            }

            query_piece.select_query = std::move(column_renaming_query);
            query_piece.metric_name_dropped = true;

            return std::move(query_piece);
        }

        case StoreMethod::SAMPLES:
        case StoreMethod::TIMESTAMP_AND_VALUE:
        {
            /// dropMetricName() must not be called with StoreMethod::SAMPLES or StoreMethod::TIMESTAMP_AND_VALUE.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cannot drop the metric name from the result of expression {} because of its store method {}",
                            getPromQLText(query_piece, context), query_piece.store_method);
        }
    }

    UNREACHABLE();
}

}
