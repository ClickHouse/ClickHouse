#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    constexpr UInt64 STALE_NAN_BITS = 0x7ff0000000000002ULL;

    /// The Prometheus stale marker is a specific `Float64` `NaN` payload, so it is recognized only on a
    /// `TimeSeries` table whose value column is `Float64`. A `Float32` value column cannot represent the
    /// payload - the write path downcasts the marker to an ordinary `NaN` - so on such a table stale samples
    /// are indistinguishable from regular `NaN` values here and flow through as ordinary data, as they did
    /// before stale-marker filtering existed. This limitation is documented for `prometheusQuery` and
    /// `prometheusQueryRange`.
    ASTPtr isStaleMarker(ASTPtr && value)
    {
        return makeASTFunction(
            "equals",
            makeASTFunction("reinterpretAsUInt64", std::move(value)),
            make_intrusive<ASTLiteral>(STALE_NAN_BITS));
    }

    ASTPtr makeFilterStaleMarkerValue(ASTPtr && value)
    {
        return makeASTFunction(
            "if",
            makeASTFunction("isNull", make_intrusive<ASTIdentifier>("x")),
            make_intrusive<ASTLiteral>(Field{}),
            makeASTFunction(
                "if",
                isStaleMarker(makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>("x"))),
                make_intrusive<ASTLiteral>(Field{}),
                std::move(value)));
    }

    SQLQueryPiece filterInstantSelectorStaleMarkers(SQLQueryPiece && expression, ConverterContext & context)
    {
        switch (expression.store_method)
        {
            case StoreMethod::EMPTY:
            {
                return std::move(expression);
            }
            case StoreMethod::VECTOR_GRID:
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                auto values = makeASTFunction(
                    "arrayMap",
                    makeASTLambda({"x"}, makeFilterStaleMarkerValue(make_intrusive<ASTIdentifier>("x"))),
                    make_intrusive<ASTIdentifier>(ColumnNames::Values));

                /// A series whose every selected sample is a stale marker is absent in Prometheus, so its row
                /// must not survive here either: downstream duplicate-series checks (vector matching,
                /// `dropMetricName`) count rows, and two such rows collapsing to the same labelset would
                /// otherwise raise a duplicate-series exception for series Prometheus does not return at all.
                ///
                /// A series which is stale only on a part of a range query keeps its row, because it is still
                /// present at the other evaluation timestamps. Two such series which are live at disjoint
                /// timestamps and collapse to the same labelset are therefore still reported as duplicates,
                /// even though Prometheus evaluates every step independently and accepts them. This is not
                /// specific to stale markers: two ordinary series which simply have samples in disjoint parts
                /// of the range behave the same way, because the duplicate-labelset checks in
                /// `applySimpleBinaryOperator`, `dropMetricName` and `applyLabelManipulationFunction` count
                /// rows rather than per-timestamp presence. Making those checks step-aware would also require
                /// merging the matching rows per timestamp, which changes the result of every PromQL binary
                /// operation and is out of the scope of stale-marker handling.
                builder.where = makeASTFunction(
                    "arrayExists",
                    makeASTLambda({"x"}, makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
                    values->clone());

                values->setAlias(ColumnNames::Values);
                builder.select_list.push_back(std::move(values));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE);
                builder.from_table = subqueries.back().name;

                expression.select_query = builder.getSelectQuery();
                return std::move(expression);
            }
            default:
            {
                throwUnexpectedStoreMethod(expression, context);
            }
        }
    }

    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    bool filter_stale_markers,
                                    ConverterContext & context)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
        /// FROM timeSeriesSelectorToGrid(<selector>, <start_time>, <end_time>, <step>, <window>)
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        /// Prometheus matrix selectors skip stale markers before functions see samples,
        /// while regular NaN samples remain real data points. Instant selectors filter stale
        /// markers after latest-sample selection, so they do not use this raw-data predicate.
        if (filter_stale_markers)
            builder.where = makeASTFunction("not", isStaleMarker(make_intrusive<ASTIdentifier>(ColumnNames::Value)));

        TimestampType min_time = node_range.start_time - node_range.window + 1;
        TimestampType max_time = node_range.end_time;

        builder.from_table_function = makeASTFunction(
            "timeSeriesSelector",
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
            make_intrusive<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(min_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(max_time, context.timestamp_data_type));

        res.select_query = builder.getSelectQuery();
        return res;
    }
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);
    return filterInstantSelectorStaleMarkers(
        applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context), context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(instant_selector_text, range_selector_node, /* filter_stale_markers = */ true, context);
}

}
