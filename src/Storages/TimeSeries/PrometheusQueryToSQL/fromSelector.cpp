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

        TimestampType min_time = node_range.start_time - node_range.window + 1;
        TimestampType max_time = node_range.end_time;

        builder.from_table_function = makeASTFunction(
            "timeSeriesSelector",
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
            make_intrusive<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(min_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(max_time, context.timestamp_data_type));

        if (filter_stale_markers)
        {
            builder.where = makeASTFunction(
                "notEquals",
                makeASTFunction("reinterpretAsUInt64", make_intrusive<ASTIdentifier>(ColumnNames::Value)),
                make_intrusive<ASTLiteral>(0x7ff0000000000002ULL));
        }

        res.select_query = builder.getSelectQuery();
        return res;
    }

    /// Prometheus uses a special NaN (with the bit pattern 0x7ff0000000000002) as a "stale marker" to mark the end of a time series.
    /// An instant selector must treat a stale marker as the absence of the series at that step: if the latest sample within
    /// the lookback window is a stale marker, the series is not present at all, even though there may be older samples in the window.
    /// `timeSeriesLastToGrid` returns the latest sample verbatim, so the vector grid built for an instant selector still contains
    /// stale markers. They must be replaced with NULL right here, before any other operator consumes the grid, because operators
    /// and aggregations use the presence of a value (`isNotNull`, `countForEach`, `anyForEach`) to decide whether a series exists
    /// at a step and would otherwise count stale series in `count(foo)`, `foo and bar`, `foo or bar` and so on.
    SQLQueryPiece replaceStaleMarkersWithNulls(SQLQueryPiece && vector_grid, ConverterContext & context)
    {
        if (vector_grid.store_method != StoreMethod::VECTOR_GRID)
            return std::move(vector_grid); /// Nothing to filter in an empty result.

        /// SELECT group,
        ///        arrayMap(x -> if(isNotNull(x) AND reinterpretAsUInt64(assumeNotNull(x)) = 0x7ff0000000000002, NULL, x), values) AS values
        /// FROM <vector_grid>
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        const String iterator_name = "x";

        /// isNotNull(x) AND reinterpretAsUInt64(assumeNotNull(x)) = 0x7ff0000000000002
        ASTPtr is_stale_marker = makeASTFunction(
            "and",
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(iterator_name)),
            makeASTFunction(
                "equals",
                makeASTFunction("reinterpretAsUInt64", makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(iterator_name))),
                make_intrusive<ASTLiteral>(0x7ff0000000000002ULL)));

        /// if(<is_stale_marker>, NULL, x)
        ASTPtr lambda_body = makeASTFunction(
            "if", std::move(is_stale_marker), make_intrusive<ASTLiteral>(Field{} /* NULL */), make_intrusive<ASTIdentifier>(iterator_name));

        builder.select_list.push_back(makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", makeASTFunction("tuple", make_intrusive<ASTIdentifier>(iterator_name)), std::move(lambda_body)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_grid.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        vector_grid.select_query = builder.getSelectQuery();
        return std::move(vector_grid);
    }
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    /// Stale markers are kept in the raw data here on purpose: `last_over_time` must see them, so that a stale marker
    /// being the latest sample within the lookback window hides the series, and then they are replaced with NULL.
    auto range_selector = fromRangeSelector(
        instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);
    auto vector_grid = applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
    return replaceStaleMarkersWithNulls(std::move(vector_grid), context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    /// Range selectors never return stale markers, see https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness
    return fromRangeSelector(
        instant_selector_text, range_selector_node, /* filter_stale_markers = */ true, context);
}

}
