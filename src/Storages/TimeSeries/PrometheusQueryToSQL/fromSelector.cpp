#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Builds:
    /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value[, is_stale_marker]
    /// FROM timeSeriesSelector(<selector>, <start_time>, <end_time>, <step>, <window>)
    /// [WHERE NOT is_stale_marker]
    ///
    /// By default (`include_stale_markers` = false) rows carrying a Prometheus stale marker are excluded
    /// entirely, matching real Prometheus' range-vector/matrix-selector semantics: a stale marker is simply
    /// not part of the matrix that an explicit range selector (or an `_over_time` function reading it) sees
    /// (see PrometheusRemoteWriteProtocol.cpp's `isPrometheusStaleMarker`).
    ///
    /// `include_stale_markers` = true additionally projects `is_stale_marker` and does NOT filter marker
    /// rows out. This is used only by fromSelector(InstantSelector*) below to implement real Prometheus'
    /// *other* staleness rule: a bare instant-vector selector (`vector_selector`) must stop at a stale
    /// marker instead of skipping over it to an older real sample.
    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    ConverterContext & context,
                                    bool include_stale_markers = false)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        if (include_stale_markers)
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::IsStaleMarker));
        else
            builder.where = makeASTFunction("not", make_intrusive<ASTIdentifier>(ColumnNames::IsStaleMarker));

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

    /// Real Prometheus evaluates a bare instant-vector selector as `last_over_time(instant_selector[window])`,
    /// but its evaluation code path (`vector_selector`) differs from an *explicit* range selector's
    /// (`matrix_selector`, see fromRangeSelector() above) in exactly one way: if the *most recent* sample in
    /// the window is a stale marker, the series is reported absent for that grid point - even if an older
    /// real sample exists earlier in the very same window. An explicit range selector, in contrast, simply
    /// never sees stale markers at all (they're excluded from its matrix), so an older real sample in its
    /// window is still visible. This is why the "samples" table keeps stale-marker rows (flagged via
    /// `is_stale_marker`) instead of dropping them outright.
    ///
    /// We can't implement this by just excluding marker rows and running last_over_time as usual - that is
    /// exactly the *explicit*-range-selector behavior and would incorrectly fall through to the older real
    /// sample. Instead we compute two independent last_over_time-style answers over the very same (unfiltered)
    /// window: the real value ignoring marker rows (`timeSeriesLastToGridIf`, condition = "not a marker"),
    /// and whether the most recent row of any kind (real sample or marker) is a marker (`timeSeriesLastToGrid`
    /// fed `is_stale_marker` as the "value"). AggregateFunctionTimeseriesToGridSparse's "most recent row (by
    /// timestamp) wins, whatever it is" semantics apply equally well to a plain 0/1 flag as to a real value,
    /// so the second call correctly tracks the recency ordering of markers vs. real samples without any
    /// changes to that aggregate function (and without disturbing the numeric `_over_time` functions, which
    /// keep using the "exclude marker rows" path unchanged). If the winning row was a marker, the final
    /// result is NULL for that grid point; otherwise it's the real value.
    auto raw_data = fromRangeSelector(instant_selector_text, instant_selector_node, context, /* include_stale_markers= */ true);

    if (raw_data.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{instant_selector_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};

    auto node_range = context.node_range_getter.get(instant_selector_node);
    auto start_time = node_range.start_time;
    auto end_time = node_range.end_time;
    auto step = node_range.step;
    auto window = node_range.window;

    auto & subqueries = context.subqueries;

    subqueries.emplace_back(subqueries.size(), std::move(raw_data.select_query), SQLSubqueryType::TABLE);
    const String raw_data_table = subqueries.back().name;

    /// SELECT group,
    ///        timeSeriesLastToGridIf(<start>,<end>,<step>,<window>)(timestamp, value, not is_stale_marker) AS values,
    ///        timeSeriesLastToGrid(<start>,<end>,<step>,<window>)(timestamp, <is_stale_marker cast to scalar type>) AS is_stale_marker
    /// FROM <raw_data_table>
    /// GROUP BY group
    ASTPtr combined_query;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        builder.select_list.push_back(addParametersToAggregateFunction(
            makeASTFunction("timeSeriesLastToGridIf",
                make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                make_intrusive<ASTIdentifier>(ColumnNames::Value),
                makeASTFunction("not", make_intrusive<ASTIdentifier>(ColumnNames::IsStaleMarker))),
            timeSeriesTimestampToAST(start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type)));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        builder.select_list.push_back(addParametersToAggregateFunction(
            makeASTFunction("timeSeriesLastToGrid",
                make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::IsStaleMarker), context.scalar_data_type)),
            timeSeriesTimestampToAST(start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(step, context.timestamp_data_type),
            timeSeriesDurationToAST(window, context.timestamp_data_type)));
        builder.select_list.back()->setAlias(ColumnNames::WinningRowIsStaleMarker);

        builder.from_table = raw_data_table;
        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        combined_query = builder.getSelectQuery();
    }

    subqueries.emplace_back(subqueries.size(), std::move(combined_query), SQLSubqueryType::TABLE);
    const String combined_table = subqueries.back().name;

    /// SELECT group,
    ///        arrayMap((x, y) -> if(coalesce(y, 0) = 1, NULL, x), values, winning_row_is_stale_marker) AS values
    /// FROM <combined_table>
    ///
    /// (`coalesce(y, 0)` treats "no row at all in the window" - a NULL from timeSeriesLastToGrid - the same
    /// as "not a marker": in that case `values` (x) is NULL anyway, since timeSeriesLastToGridIf can't have
    /// found anything either, so the choice doesn't change the result.)
    SelectQueryBuilder outer_builder;
    outer_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    outer_builder.select_list.push_back(makeASTFunction(
        "arrayMap",
        makeASTFunction(
            "lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
            makeASTFunction(
                "if",
                makeASTFunction("equals",
                    makeASTFunction("coalesce", make_intrusive<ASTIdentifier>("y"), make_intrusive<ASTLiteral>(0.0)),
                    make_intrusive<ASTLiteral>(1.0)),
                make_intrusive<ASTLiteral>(Field{} /* NULL */),
                make_intrusive<ASTIdentifier>("x"))),
        make_intrusive<ASTIdentifier>(ColumnNames::Values),
        make_intrusive<ASTIdentifier>(ColumnNames::WinningRowIsStaleMarker)));
    outer_builder.select_list.back()->setAlias(ColumnNames::Values);

    outer_builder.from_table = combined_table;

    SQLQueryPiece res{instant_selector_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.select_query = outer_builder.getSelectQuery();
    res.start_time = start_time;
    res.end_time = end_time;
    res.step = step;
    /// last_over_time keeps the metric name (see the `last_over_time` entry in applyFunctionOverRange.cpp's
    /// impl_map), matching a bare instant selector, which also keeps it - so metric_name_dropped stays false.
    return res;
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(instant_selector_text, range_selector_node, context);
}

}
