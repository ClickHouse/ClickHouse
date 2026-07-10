#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Common/Exception.h>
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


    /// Replaces Prometheus stale-marker entries in a VECTOR_GRID's `values` array with NULL.
    /// A stale marker (bit pattern 0x7ff0000000000002) means "the series has gone stale here", so Prometheus drops
    /// that step entirely. `timeSeriesFromGrid` and the instant-vector finalizer both skip NULL entries, which matches
    /// that behavior. This is applied at the single point where stale markers first enter an instant-vector grid
    /// (see fromSelector below), so all downstream consumers - the instant-vector and range-vector finalizers,
    /// elementwise operators and functions, and subqueries - observe stale steps as NULL rather than as real samples.
    void dropStaleMarkersFromVectorGrid(SQLQueryPiece & expression, ConverterContext & context)
    {
        chassert(expression.store_method == StoreMethod::VECTOR_GRID);
        chassert(expression.select_query);

        /// SELECT group,
        ///        arrayMap(x -> if(isNotNull(x) AND reinterpretAsUInt64(assumeNotNull(x)) = <stale_marker>, NULL, x), values) AS values
        /// FROM (<previous select query>)
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        const String iterator_name = "x";

        /// isNotNull(x) AND reinterpretAsUInt64(assumeNotNull(x)) = 0x7ff0000000000002
        /// (0x7ff0000000000002 is the bit representation of the Prometheus stale marker.)
        ASTPtr is_stale_marker = makeASTFunction(
            "and",
            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>(iterator_name)),
            makeASTFunction(
                "equals",
                makeASTFunction("reinterpretAsUInt64", makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(iterator_name))),
                make_intrusive<ASTLiteral>(0x7ff0000000000002ULL)));

        /// if(<is_stale_marker>, NULL, x)
        ASTPtr lambda_body = makeASTFunction(
            "if",
            std::move(is_stale_marker),
            make_intrusive<ASTLiteral>(Field{} /* NULL */),
            make_intrusive<ASTIdentifier>(iterator_name));

        ASTPtr values = makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", makeASTFunction("tuple", make_intrusive<ASTIdentifier>(iterator_name)), std::move(lambda_body)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));
        values->setAlias(ColumnNames::Values);
        builder.select_list.push_back(std::move(values));

        context.subqueries.emplace_back(context.subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE);
        builder.from_table = context.subqueries.back().name;

        expression.select_query = builder.getSelectQuery();
    }
}


SQLQueryPiece fromSelector(const PQT::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);

    /// A bare instant selector is evaluated as `last_over_time` over an implicit lookback window.
    /// Stale markers are intentionally kept in the raw data here (filter_stale_markers = false) so that
    /// `last_over_time` can observe a stale marker as the latest sample: a stale marker means the series has gone
    /// stale and must mask any earlier real sample in the window rather than resurrect it.
    auto range_selector = fromRangeSelector(
        instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);
    auto result = applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context);

    /// Now that `last_over_time` has collapsed each window to its latest sample, drop the stale markers from the grid
    /// so they never surface as samples and never feed downstream vector operations. This is the sole entry point of
    /// stale markers into an instant-vector grid, so filtering here covers every consumer of the grid.
    if (result.store_method == StoreMethod::VECTOR_GRID)
        dropStaleMarkersFromVectorGrid(result, context);

    return result;
}


SQLQueryPiece fromSelector(const PQT::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(
        instant_selector_text, range_selector_node, /* filter_stale_markers = */ true, context);
}

}
