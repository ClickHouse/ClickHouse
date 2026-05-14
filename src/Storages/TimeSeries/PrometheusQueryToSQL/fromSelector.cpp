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


SQLQueryPiece fromSelector(const PQT::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(instant_selector_text, instant_selector_node, /* filter_stale_markers = */ false, context);
    return filterInstantSelectorStaleMarkers(
        applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context), context);
}


SQLQueryPiece fromSelector(const PQT::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(instant_selector_text, range_selector_node, /* filter_stale_markers = */ true, context);
}

}
