#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Interpreters/ContextTimeSeriesTagsCollector.h>
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
    /// Whether the raw selector rows are consumed by a `timeSeries*ToGrid` aggregate function,
    /// which classifies every sample by timestamp and ignores samples outside its window.
    /// For such consumers the transpiled SQL does not need a row-level timestamp filter:
    /// with relaxed selector filtering (see `StorageTimeSeriesSelector`) the read may return
    /// rows outside the requested time range, and the aggregate drops them for free.
    bool isConsumedByFunctionOverRange(const Node * node)
    {
        const Node * parent = node->parent;
        /// An `offset`/`@` modifier wraps the range selector without changing the consumer.
        while (parent && (parent->node_type == PrometheusQueryTree::NodeType::Offset))
            parent = parent->parent;
        if (!parent || (parent->node_type != PrometheusQueryTree::NodeType::Function))
            return false;
        return isFunctionOverRange(static_cast<const PrometheusQueryTree::Function *>(parent)->function_name);
    }

    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    bool add_timestamp_filter,
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

        if (context.selector_relaxed_filtering)
        {
            /// WHERE group != <UNKNOWN_GROUP>
            ///
            /// With relaxed selector filtering (see `StorageTimeSeriesSelector`) the read may return
            /// extra rows whose identifiers do not belong to any selected time series; they get
            /// UNKNOWN_GROUP from timeSeriesIdToGroup() and are dropped here, at the only place where
            /// raw selector rows enter the transpiled SQL.
            builder.where = makeASTFunction(
                "notEquals",
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                make_intrusive<ASTLiteral>(ContextTimeSeriesTagsCollector::UNKNOWN_GROUP));

            /// AND (timestamp >= <min_time>) AND (timestamp <= <max_time>)
            ///
            /// Needed only when the raw rows can reach the query result (or a consumer which does not
            /// classify samples by timestamp): with relaxed selector filtering the read may return
            /// rows outside [min_time, max_time]. When a `timeSeries*ToGrid` aggregate consumes the
            /// rows, it drops out-of-window samples itself and the filter is omitted.
            if (add_timestamp_filter)
            {
                builder.where = makeASTFunction(
                    "and",
                    std::move(builder.where),
                    makeASTFunction(
                        "greaterOrEquals",
                        make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                        timeSeriesTimestampToAST(min_time, context.timestamp_data_type)),
                    makeASTFunction(
                        "lessOrEquals",
                        make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                        timeSeriesTimestampToAST(max_time, context.timestamp_data_type)));
            }
        }

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
    /// The rows are consumed by the last_over_time aggregation below, which drops out-of-window
    /// samples itself: no row-level timestamp filter is needed.
    auto range_selector = fromRangeSelector(
        instant_selector_text, instant_selector_node, /* add_timestamp_filter = */ false, context);
    return applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(
        instant_selector_text,
        range_selector_node,
        /* add_timestamp_filter = */ !isConsumedByFunctionOverRange(range_selector_node),
        context);
}

}
