#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>

#include <Core/DecimalFunctions.h>
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
    /// Converts a timestamp from `result_timestamp_scale` to `table_timestamp_scale`.
    /// The table scale can be less than the result scale, then the timestamp is rounded up or down as specified.
    TimestampType convertToTableScale(TimestampType timestamp, bool round_up, const ConverterContext & context)
    {
        chassert(context.table_timestamp_scale <= context.result_timestamp_scale);
        if (context.table_timestamp_scale == context.result_timestamp_scale)
            return timestamp;

        auto divisor = DecimalUtils::scaleMultiplier<Int64>(context.result_timestamp_scale - context.table_timestamp_scale);
        Int64 quotient = timestamp.value / divisor;
        Int64 remainder = timestamp.value % divisor;
        if (round_up && (remainder > 0))
            ++quotient;
        else if (!round_up && (remainder < 0))
            --quotient;
        return TimestampType{quotient};
    }

    SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text,
                                    const Node * node,
                                    ConverterContext & context)
    {
        auto node_range = context.node_range_getter.get(node);
        if (node_range.empty())
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        /// SELECT timeSeriesIdToGroup(id) AS group, timestamp::result_timestamp_type AS timestamp, value
        /// FROM timeSeriesSelector(<database>, <table>, <selector>, <min_time>, <max_time>)
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        /// The timestamps in the table can have a smaller scale than `result_timestamp_type` (for example, DateTime or UInt32),
        /// we convert them here because the functions over ranges (see applyFunctionOverRange()) expect timestamps and
        /// their parameters to have the same scale. The values are not converted here, see the comment for StoreMethod::RAW_DATA.
        if (context.table_timestamp_type->equals(*context.result_timestamp_type))
        {
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        }
        else
        {
            builder.select_list.push_back(
                timeSeriesTimestampASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), context.result_timestamp_type));
            builder.select_list.back()->setAlias(ColumnNames::Timestamp);
        }

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));

        /// The range is (start_time - window, end_time] at the result scale. The functions over ranges select samples exactly,
        /// so if the table scale is smaller we round the bounds outwards to make sure we don't skip any samples.
        TimestampType min_time = convertToTableScale(node_range.start_time - node_range.window + 1, /* round_up = */ true, context);
        TimestampType max_time = convertToTableScale(node_range.end_time, /* round_up = */ false, context);

        builder.from_table_function = makeASTFunction(
            "timeSeriesSelector",
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
            make_intrusive<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(min_time, context.table_timestamp_type),
            timeSeriesTimestampToAST(max_time, context.table_timestamp_type));

        res.select_query = builder.getSelectQuery();
        return res;
    }
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = instant_selector_node->toString(*context.promql_tree);
    auto range_selector = fromRangeSelector(instant_selector_text, instant_selector_node, context);
    return applyFunctionOverRange(instant_selector_node, "last_over_time", {std::move(range_selector)}, context);
}


SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = range_selector_node->getInstantSelector()->toString(*context.promql_tree);
    return fromRangeSelector(instant_selector_text, range_selector_node, context);
}

}
