#include <Storages/TimeSeries/PrometheusQueryToSQL/makeSelector.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    SQLQueryPiece makeRangeSelector(std::string_view instant_selector_text,
                                    const PrometheusQueryTree::Node * node,
                                    ConverterContext & context)
    {
        auto evaluation_range = context.node_evaluation_range_getter.get(node);

        if (evaluation_range.start_time > evaluation_range.end_time)
            return SQLQueryPiece{node, ResultType::RANGE_VECTOR, StoreMethod::EMPTY};

        SQLQueryPiece res{node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        /// SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
        /// FROM timeSeriesSelectorToGrid(<selector>, <start_time>, <end_time>, <step>, <window>)
        SelectQueryParams params;

        params.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID)));
        params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);

        params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));

        params.from_table_function = makeASTFunction(
            "timeSeriesSelectorToGrid",
            std::make_shared<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            std::make_shared<ASTLiteral>(context.time_series_storage_id.getTableName()),
            std::make_shared<ASTLiteral>(String{instant_selector_text}),
            timeseriesTimeToAST(evaluation_range.start_time),
            timeseriesTimeToAST(evaluation_range.end_time),
            timeseriesDurationToAST(evaluation_range.step),
            timeseriesDurationToAST(evaluation_range.window));

        res.select_query = buildSelectQuery(std::move(params));
        return res;
    }
}


SQLQueryPiece makeSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = context.promql_tree.getQuery(instant_selector_node);
    auto range_selector = makeRangeSelector(instant_selector_text, instant_selector_node, context);
    return applyFunctionOverRange("last_over_time", std::move(range_selector), instant_selector_node, context);
}


SQLQueryPiece makeSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = context.promql_tree.getQuery(range_selector_node->getInstantSelector());
    return makeRangeSelector(instant_selector_text, range_selector_node, context);
}

}
