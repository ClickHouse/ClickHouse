#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionTimestamp.h>

#include <Common/Exception.h>
#include <Core/DecimalFunctions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];
        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }

    ASTPtr makeTimeGridAST(const SQLQueryPiece & argument, const ConverterContext & context)
    {
        return makeASTFunction(
            "CAST",
            makeASTFunction(
                "timeSeriesRange",
                timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(argument.step, context.timestamp_data_type)),
            make_intrusive<ASTLiteral>(fmt::format("Array({})", context.scalar_data_type->getName())));
    }

    SQLQueryPiece makeScalarLikeTimestampResult(
        const PQT::Function * function_node,
        const SQLQueryPiece & argument,
        ConverterContext & context)
    {
        SQLQueryPiece res{function_node, function_node->result_type, argument.store_method};
        res.start_time = argument.start_time;
        res.end_time = argument.end_time;
        res.step = argument.step;
        res.metric_name_dropped = argument.metric_name_dropped;

        if (argument.start_time == argument.end_time)
        {
            res.store_method = StoreMethod::CONST_SCALAR;
            res.scalar_value = DecimalUtils::convertTo<Float64>(argument.start_time, context.timestamp_scale);
            return dropMetricName(std::move(res), context);
        }

        SelectQueryBuilder builder;
        builder.select_list.push_back(makeTimeGridAST(argument, context));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        res.store_method = StoreMethod::SCALAR_GRID;
        res.select_query = builder.getSelectQuery();
        return dropMetricName(std::move(res), context);
    }

    struct InstantSelectorArgument
    {
        const PQT::InstantSelector * instant_selector = nullptr;
        const PQT::Offset * offset = nullptr;
    };

    InstantSelectorArgument getInstantSelectorArgument(const PQT::Function * function_node)
    {
        const auto & function_arguments = function_node->getArguments();
        if (function_arguments.size() != 1)
            return {};

        const auto * argument_node = function_arguments[0];
        if (argument_node->node_type == NodeType::InstantSelector)
            return {static_cast<const PQT::InstantSelector *>(argument_node), nullptr};

        if (argument_node->node_type == NodeType::Offset)
        {
            const auto * offset = static_cast<const PQT::Offset *>(argument_node);
            const auto * expression = offset->getExpression();
            if (expression->node_type == NodeType::InstantSelector)
                return {static_cast<const PQT::InstantSelector *>(expression), offset};
        }

        return {};
    }

    SQLQueryPiece makeTimestampResultForInstantSelector(
        const PQT::Function * function_node,
        const InstantSelectorArgument & selector_argument,
        ConverterContext & context)
    {
        const auto * instant_selector = selector_argument.instant_selector;
        auto node_range = context.node_range_getter.get(function_node);
        auto selector_range = context.node_range_getter.get(instant_selector);
        if (node_range.empty() || selector_range.empty())
            return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};

        const auto instant_selector_text = instant_selector->toString(*context.promql_tree);

        SelectQueryBuilder raw_builder;
        raw_builder.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", make_intrusive<ASTIdentifier>(ColumnNames::ID)));
        raw_builder.select_list.back()->setAlias(ColumnNames::Group);
        raw_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp));
        raw_builder.select_list.push_back(makeASTFunction("toFloat64", make_intrusive<ASTIdentifier>(ColumnNames::Timestamp)));
        raw_builder.select_list.back()->setAlias(ColumnNames::Value);
        raw_builder.from_table_function = makeASTFunction(
            "timeSeriesSelector",
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(context.time_series_storage_id.getTableName()),
            make_intrusive<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(selector_range.start_time - selector_range.window + 1, context.timestamp_data_type),
            timeSeriesTimestampToAST(selector_range.end_time, context.timestamp_data_type));

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), raw_builder.getSelectQuery(), SQLSubqueryType::TABLE});

        SelectQueryBuilder grid_builder;
        grid_builder.from_table = context.subqueries.back().name;
        grid_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        grid_builder.select_list.push_back(addParametersToAggregateFunction(
            makeASTFunction(
                "timeSeriesLastToGrid",
                make_intrusive<ASTIdentifier>(ColumnNames::Timestamp),
                make_intrusive<ASTIdentifier>(ColumnNames::Value)),
            timeSeriesTimestampToAST(selector_range.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(selector_range.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(selector_range.step, context.timestamp_data_type),
            timeSeriesDurationToAST(selector_range.window, context.timestamp_data_type)));
        grid_builder.select_list.back()->setAlias(ColumnNames::Values);
        grid_builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        SQLQueryPiece res{function_node, function_node->result_type, StoreMethod::VECTOR_GRID};
        res.select_query = grid_builder.getSelectQuery();
        res.start_time = selector_range.start_time;
        res.end_time = selector_range.end_time;
        res.step = selector_range.step;
        res.metric_name_dropped = false;

        if (selector_argument.offset)
            res = applyOffset(selector_argument.offset, std::move(res), context);

        return dropMetricName(std::move(res), context);
    }
}


bool isFunctionTimestamp(std::string_view function_name)
{
    return function_name == "timestamp";
}


SQLQueryPiece applyFunctionTimestamp(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    auto & argument = arguments[0];
    if (const auto selector_argument = getInstantSelectorArgument(function_node); selector_argument.instant_selector)
        return makeTimestampResultForInstantSelector(function_node, selector_argument, context);

    SQLQueryPiece res{function_node, function_node->result_type, argument.store_method};
    res.start_time = argument.start_time;
    res.end_time = argument.end_time;
    res.step = argument.step;
    res.metric_name_dropped = argument.metric_name_dropped;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            res.store_method = StoreMethod::EMPTY;
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        {
            /// Scalar-like instant vectors have a sample at every evaluation step.
            return makeScalarLikeTimestampResult(function_node, argument, context);
        }

        case StoreMethod::SCALAR_GRID:
        {
            SelectQueryBuilder builder;
            builder.select_list.push_back(makeTimeGridAST(argument, context));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            res.store_method = StoreMethod::SCALAR_GRID;
            res.select_query = builder.getSelectQuery();
            return dropMetricName(std::move(res), context);
        }

        case StoreMethod::VECTOR_GRID:
        {
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});

            SelectQueryBuilder builder;
            builder.from_table = context.subqueries.back().name;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            /// Computed vectors have samples at the evaluation timestamp.
            /// Keep NULL positions so the normal vector-grid finalizer preserves PromQL sparsity.
            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("value"), make_intrusive<ASTIdentifier>("timestamp")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("value")),
                        make_intrusive<ASTIdentifier>("timestamp"),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                make_intrusive<ASTIdentifier>(ColumnNames::Values),
                makeTimeGridAST(argument, context)));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            res.store_method = StoreMethod::VECTOR_GRID;
            res.select_query = builder.getSelectQuery();
            return dropMetricName(std::move(res), context);
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            throwUnexpectedStoreMethod(argument, context);
        }
    }

    UNREACHABLE();
}

}
