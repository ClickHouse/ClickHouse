#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionTimestamp.h>

#include <Common/Exception.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionOverRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyOffset.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromSelector.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    const DataTypePtr & timestampValueDataType()
    {
        static const DataTypePtr data_type = std::make_shared<DataTypeFloat64>();
        return data_type;
    }

    /// Returns the InstantSelector if `node` is a bare InstantSelector or an Offset node
    /// directly wrapping a bare InstantSelector; returns nullptr for any other expression
    /// (unary/binary operators, aggregations, functions, etc.).
    const PQT::InstantSelector * peelToInstantSelector(const Node * node, const PQT::Offset *& offset_node)
    {
        if (node->node_type == NodeType::InstantSelector)
        {
            return static_cast<const PQT::InstantSelector *>(node);
        }
        if (node->node_type == NodeType::Offset)
        {
            const auto * offset = static_cast<const PQT::Offset *>(node);
            if (offset->getExpression()->node_type == NodeType::InstantSelector)
            {
                offset_node = offset;
                return static_cast<const PQT::InstantSelector *>(offset->getExpression());
            }
        }
        return nullptr;
    }

    /// Evaluates timestamp() for a general instant vector expression (e.g. timestamp(test * 1), timestamp(-test),
    /// timestamp(timestamp(test))). For non-selector expressions, Prometheus materializes the expression at each
    /// query step evaluation timestamp T_eval, returning T_eval (in seconds since epoch) for every present sample.
    SQLQueryPiece applyTimestampToGeneralExpression(
        const PQT::Function * function_node, SQLQueryPiece && argument, ConverterContext & context)
    {
        switch (argument.store_method)
        {
            case StoreMethod::EMPTY:
            {
                SQLQueryPiece res{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
                res.value_data_type = timestampValueDataType();
                return res;
            }

            case StoreMethod::CONST_SCALAR:
            case StoreMethod::SINGLE_SCALAR:
            case StoreMethod::SCALAR_GRID:
            {
                auto node_range = context.node_range_getter.get(function_node);
                if (node_range.empty())
                {
                    SQLQueryPiece res{function_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
                    res.value_data_type = timestampValueDataType();
                    return res;
                }

                if (node_range.start_time == node_range.end_time)
                {
                    /// Single evaluation time, so we use StoreMethod::CONST_SCALAR with INSTANT_VECTOR type.
                    SQLQueryPiece res{function_node, ResultType::INSTANT_VECTOR, StoreMethod::CONST_SCALAR};
                    res.start_time = node_range.start_time;
                    res.end_time = node_range.end_time;
                    res.step = node_range.step;
                    res.scalar_value = DecimalUtils::convertTo<Float64>(node_range.start_time, context.timestamp_scale);
                    res.value_data_type = timestampValueDataType();
                    return res;
                }
                else
                {
                    /// Range of evaluation times, so we use StoreMethod::SCALAR_GRID with INSTANT_VECTOR type.
                    SQLQueryPiece res{function_node, ResultType::INSTANT_VECTOR, StoreMethod::SCALAR_GRID};
                    res.start_time = node_range.start_time;
                    res.end_time = node_range.end_time;
                    res.step = node_range.step;

                    SelectQueryBuilder builder;
                    builder.select_list.push_back(makeASTFunction(
                        "CAST",
                        makeASTFunction(
                            "timeSeriesRange",
                            timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type),
                            timeSeriesTimestampToAST(node_range.end_time, context.timestamp_data_type),
                            timeSeriesDurationToAST(node_range.step, context.timestamp_data_type)),
                        make_intrusive<ASTLiteral>(fmt::format("Array({})", timestampValueDataType()->getName()))));

                    builder.select_list.back()->setAlias(ColumnNames::Values);
                    res.select_query = builder.getSelectQuery();
                    res.value_data_type = timestampValueDataType();

                    return res;
                }
            }

            case StoreMethod::VECTOR_GRID:
            {
                context.subqueries.emplace_back(SQLSubquery{
                    context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
                String subquery_name = context.subqueries.back().name;

                SQLQueryPiece res{function_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
                res.start_time = argument.start_time;
                res.end_time = argument.end_time;
                res.step = argument.step;
                res.value_data_type = timestampValueDataType();

                SelectQueryBuilder builder;
                builder.from_table = subquery_name;
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                ASTPtr values_col = make_intrusive<ASTIdentifier>(ColumnNames::Values);
                ASTPtr range_arr = makeASTFunction(
                    "CAST",
                    makeASTFunction(
                        "timeSeriesRange",
                        timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                        timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                        timeSeriesDurationToAST(argument.step, context.timestamp_data_type)),
                    make_intrusive<ASTLiteral>("Array(Nullable(Float64))"));

                ASTPtr lambda_expr = makeASTFunction(
                    "arrayMap",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("t")),
                        makeASTFunction(
                            "if",
                            makeASTFunction("isNull", make_intrusive<ASTIdentifier>("x")),
                            makeASTFunction("CAST", make_intrusive<ASTLiteral>(Field{}), make_intrusive<ASTLiteral>("Nullable(Float64)")),
                            makeASTFunction("CAST", make_intrusive<ASTIdentifier>("t"), make_intrusive<ASTLiteral>("Nullable(Float64)")))),
                    std::move(values_col),
                    std::move(range_arr));

                lambda_expr->setAlias(ColumnNames::Values);
                builder.select_list.push_back(std::move(lambda_expr));

                res.select_query = builder.getSelectQuery();
                return dropMetricName(std::move(res), context);
            }

            default:
                throwUnexpectedStoreMethod(argument, context);
        }
    }
}


bool isFunctionTimestamp(std::string_view function_name)
{
    return function_name == "timestamp";
}


SQLQueryPiece applyFunctionTimestamp(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    if (arguments.size() != 1)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function 'timestamp' expects 1 argument, but was called with {} arguments",
                        arguments.size());
    }

    if (arguments[0].type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function 'timestamp' expects an argument of type {}, but expression {} has type {}",
                        ResultType::INSTANT_VECTOR,
                        getPromQLText(arguments[0], context), arguments[0].type);
    }

    const PQT::Offset * offset_node = nullptr;
    const auto * instant_selector = peelToInstantSelector(function_node->getArguments().at(0), offset_node);
    if (instant_selector)
    {
        /// Direct instant vector selector (plus optional direct offset/@ modifier):
        /// Returns the raw sample timestamp from storage.
        auto instant_selector_text = instant_selector->toString(*context.promql_tree);
        auto range_selector = fromRangeSelector(instant_selector_text, instant_selector, context);
        auto res = applyFunctionOverRange(instant_selector, "timestamp", {std::move(range_selector)}, context);
        if (offset_node)
            res = applyOffset(offset_node, std::move(res), context);
        res.node = function_node;
        res.value_data_type = timestampValueDataType();
        return res;
    }

    /// General instant vector expression:
    /// Evaluates arguments[0] as a standard instant vector and replaces non-null sample values with evaluation step timestamps (T_eval).
    return applyTimestampToGeneralExpression(function_node, std::move(arguments[0]), context);
}

}
