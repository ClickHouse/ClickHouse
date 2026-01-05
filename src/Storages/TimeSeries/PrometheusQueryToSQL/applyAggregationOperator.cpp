#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Returns true if the aggregation operator takes two arguments.
    /// (Some aggregation operators take one argument and others take two arguments.)
    bool isTwoArgumentsAggregationOperator(std::string_view operator_name)
    {
        static const std::vector<std::string_view> two_arguments_operators = {
            "bottomk", "topk", "limitk", "limit_ratio", "count_values", "quantile"
        };
        return std::find(two_arguments_operators.begin(), two_arguments_operators.end(), operator_name) != two_arguments_operators.end();
    }

    /// Checks if the types of the specified arguments are valid for the aggregation operator.
    void checkArgumentTypes(
        const PQT::AggregationOperator * operator_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        std::string_view operator_name = operator_node->operator_name;

        size_t expected_number_of_arguments = isTwoArgumentsAggregationOperator(operator_name) ? 2 : 1;

        if (arguments.size() != expected_number_of_arguments)
        {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Operator '{}' expects {} {}, got {} arguments",
                                operator_name, expected_number_of_arguments, (expected_number_of_arguments == 1 ? "argument" : "arguments"),
                                arguments.size());
        }

        if (expected_number_of_arguments == 2)
        {
            size_t scalar_argument_index = 0;
            const auto & scalar_argument = arguments[scalar_argument_index];
            if (scalar_argument.type != ResultType::SCALAR)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Operator '{}' expects argument #{} of type {}, but expression {} has type {}",
                                operator_name, scalar_argument_index + 1, ResultType::SCALAR,
                                getPromQLQuery(scalar_argument, context), scalar_argument.type);
            }
        }

        size_t vector_argument_index = expected_number_of_arguments - 1;
        const auto & vector_argument = arguments[vector_argument_index];
        if (vector_argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Operator '{}' expects argument #{} of type {}, but expression {} has type {}",
                            operator_name, ResultType::INSTANT_VECTOR, vector_argument_index + 1,
                            getPromQLQuery(vector_argument, context), vector_argument.type);
        }
    }

    /// Returns an AST to evaluate the group which we use for aggregation.
    ASTPtr makeExpressionForAggregationGroup(
        const PQT::AggregationOperator * operator_node,
        ASTPtr && argument_group,
        bool metric_name_dropped_from_argument_group,
        bool & metric_name_dropped_from_aggregation_group)
    {
        /// Group #0 always means a group with no tags.
        if (const auto * literal = argument_group->as<const ASTLiteral>(); literal && literal->value == Field{0u})
        {
            metric_name_dropped_from_aggregation_group = true;
            return std::move(argument_group);
        }

        if (operator_node->by)
        {
            if (operator_node->labels.empty())
            {
                /// BY() means we ignore all tags.
                metric_name_dropped_from_aggregation_group = true;
                return std::make_shared<ASTLiteral>(0u);
            }
            else
            {
                /// BY(tags) means we ignore all tags except the specified ones.
                /// If the metric name "__name__" is among the tags in BY(tags) we don't remove it from the result group.

                /// timeSeriesRemoveAllTagsExcept(argument_group, by_tags)
                Strings tags_to_keep = operator_node->labels;
                std::sort(tags_to_keep.begin(), tags_to_keep.end());
                tags_to_keep.erase(std::unique(tags_to_keep.begin(), tags_to_keep.end()), tags_to_keep.end());

                metric_name_dropped_from_aggregation_group = !std::binary_search(tags_to_keep.begin(), tags_to_keep.end(), kMetricName);

                return makeASTFunction(
                           "timeSeriesRemoveAllTagsExcept",
                           std::move(argument_group),
                           std::make_shared<ASTLiteral>(Array{tags_to_keep.begin(), tags_to_keep.end()}));
            }
        }
        else if (operator_node->without)
        {
            /// WITHOUT(tags) means we ignore the specified tags, and also the metric name "__name__".

            /// timeSeriesRemoveTags(argument_group, without_tags + ['__name__'])
            Strings tags_to_remove = operator_node->labels;
            if (!metric_name_dropped_from_argument_group && (std::find(tags_to_remove.begin(), tags_to_remove.end(), kMetricName) == tags_to_remove.end()))
                tags_to_remove.push_back(kMetricName);
            std::sort(tags_to_remove.begin(), tags_to_remove.end());
            tags_to_remove.erase(std::unique(tags_to_remove.begin(), tags_to_remove.end()), tags_to_remove.end());

            metric_name_dropped_from_aggregation_group = true;

            return makeASTFunction(
                       "timeSeriesRemoveTags",
                       std::move(argument_group),
                       std::make_shared<ASTLiteral>(Array{tags_to_remove.begin(), tags_to_remove.end()}));
        }
        else
        {
            /// Neither BY() nor WITHOUT() keywords are specified, so we ignore all tags.
            metric_name_dropped_from_aggregation_group = true;
            return std::make_shared<ASTLiteral>(0u);
        }
    }

    /// Returns an AST to perform the aggregation.
    ASTPtr makeExpressionForAggregatedValues(const PQT::AggregationOperator * operator_node, ASTPtr && vector_argument, ASTPtr && scalar_argument)
    {
        const auto & operator_name = operator_node->operator_name;

        if (operator_name == "count")
        {
            /// We use sumForEach() and not countForEach() here because we want to retain NULLs and not to convert them into zeros.
            /// sumForEach(arrayMap(x-> if(isNull(x), NULL, 1), values))
            return makeASTFunction(
                "sumForEach",
                makeASTFunction(
                    "arrayMap",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                        makeASTFunction(
                            "if",
                            makeASTFunction("isNull", std::make_shared<ASTIdentifier>("x")),
                            std::make_shared<ASTLiteral>(Field{} /* NULL */),
                            std::make_shared<ASTLiteral>(1u))),
                    std::move(vector_argument)));
        }
        else if (operator_name == "count_values")
        {
            chassert(scalar_argument);
            if (scalar_argument->as<ASTIdentifier>())
            {
                /// sumForEach(arrayMap(x-> if(x == y, 1, NULL), v, <scalar>))
                return makeASTFunction(
                    "sumForEach",
                    makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                            makeASTFunction(
                                "if",
                                makeASTFunction("equals", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                                std::make_shared<ASTLiteral>(1u),
                                std::make_shared<ASTLiteral>(Field{} /* NULL */))),
                        std::move(vector_argument),
                        std::move(scalar_argument)));
            }
            else
            {
                /// sumForEach(arrayMap(x-> if(x == <scalar>, 1, NULL), v))
                return makeASTFunction(
                    "sumForEach",
                    makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                            makeASTFunction(
                                "if",
                                makeASTFunction("equals", std::make_shared<ASTIdentifier>("x"), std::move(scalar_argument)),
                                std::make_shared<ASTLiteral>(1u),
                                std::make_shared<ASTLiteral>(Field{} /* NULL */))),
                        std::move(vector_argument)));
            }
        }
        else if (operator_name == "group")
        {
            /// anyForEach(arrayMap(x-> if(isNull(x), NULL, 1), values))
            return makeASTFunction(
                "anyForEach",
                makeASTFunction(
                    "arrayMap",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                        makeASTFunction(
                            "if",
                            makeASTFunction("isNull", std::make_shared<ASTIdentifier>("x")),
                            std::make_shared<ASTLiteral>(Field{} /* NULL */),
                            std::make_shared<ASTLiteral>(1u))),
                    std::move(vector_argument)));
        }
        else
        {
            static const std::unordered_map<String, String> sql_function_names{
                {"sum", "sumForEach"},
                {"avg", "avgForEach"},
                {"min", "minForEach"},
                {"max", "maxForEach"},
                {"stddev", "stddevPopStable"},
                {"stdvar", "varPopStable"},
            };
            auto it = sql_function_names.find(operator_name);
            if (it == sql_function_names.end())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregation operator {} is not implemented", operator_name);
            const auto & sql_function_name = it->second;

            return makeASTFunction(sql_function_name, std::move(vector_argument));
        }
    }
}


SQLQueryPiece applyAggregationOperator(
    const PQT::AggregationOperator * operator_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context)
{
    checkArgumentTypes(operator_node, arguments, context);

    auto vector_argument = std::move(arguments.back());

    SQLQueryPiece res = vector_argument;
    res.node = operator_node;

    if (vector_argument.store_method == StoreMethod::EMPTY)
    {
        /// If the vector argument has no data, the result also has no data.
        return res;
    }

    vector_argument = toVectorGrid(std::move(vector_argument), context);

    ASTPtr scalar_ast;
    if (arguments.size() >= 2)
    {
        auto scalar_argument = std::move(arguments.front());
        if (scalar_argument.store_method == StoreMethod::CONST_SCALAR)
        {
            scalar_ast = std::make_shared<ASTLiteral>(scalar_argument.scalar_value);
        }
        else
        {
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR});
            scalar_ast = std::make_shared<ASTIdentifier>(context.subqueries.back().name);
        }
    }

    /// SELECT timeSeriesRemoveAllTagsExcept(group, by_tags) AS group,
    ///        sumForEach(values) AS values
    /// FROM <vector_grid>
    /// GROUP BY group
    SelectQueryParams params;

    bool metric_name_dropped_from_result;
    params.select_list.push_back(makeExpressionForAggregationGroup(operator_node,
        std::make_shared<ASTIdentifier>(ColumnNames::Group),
        vector_argument.metric_name_dropped,
        metric_name_dropped_from_result));

    params.select_list.back()->setAlias(ColumnNames::Group);

    params.select_list.push_back(
        makeExpressionForAggregatedValues(operator_node, std::make_shared<ASTIdentifier>(ColumnNames::Values), std::move(scalar_ast)));

    params.select_list.back()->setAlias(ColumnNames::Values);

    auto & subqueries = context.subqueries;
    subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(vector_argument.select_query), SQLSubqueryType::TABLE});
    params.from_table = subqueries.back().name;

    params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

    res.select_query = buildSelectQuery(std::move(params));
    res.metric_name_dropped = metric_name_dropped_from_result;
    
    return res;
}

}
