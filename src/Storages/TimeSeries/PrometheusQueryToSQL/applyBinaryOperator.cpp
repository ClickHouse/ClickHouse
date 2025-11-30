#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        const SQLQueryPiece & left_argument,
        const SQLQueryPiece & right_argument,
        const ConverterContext & context)
    {
        if (!(left_argument.type == ResultType::SCALAR || left_argument.type == ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR, context.promql_tree.getQuery(node), argument.type);
        }

        if (!(right_argument.type == ResultType::SCALAR || right_argument.type == ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR, context.promql_tree.getQuery(node), argument.type);
        }
    }

    Float64 evaluateConstBinaryOperator(std::string_view operator_name, Float64 left, Float64 right)
    {
        if (operator_name == "+")
        {
            return left + right;
        }
        else if (operator_name == "-")
        {
            return left - right;
        }
        else if (operator_name == "*")
        {
            return left * right;
        }
        else if (operator_name == "/")
        {
            return left / right;
        }
        else if (operator_name == "%")
        {
            return fmod(left, right);
        }
        else if (operator_name == "^")
        {
            return pow(left, right);
        }
        else if (operator_name == "atan2")
        {
            return atan2(left, right);
        }
        else if (operator_name == "==")
        {
            return left == right;
        }
        else if (operator_name == "!=")
        {
            return left != right;
        }
        else if (operator_name == ">")
        {
            return left > right;
        }
        else if (operator_name == "<")
        {
            return left < right;
        }
        else if (operator_name == ">=")
        {
            return left >= right;
        }
        else if (operator_name == "<=")
        {
            return left <= right;
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operator '{}' is not implemented", operator_name);
        }
    }

    bool isComparisonWithoutBool(const PrometheusQueryTree::BinaryOperator * operator_node)
    {
        if (operator_node->bool_modifier)
            return false;

        std::string_view operator_name = operator_node->operator_name;
        return (operator_name == "==") || ()
    }

    ASTPtr makeTransformAST(std::string_view operator_name, ASTPtr left_argument, ASTPtr right_argument)
    {
        static const std::unordered_map<std::string_view, std::string_view> function_names = {
            {"+", "plus"},
            {"-", "minus"},
            {"*", "multiply"},
            {"/", "divide"},
            {"%", "modulo"},
            {"^", "pow"},
            {"atan2", "atan2"},
            {"==", "equals"},
            {"!=", "notEquals"},
            {">", "greater"},
            {"<", "less"},
            {">=", "greaterOrEquals"},
            {"<=", "lessOrEquals"},
        };
        auto it = function_names.find(operator_name);
        if (it == function_names.end())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operator '{}' is not implemented", operator_name);
        return makeASTFunction(function_name, left_argument, right_argument);
    }

    /// Applies a binary operator if one of the arguments is a SCALAR represented by StoreMethod::CONST_SCALAR,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can be either SCALAR or INSTANT_VECTOR with any store method of
    /// {StoreMethod::CONST_SCALAR, StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithScalarByConstScalar(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        size_t argument_index,
        ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;
        bool is_comparison_without_bool = isComparisonWithoutBool(operator_node);

        const SQLQueryPiece & scalar_argument = argument_index ? right_argument : left_argument;
        chassert((scalar_argument.type == ResultType::SCALAR) && (scalar_argument.store_method == StoreMethod::CONST_SCALAR));
    
        const SQLQueryPiece & other_argument = argument_index ? left_argument : right_argument;
        auto other_type = other_argument.type;
        auto other_store_method = other_argument.store_method;

        SQLQueryPiece res = other_argument;
        res.node = operator_node;

        switch (other_store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                Float64 const_result = evaluateConstBinaryOperator(operator_name, left_argument.scalar_value, right_argument.scalar_value);
                if (is_comparison_without_bool && (res.type == ResultType::INSTANT_VECTOR))
                {
                    if (const_result)
                        return res;
                    else
                        return SQLQueryPiece{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
                }
                res.scalar_value = const_result;
                return res;
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                /// Case 1: other_store_method == SCALAR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT 0 AS group, arrayMap(x -> if (x == <scalar_value>, x, NULL), values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 2: other_store_method == SCALAR_GRID, other operators (e.g. +):
                /// SELECT arrayMap(x -> x + <scalar_value>, values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 3: other_store_method == VECTOR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT group, arrayMap(x -> if (x == <scalar_value>, x, NULL), values) AS values
                /// FROM <other_vector_grid>
                ///
                /// Case 4: other_store_method == VECTOR_GRID, other operators (e.g. +):
                /// SELECT group, arrayMap(x -> x + <scalar_value>, values) AS values
                /// FROM <other_vector_grid>

                SelectQueryParams params;

                auto scalar_value = scalar_argument.scalar_value;

                if (other_store_method == StoreMethod::VECTOR_GRID)
                {
                    params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
                }
                else if (is_comparison_without_bool && (res.type == ResultType::INSTANT_VECTOR))
                {
                    params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
                    params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);
                    res.store_method = StoreMethod::VECTOR_GRID;
                    res.metric_name_dropped = true;
                }

                ASTPtr left_ast = argument_index ? std::make_shared<ASTIdentifier>("x") : std::make_shared<ASTLiteral>(scalar_value);
                ASTPtr right_ast = argument_index ? std::make_shared<ASTLiteral>(scalar_value) : std::make_shared<ASTIdentifier>("x");

                ASTPtr transform_ast = makeTransformAST(operator_name, left_ast, right_ast);

                if (is_comparison_without_bool && (res.type == ResultType::INSTANT_VECTOR))
                {
                    ASTPtr value_if_match = std::make_shared<ASTIdentifier>("x");
                    params.select_list.push_back(makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                            makeASTFunction("if", transform_ast, value_if_match, std::make_shared<ASTLiteral>(Field{})),
                            std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values))));
                }
                else
                {
                    params.select_list.push_back(makeASTFunction(
                        "arrayMap",
                        makeASTFunction("lambda", makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")), transform_ast),
                        std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values)));
                }
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(other_argument.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = context.subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));

                if (!is_comparison_without_bool)
                    res = dropMetricName(std::move(res), context);

                return res;
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Argument #{} of the operator '{}' in expression {} has unexpected type {} (store_method: {})",
                                2 - argument_index, operator_name, getPromQLQuery(other_argument, context), other_type, other_store_method);
            }
        }

        UNREACHABLE();
    }


    /// Applies a binary operator if one of the arguments is a SCALAR represented by StoreMethod::SCALAR_GRID,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can be either SCALAR or INSTANT_VECTOR with any store method of
    /// {StoreMethod::CONST_SCALAR, StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithScalarByScalarGrid(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        size_t argument_index,
        ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;
        bool is_comparison_without_bool = isComparisonWithoutBool(operator_node);

        const SQLQueryPiece & scalar_argument = argument_index ? right_argument : left_argument;
        chassert((scalar_argument.type == ResultType::SCALAR) && (scalar_argument.store_method == StoreMethod::SCALAR_GRID));

        const SQLQueryPiece & other_argument = argument_index ? left_argument : right_argument;
        auto other_type =  other_argument.type;
        auto other_store_method = other_argument.store_method;

        SQLQueryPiece res = other_argument;
        res.node = operator_node;

        switch (other_store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                if (other_type != ResultType::INSTANT_VECTOR)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Argument #{} of the operator '{}' has wrong type {}", 2 - argument_index, operator_name, other_type);

                auto other_scalar_value = other_argument.scalar_value;
                res.store_method = StoreMethod::SCALAR_GRID;

                /// Case 1: Comparison operator (e.g. ==) without bool modifier:
                /// SELECT 0 AS group, arrayMap(x -> if (x == <other_scalar_value>, <other_scalar_value>, NULL), values) AS values
                /// FROM <scalar_grid>
                ///
                /// Case 2: Other operators (e.g. +):
                /// SELECT arrayMap(x -> x + <other_scalar_value>, values) AS values
                /// FROM <scalar_grid>
                SelectQueryParams params;

                chassert(res.type == ResultType::INSTANT_VECTOR);
                if (is_comparison_without_bool)
                {
                    params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
                    params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);
                    res.store_method = StoreMethod::VECTOR_GRID;
                    res.metric_name_dropped = true;
                }

                ASTPtr left_ast = argument_index ? std::make_shared<ASTLiteral>(other_scalar_value) : std::make_shared<ASTIdentifier>("x");
                ASTPtr right_ast = argument_index ? std::make_shared<ASTIdentifier>("x") : std::make_shared<ASTLiteral>(other_scalar_value);

                ASTPtr transform_ast = makeTransformAST(operator_name, left_ast, right_ast);

                if (is_comparison_without_bool)
                {
                    ASTPtr value_if_match = std::make_shared<ASTLiteral>(other_scalar_value);
                    params.select_list.push_back(makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                            makeASTFunction("if", transform_ast, value_if_match, std::make_shared<ASTLiteral>(Field{})),
                            std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values))));
                }
                else
                {
                    params.select_list.push_back(makeASTFunction(
                        "arrayMap",
                        makeASTFunction("lambda", makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")), transform_ast),
                        std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values)));
                }
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = context.subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));

                if (!is_comparision_without_null)
                    res = dropMetricName(std::move(res), context);

                return res;
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                /// Case 1: other_store_method == SCALAR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT 0 AS group, arrayMap(x, y -> if (x == y, x, NULL), values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 2: other_store_method == SCALAR_GRID, other operators (e.g. +):
                /// SELECT arrayMap(x, y -> x + y, <scalar_grid>, values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 3: other_store_method == VECTOR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT group, arrayMap(x, y -> if (x == y, x, NULL), <scalar_grid>, values) AS values
                /// FROM <other_vector_grid>
                ///
                /// Case 4: other_store_method == VECTOR_GRID, other operators (e.g. +):
                /// SELECT group, arrayMap(x, y -> x + y, <scalar_grid>, values) AS values
                /// FROM <other_vector_grid>
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR});
                String scalar_grid = context.subqueries.back().name;

                SelectQueryParams params;

                bool is_comparison_without_bool = isComparisonWithoutBool(operator_node);

                if (store_method == StoreMethod::VECTOR_GRID)
                {
                    params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
                }
                else if (is_comparison_without_bool && (res.type == StoreMethod::INSTANT_VECTOR))
                {
                    params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
                    params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);
                    res.store_method = StoreMethod::VECTOR_GRID;
                    res.metric_name_dropped = true;
                }

                ASTPtr transform_ast = makeTransformAST(operator_name, std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y"));
                ASTPtr left_ast = std::make_shared<ASTIdentifier>(argument_index ? scalar_grid : TimeSeriesColumnNames::Values);
                ASTPtr right_ast = std::make_shared<ASTIdentifier>(argument_index ? TimeSeriesColumnNames::Values : scalar_grid);

                if (is_comparison_without_bool && (res.type == StoreMethod::INSTANT_VECTOR))
                {
                    ASTPtr value_if_match = std::make_shared<ASTIdentifier>(argument_index ? "x" : "y");
                    params.select_list.push_back(makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                            makeASTFunction("if", transform_ast, value_if_match, std::make_shared<ASTLiteral>(Field{})),
                            std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values))));
                }
                else
                {
                    params.select_list.push_back(makeASTFunction(
                        "arrayMap",
                        makeASTFunction(
                            "lambda",
                            makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                            transform_ast),
                        std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values)));
                }
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(other_argument.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = context.subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));

                if (!is_comparision_without_null)
                    res = dropMetricName(std::move(res), context);

                return res;
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Argument #{} of the operator '{}' in expression {} has unexpected type {} (store_method: {})",
                                2 - argument_index, operator_name, getPromQLQuery(other_argument, context), other_type, other_store_method);
            }
        }

        UNREACHABLE();
    }


    /// Applies a binary operator both of the arguments are instant vectors, and one of the arguments is represented by StoreMethod::CONST_SCALAR,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can use any store method of {StoreMethod::CONST_SCALAR, StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithVectorsByConstScalar(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        size_t argument_index,
        ConverterContext & context)
    {

    }


    /// Applies a binary operator both of the arguments are instant vectors, and one of the arguments is represented by StoreMethod::SCALAR_GRID,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can use any store method of {StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithVectorsByScalarGrid(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        size_t argument_index,
        ConverterContext & context)
    {

    }


    /// Applies a binary operator both of the arguments are instant vectors represented by StoreMethod::VECTOR_GRID.
    SQLQueryPiece binaryOperatorWithVectorsByVectorGrids(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        size_t argument_index,
        ConverterContext & context)
    {

    }
}


SQLQueryPiece applyBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);
    const auto & operator_name = operator_node->operator_name;

    if ((left_argument.store_method == StoreMethod::EMPTY) || (right_argument.store_method == StoreMethod::EMPTY))
    {
        /// If one of the arguments has no data, the result also has no data.
        return SQLQueryPiece(operator_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY);
    }

    /// Check if one of the arguments is scalar.
    if ((left_argument.type == ResultType::SCALAR) && (left_argument.store_method == StoreMethod::CONST_SCALAR))
        return binaryOperatorWithScalarByConstScalar(operator_node, left_argument, right_argument, 0, context);

    if ((right_argument.type == ResultType::SCALAR) && (right_argument.store_method == StoreMethod::CONST_SCALAR))
        return binaryOperatorWithScalarByConstScalar(operator_node, left_argument, right_argument, 1, context);

    if ((left_argument.type == ResultType::SCALAR) && (left_argument.store_method == StoreMethod::SCALAR_GRID))
        return binaryOperatorWithScalarByScalarGrid(operator_node, left_argument, right_argument, 0, context);

    if ((right_argument.type == ResultType::SCALAR) && (right_argument.store_method == StoreMethod::SCALAR_GRID))
        return binaryOperatorWithScalarByScalarGrid(operator_node, left_argument, right_argument, 1, context);

    /// Both of the arguments are instant vectors.
    /// (The arguments can be either scalars or instant vectors and we've checked scalars already.)
    chassert(left_argument.type == ResultType::INSTANT_VECTOR);
    chassert(right_argument.type == ResultType::INSTANT_VECTOR);
    
    if (left_argument.store_method == StoreMethod::CONST_SCALAR)
        return binaryOperatorWithVectorsByConstScalar(operator_node, left_argument, right_argument, 0, context);

    if (right_argument.store_method == StoreMethod::CONST_SCALAR)
        return binaryOperatorWithVectorsByConstScalar(operator_node, left_argument, right_argument, 1, context);

    if (left_argument.store_method == StoreMethod::SCALAR_GRID)
        return binaryOperatorWithVectorsByScalarGrid(operator_node, left_argument, right_argument, 0, context);

    if (right_argument.store_method == StoreMethod::SCALAR_GRID)
        return binaryOperatorWithVectorsByScalarGrid(operator_node, left_argument, right_argument, 1, context);

    if (left.argument.store_method != StoreMethod::VECTOR_GRID)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Left argument of the operator '{}' in expression {} has unexpected type {} (store_method: {})",
                        operator_name, getPromQLQuery(left_argument, context), left_argument.type, left_argument.store_method);
    }

    if (right.argument.store_method != StoreMethod::VECTOR_GRID)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Right argument of the operator '{}' in expression {} has unexpected type {} (store_method: {})",
                        operator_name, getPromQLQuery(right_argument, context), right_argument.type, right_argument.store_method);
    }

    return binaryOperatorWithVectorsByVectorGrids(operator_node, left_argument, right_argument, context);
}

}
