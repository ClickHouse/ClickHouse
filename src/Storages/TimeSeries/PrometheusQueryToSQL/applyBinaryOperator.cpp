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

    Float64 evaluateConstOperator(std::string_view operator_name, Float64 left, Float64 right)
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

    Float64 evaluateConstOperator(std::string_view operator_name, Float64 argument, Float64 other_argument, bool left_to_right)
    {
        if (left_to_right)
            return evaluateConstOperator(operator_name, argument, other_argument);
        else
            return evaluateConstOperator(operator_name, other_argument, argument);
    }

    ASTPtr makeOperatorAST(std::string_view operator_name, ASTPtr left_argument, ASTPtr right_argument)
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

    /// Returns an AST for evaluating a binary operator, for example "plus(argument, other_argument)".
    ASTPtr makeOperatorAST(std::string_view operator_name, ASTPtr argument, ASTPtr other_argument, bool left_to_right)
    {
        if (left_to_right)
            return makeOperatorAST(operator_name, argument, other_argument);
        else
            return makeOperatorAST(operator_name, other_argument, argument);
    }

    /// Comparison operators without BOOL modifier work as filters and require special handling.
    bool isComparisonWithoutBool(const PrometheusQueryTree::BinaryOperator * operator_node)
    {
        if (operator_node->bool_modifier)
            return false;

        static const std::unordered_set<std::string_view> comparison_operators = {
            "==", "!=", ">", "<", ">=", "<="
        };
        return comparison_operators.contains(operator_node->operator_name);
    }

    /// Returns an AST for evaluating a binary operator on a scalar and each element of an array.
    /// For example, the function can return
    /// arrayMap(x -> scalar_argument + x, array_argument)
    ASTPtr makeOperatorASTForScalarAndArray(std::string_view operator_name, Float64 scalar_argument, ASTPtr array_argument, bool left_to_right)
    {
        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                makeOperatorAST(
                    operator_name, std::make_shared<ASTLiteral>(scalar_argument), std::make_shared<ASTIdentifier>("x"), left_to_right),
                array_argument));
    }

    ASTPtr makeOperatorASTForArrayAndScalar(std::string_view operator_name, ASTPtr array_argument, Float64 scalar_argument, bool left_to_right)
    {
        return makeOperatorASTForScalarAndArray(operator_name, scalar_argument, array_argument, !left_to_right);
    }

    /// Returns an AST for evaluating a binary operator on corresponding elements of two arrays.
    /// For example, the function can return
    /// arrayMap(x, y -> x + y, array_argument, other_array_argument)
    ASTPtr makeOperatorASTForTwoArrays(std::string_view operator_name, ASTPtr array_argument, ASTPtr other_array_argument, bool left_to_right)
    {
        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                makeOperatorAST(operator_name, std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y"), left_to_right),
                array_argument,
                other_array_argument));
    }

    /// Returns an AST for evaluating a filter based on a comparison operator on a scalar and each element of an array.
    /// For example, the function can return
    /// arrayMap(x -> if (scalar_argument >= x, x, NULL), array_argument)
    ASTPtr makeFilterASTForScalarAndArray(
        std::string_view operator_name,
        Float64 scalar_argument,
        ASTPtr array_argument,
        bool left_to_right,
        bool return_elements_of_array_argument_if_match)
    {
        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                makeASTFunction(
                    "if",
                    makeOperatorAST(
                        operator_name, std::make_shared<ASTLiteral>(scalar_argument), std::make_shared<ASTIdentifier>("x"), left_to_right),
                    return_elements_of_array_argument_if_match ? std::make_shared<ASTIdentifier>("x")
                                                               : std::make_shared<ASTLiteral>(scalar_argument))),
            array_argument);
    }

    ASTPtr makeFilterASTForArrayAndScalar(
        std::string_view operator_name,
        ASTPtr array_argument,
        Float64 scalar_argument,
        bool left_to_right,
        bool return_scalar_argument_if_match)
    {
        return makeFilterASTForScalarAndArray(operator_name, scalar_argument, array_argument, !left_to_right, !return_scalar_argument_if_match);
    }

    /// Returns an AST for evaluating a filter based on a comparison operator on two arrays.
    /// For example, the function can return
    /// arrayMap(x, y -> if (x >= y, y, NULL), array_argument, other_array_argument)
    ASTPtr makeFilterASTForTwoArrays(
        std::string_view operator_name,
        ASTPtr array_argument,
        ASTPtr other_array_argument,
        bool left_to_right,
        bool return_elements_of_other_array_argument_if_match)
    {
        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                makeASTFunction(
                    "if",
                    makeOperatorAST(
                        operator_name, std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y"), left_to_right),
                    std::make_shared<ASTIdentifier>(return_elements_of_other_array_argument_if_match ? "y" : "x"))),
            array_argument,
            other_array_argument);
    }


    /// Applies a binary operator if one of the arguments is a SCALAR represented by StoreMethod::CONST_SCALAR,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can be either SCALAR or INSTANT_VECTOR with any store method of
    /// {StoreMethod::CONST_SCALAR, StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithScalarByConstScalar(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && argument,
        SQLQueryPiece && other_argument,
        bool left_to_right,
        ConverterContext & context)
    {
        chassert((argument.type == ResultType::SCALAR) && (argument.store_method == StoreMethod::CONST_SCALAR));

        const auto & operator_name = operator_node->operator_name;
        bool is_comparison_without_bool = isComparisonWithoutBool(operator_node);
        auto scalar_value = argument.scalar_value;
        auto other_type = other_argument.type;
        auto other_store_method = other_argument.store_method;

        SQLQueryPiece res = other_argument;
        res.node = operator_node;

        switch (other_store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                Float64 float_result = evaluateConstOperator(operator_name, scalar_value, other_argument.scalar_value, left_to_right);
                if (is_comparison_without_bool && (res.type == ResultType::INSTANT_VECTOR))
                {
                    if (float_result)
                        return res;
                    else
                        return SQLQueryPiece{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
                }
                res.scalar_value = float_result;
                return res;
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                /// Case 1: other_store_method == SCALAR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT 0 AS group, arrayMap(x -> if (<scalar_value> == x, x, NULL), values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 2: other_store_method == SCALAR_GRID, other operators (e.g. +):
                /// SELECT arrayMap(x -> <scalar_value> + x, values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 3: other_store_method == VECTOR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT group, arrayMap(x -> if (<scalar_value> == x, x, NULL), values) AS values
                /// FROM <other_vector_grid>
                ///
                /// Case 4: other_store_method == VECTOR_GRID, other operators (e.g. +):
                /// SELECT group, arrayMap(x -> <scalar_value> + x, values) AS values
                /// FROM <other_vector_grid>

                SelectQueryParams params;

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

                if (is_comparison_without_bool && (res.type == ResultType::INSTANT_VECTOR))
                {
                    params.select_list.push_back(makeFilterASTForScalarAndArray(
                        operator_name, scalar_value, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), left_to_right,
                        /* return_elements_of_array_argument_if_match = */ true));
                }
                else
                {
                    params.select_list.push_back(makeOperatorASTForScalarAndArray(
                        operator_name, scalar_value, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), left_to_right));
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
                                "Other argument of the operator '{}' in expression {} has unexpected type {} (store_method: {})",
                                operator_name, getPromQLQuery(other_argument, context), other_type, other_store_method);
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
        SQLQueryPiece && argument,
        SQLQueryPiece && other_argument,
        bool left_to_right,
        ConverterContext & context)
    {
        chassert((argument.type == ResultType::SCALAR) && (argument.store_method == StoreMethod::SCALAR_GRID));
        chassert(!((other_argument.type == ResultType::SCALAR) && (other_argument.store_method == StoreMethod::CONST_SCALAR)));

        const auto & operator_name = operator_node->operator_name;
        bool is_comparison_without_bool = isComparisonWithoutBool(operator_node);
        auto other_type = other_argument.type;
        auto other_store_method = other_argument.store_method;

        SQLQueryPiece res = other_argument;
        res.node = operator_node;

        switch (other_store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                /// This case must be already handled - see the code of applyBinaryOperator().
                chassert(other_type == ResultType::INSTANT_VECTOR);

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

                if (is_comparison_without_bool)
                {
                    params.select_list.push_back(makeFilterASTForArrayAndScalar(
                        operator_name, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), other_scalar_value, left_to_right,
                        /* return_scalar_argument_if_match = */ true));
                }
                else
                {
                    params.select_list.push_back(
                        makeOperatorASTForArrayAndScalar(
                        operator_name, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), other_scalar_value, left_to_right));
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
                /// SELECT 0 AS group, arrayMap(x, y -> if (x == y, y, NULL), <scalar_grid>, values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 2: other_store_method == SCALAR_GRID, other operators (e.g. +):
                /// SELECT arrayMap(x, y -> x + y, <scalar_grid>, values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 3: other_store_method == VECTOR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT group, arrayMap(x, y -> if (x == y, y, NULL), <scalar_grid>, values) AS values
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

                if (is_comparison_without_bool && (res.type == StoreMethod::INSTANT_VECTOR))
                {
                    params.select_list.push_back(makeFilterASTForTwoArrays(
                        operator_name,
                        std::make_shared<ASTIdentifier>(scalar_grid),
                        std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values),
                        left_to_right,
                        /* return_elements_of_other_array_argument_if_match = */ true));
                }
                else
                {
                    params.select_list.push_back(makeOperatorASTForTwoArrays(
                        operator_name,
                        std::make_shared<ASTIdentifier>(scalar_grid),
                        std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values),
                        left_to_right));
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


    /// Returns an AST to evaluate the join group which is a parameter we join both sides of a binary operator on instant vectors.
    ASTPtr makeJoinGroupAST(const PrometheusQueryTree::BinaryOperator * operator_node, ASTPtr group)
    {
        if (const auto * literal = arg->as<const ASTLiteral>(); literal && literal->value == Field{0u})
            return group;

        if (operator_node->on)
        {
            if (operator_node->labels.empty())
                return std::make_shared<ASTLiteral>(0u);
            else
                return makeASTFunction("timeSeriesRemoveAllTagsExcept", group,
                    std::make_shared<ASTLiteral>(Array{operator_node->labels.begin(), operator_node->labels.end()}));
        }
        else if (operator_node->ignoring && !operator_node->labels.empty())
        {
            return makeASTFunction("timeSeriesRemoveTags", group,
                std::make_shared<ASTLiteral>(Array{operator_node->labels.begin(), operator_node->labels.end()}));
        }
        else
        {
            return group;
        }
    }

    /// Returns an AST to evaluate the group which will be set for the result of a binary operator on instant vectors.
    ASTPtr makeResultGroupAST(const PrometheusQueryTree::BinaryOperator * operator_node, ASTPtr left_group, ASTPtr right_group, ASTPtr join_group)
    {
        if (group_left && 
    }


    /// Applies a binary operator both of the arguments are instant vectors, and one of the arguments is represented by StoreMethod::CONST_SCALAR,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can use any store method of {StoreMethod::CONST_SCALAR, StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithVectorsByConstScalar(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && argument,
        SQLQueryPiece && other_argument,
        bool left_to_right,
        ConverterContext & context)
    {
        chassert((argument.type == ResultType::INSTANT_VECTOR) && (other_argument.type == ResultType::INSTANT_VECTOR));
        chassert((argument.store_method == StoreMethod::CONST_SCALAR));

        const auto & operator_name = operator_node->operator_name;
        bool is_comparison_without_bool = isComparisonWithoutBool(operator_node);
        auto other_store_method = other_argument.store_method;

        SQLQueryPiece res = other_argument;
        res.node = operator_node;

        switch (other_store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                Float64 float_result = evaluateConstBinaryOperator(operator_name, argument.scalar_value, other_argument.scalar_value, left_to_right);
                if (is_comparison_without_bool)
                {
                    if (float_result)
                        return getLeftArgument(argument, other_argument, left_to_right);
                    else
                        return SQLQueryPiece{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY};
                }
                res.scalar_value = float_result;
                return res;
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                /// Case 1: other_store_method == SCALAR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT 0 AS group, arrayMap(x -> if (<scalar_value> == x, <scalar_value>, NULL), values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 2: other_store_method == SCALAR_GRID, other operators (e.g. +):
                /// SELECT arrayMap(x -> <scalar_value> + x, values) AS values
                /// FROM <other_scalar_grid>
                ///
                /// Case 3: other_store_method == VECTOR_GRID, comparison operator (e.g. ==) without bool modifier:
                /// SELECT copyTags(removeAllTagsExcept(group, on_tags) AS join_group, group, tags_to_copy) AS group,
                ///        arrayMap(x -> if (<scalar_value> == x, x, NULL), values) AS values
                /// FROM <other_vector_grid>
                /// WHERE join_group == 0
                ///
                /// Case 4: other_store_method == VECTOR_GRID, other operators (e.g. +):
                /// SELECT group, arrayMap(x -> <scalar_value> + x, values) AS values
                /// FROM <other_vector_grid>
            }

            case StoreMethod::VECTOR_GRID:
            {

            }
        }
    }


    /// Applies a binary operator both of the arguments are instant vectors, and one of the arguments is represented by StoreMethod::SCALAR_GRID,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can use any store method of {StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithVectorsByScalarGrid(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && argument,
        SQLQueryPiece && other_argument,
        bool left_to_right,
        ConverterContext & context)
    {
        chassert((argument.type == ResultType::INSTANT_VECTOR) && (other_argument.type == ResultType::INSTANT_VECTOR));
        chassert(argument.store_method == StoreMethod::SCALAR_GRID);
        chassert(other_argument.store_method != StoreMethod::CONST_SCALAR);

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {

            }
    }


    /// Applies a binary operator both of the arguments are instant vectors represented by StoreMethod::VECTOR_GRID.
    SQLQueryPiece binaryOperatorWithVectorsByVectorGrids(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && argument,
        SQLQueryPiece && other_argument,
        bool left_to_right,
        ConverterContext & context)
    {
        inner join
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
        return binaryOperatorWithScalarByConstScalar(operator_node, std::move(left_argument), std::move(right_argument), true, context);

    if ((right_argument.type == ResultType::SCALAR) && (right_argument.store_method == StoreMethod::CONST_SCALAR))
        return binaryOperatorWithScalarByConstScalar(operator_node, std::move(right_argument), std::move(left_argument), false, context);

    if ((left_argument.type == ResultType::SCALAR) && (left_argument.store_method == StoreMethod::SCALAR_GRID))
        return binaryOperatorWithScalarByScalarGrid(operator_node, std::move(left_argument), std::move(right_argument), true, context);

    if ((right_argument.type == ResultType::SCALAR) && (right_argument.store_method == StoreMethod::SCALAR_GRID))
        return binaryOperatorWithScalarByScalarGrid(operator_node, std::move(right_argument), std::move(left_argument), false, context);

    /// Both of the arguments are instant vectors.
    /// (The arguments can be either scalars or instant vectors and we've checked scalars already.)
    chassert(left_argument.type == ResultType::INSTANT_VECTOR);
    chassert(right_argument.type == ResultType::INSTANT_VECTOR);
    
    if (left_argument.store_method == StoreMethod::CONST_SCALAR)
        return binaryOperatorWithVectorsByConstScalar(operator_node, std::move(left_argument), std::move(right_argument), true, context);

    if (right_argument.store_method == StoreMethod::CONST_SCALAR)
        return binaryOperatorWithVectorsByConstScalar(operator_node, std::move(right_argument), std::move(left_argument), false, context);

    if (left_argument.store_method == StoreMethod::SCALAR_GRID)
        return binaryOperatorWithVectorsByScalarGrid(operator_node, std::move(left_argument), std::move(right_argument), true, context);

    if (right_argument.store_method == StoreMethod::SCALAR_GRID)
        return binaryOperatorWithVectorsByScalarGrid(operator_node, std::move(right_argument), std::move(left_argument), false, context);

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

    return binaryOperatorWithVectorsByVectorGrids(operator_node, std::move(left_argument), std::move(right_argument), context);
}

}
