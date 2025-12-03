#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/addParametersToAggregateFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    bool isComparisonOperator(std::string_view operator_name)
    {
        static const std::vector<std::string_view> comparison_operators = {
            "==", "!=", ">", "<", ">=", "<="
        };
        return std::find(comparison_operators.begin(), comparison_operators.end(), operator_name) != comparison_operators.end();
    }

    bool isSetOperator(std::string_view operator_name)
    {
        static const std::vector<std::string_view> set_operators = {
            "and", "or", "unless"
        };
        return std::find(set_operators.begin(), set_operators.end(), operator_name) != set_operators.end();
    }


    void checkArgumentTypes(
        const PQT::BinaryOperator * operator_node,
        const SQLQueryPiece & left_argument,
        const SQLQueryPiece & right_argument,
        const ConverterContext & context)
    {
        std::string_view operator_name = operator_node->operator_name;

        bool is_comparison_operator = isComparisonOperator(operator_name);
        if (operator_node->bool_modifier && !is_comparison_operator)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "The 'bool' modifier is not allowed for operator '{}'", operator_name);
        }

        bool is_set_operator = isSetOperator(operator_name);
        if (operator_node->group_left && is_set_operator)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "group_left is not allowed for operator '{}'", operator_name);
        }

        if (operator_node->group_right && is_set_operator)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "group_right is not allowed for operator '{}'", operator_name);
        }

        if (is_set_operator)
        {
            if (left_argument.type != ResultType::INSTANT_VECTOR)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Operator '{}' expects two arguments of type {}, but expression {} has type {}",
                                operator_name, ResultType::INSTANT_VECTOR,
                                getPromQLQuery(left_argument, context), left_argument.type);
            }

            if (right_argument.type != ResultType::INSTANT_VECTOR)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Operator '{}' expects two arguments of type {}, but expression {} has type {}",
                                operator_name, ResultType::INSTANT_VECTOR,
                                getPromQLQuery(right_argument, context), right_argument.type);
            }
        }
        else
        {
            if (!(left_argument.type == ResultType::SCALAR || left_argument.type == ResultType::INSTANT_VECTOR))
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                                operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                                getPromQLQuery(left_argument, context), left_argument.type);
            }

            if (!(right_argument.type == ResultType::SCALAR || right_argument.type == ResultType::INSTANT_VECTOR))
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                                operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                                getPromQLQuery(right_argument, context), right_argument.type);
            }
        }

        if (is_comparison_operator && (left_argument.type == ResultType::SCALAR) && (right_argument.type == ResultType::SCALAR)
            && !operator_node->bool_modifier)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Operator '{}' on scalar requires the 'bool' modifier", operator_name);
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
        return makeASTFunction(it->second, left_argument, right_argument);
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
    bool isComparisonWithoutBool(const PQT::BinaryOperator * operator_node)
    {
        return !operator_node->bool_modifier && isComparisonOperator(operator_node->operator_name);
    }

    /// Returns an AST for evaluating a binary operator on a scalar and each element of an array.
    ASTPtr makeOperatorASTForScalarAndArray(
        const PQT::BinaryOperator * operator_node,
        Float64 scalar_argument,
        ASTPtr array_argument,
        bool left_to_right,
        bool return_scalar_argument_if_match)
    {
        ASTPtr operator_expr = makeOperatorAST(
            operator_node->operator_name,
            std::make_shared<ASTLiteral>(scalar_argument),
            std::make_shared<ASTIdentifier>("x"),
            left_to_right);

        if (isComparisonWithoutBool(operator_node))
        {
            /// E.g. arrayMap(x -> if (scalar_argument >= x, x, NULL), array_argument)
            ASTPtr expr_if_match = return_scalar_argument_if_match ? static_cast<ASTPtr>(std::make_shared<ASTLiteral>(scalar_argument))
                                                                   : std::make_shared<ASTIdentifier>("x");
            operator_expr = makeASTFunction("if", operator_expr, expr_if_match, std::make_shared<ASTLiteral>(Field{} /* NULL */));
        }

        /// E.g. arrayMap(x -> scalar_argument + x, array_argument)
        return makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")), operator_expr),
            array_argument);
    }

    ASTPtr makeOperatorASTForArrayAndScalar(
        const PQT::BinaryOperator * operator_node,
        ASTPtr array_argument,
        Float64 scalar_argument,
        bool left_to_right,
        bool return_scalar_argument_if_match)
    {
        return makeOperatorASTForScalarAndArray(operator_node, scalar_argument, array_argument, !left_to_right, return_scalar_argument_if_match);
    }

    /// Returns an AST for evaluating a binary operator on corresponding elements of two arrays.
    ASTPtr makeOperatorASTForTwoArrays(
        const PQT::BinaryOperator * operator_node,
        ASTPtr array_argument,
        ASTPtr other_array_argument,
        bool left_to_right,
        bool return_array_argument_if_match)
    {
        ASTPtr operator_expr = makeOperatorAST(
            operator_node->operator_name, std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y"), left_to_right);

        if (isComparisonWithoutBool(operator_node))
        {
            /// E.g. arrayMap(x, y -> if (x >= y, y, NULL), array_argument, other_array_argument)
            operator_expr = makeASTFunction(
                "if",
                operator_expr,
                std::make_shared<ASTIdentifier>(return_array_argument_if_match ? "x" : "y"),
                std::make_shared<ASTLiteral>(Field{} /* NULL */));
        }

        /// E.g. arrayMap(x, y -> x + y, array_argument, other_array_argument)
        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                operator_expr,
                array_argument,
                other_array_argument));
    }


    /// Applies a binary operator if one of the arguments is a SCALAR represented by StoreMethod::CONST_SCALAR,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can be either SCALAR or INSTANT_VECTOR with any store method of
    /// {StoreMethod::CONST_SCALAR, StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithScalarByConstScalar(
        const PQT::BinaryOperator * operator_node,
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
                if (is_comparison_without_bool)
                {
                    chassert(res.type == ResultType::INSTANT_VECTOR);
                    if (float_result != 0)
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
                SelectQueryParams params;

                if (other_store_method == StoreMethod::VECTOR_GRID)
                {
                    params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));
                }
                else if (is_comparison_without_bool)
                {
                    chassert(res.type == ResultType::INSTANT_VECTOR);
                    params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
                    params.select_list.back()->setAlias(ColumnNames::Group);
                    res.store_method = StoreMethod::VECTOR_GRID;
                    res.metric_name_dropped = true;
                }

                params.select_list.push_back(makeOperatorASTForScalarAndArray(
                    operator_node, scalar_value, std::make_shared<ASTIdentifier>(ColumnNames::Values), left_to_right,
                    /* return_scalar_argument_if_match = */ false));

                params.select_list.back()->setAlias(ColumnNames::Values);

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(other_argument.select_query), SQLSubqueryType::TABLE});
                params.from_table = context.subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));

                if (!is_comparison_without_bool)
                    res = dropMetricName(std::move(res), context);

                return res;
            }

            case StoreMethod::EMPTY:
            {
                UNREACHABLE(); /// Can't get in here, case StoreMethod::EMPTY has been already handled.
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
        const PQT::BinaryOperator * operator_node,
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
                /// Otherwise case must be already handled - see the code of applyBinaryOperator().
                chassert(other_type == ResultType::INSTANT_VECTOR);

                auto other_scalar_value = other_argument.scalar_value;
                res.store_method = StoreMethod::SCALAR_GRID;

                SelectQueryParams params;
                params.select_list.push_back(makeOperatorASTForArrayAndScalar(
                    operator_node, std::make_shared<ASTIdentifier>(ColumnNames::Values), other_scalar_value, left_to_right,
                    /* return_scalar_argument_if_match = */ true));

                params.select_list.back()->setAlias(ColumnNames::Values);

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
                params.from_table = context.subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));

                if (!is_comparison_without_bool)
                    res = dropMetricName(std::move(res), context);

                return res;
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::SCALAR});
                String scalar_grid = context.subqueries.back().name;

                SelectQueryParams params;

                if (other_store_method == StoreMethod::VECTOR_GRID)
                {
                    params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));
                }
                else if (is_comparison_without_bool)
                {
                    chassert(res.type == ResultType::INSTANT_VECTOR);
                    params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
                    params.select_list.back()->setAlias(ColumnNames::Group);
                    res.store_method = StoreMethod::VECTOR_GRID;
                    res.metric_name_dropped = true;
                }

                params.select_list.push_back(makeOperatorASTForTwoArrays(
                    operator_node,
                    std::make_shared<ASTIdentifier>(scalar_grid),
                    std::make_shared<ASTIdentifier>(ColumnNames::Values),
                    left_to_right,
                    /* return_array_argument_if_match = */ false));

                params.select_list.back()->setAlias(ColumnNames::Values);

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(other_argument.select_query), SQLSubqueryType::TABLE});
                params.from_table = context.subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));

                if (!is_comparison_without_bool)
                    res = dropMetricName(std::move(res), context);

                return res;
            }

            case StoreMethod::EMPTY:
            {
                UNREACHABLE(); /// Can't get in here, case StoreMethod::EMPTY has been already handled.
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


    struct GroupAST
    {
        ASTPtr ast;
        bool metric_name_dropped = false;
    };


    /// Returns an AST to evaluate the join group which is a parameter we join both sides of a binary operator on instant vectors on.
    GroupAST makeJoinGroupAST(const PQT::BinaryOperator * operator_node, GroupAST && group)
    {
        if (const auto * literal = group.ast->as<const ASTLiteral>(); literal && literal->value == Field{0u})
            return {.ast = std::move(group.ast), .metric_name_dropped = true};

        if (operator_node->on)
        {
            if (operator_node->labels.empty())
            {
                /// ON() means we ignore all tags
                return {.ast = std::make_shared<ASTLiteral>(0u), .metric_name_dropped = true};
            }
            else
            {
                /// ON(tags) means we ignore all tags except the specified `tags`.
                Strings tags = operator_node->labels;
                std::sort(tags.begin(), tags.end());
                tags.erase(std::unique(tags.begin(), tags.end()), tags.end());
                bool metric_name_dropped = (std::find(tags.begin(), tags.end(), "__name__") == tags.end());
                return {
                    .ast = makeASTFunction(
                        "timeSeriesRemoveAllTagsExcept", std::move(group.ast), std::make_shared<ASTLiteral>(Array{tags.begin(), tags.end()})),
                    .metric_name_dropped = metric_name_dropped};
            }
        }
        else if (operator_node->ignoring && !operator_node->labels.empty())
        {
            /// IGNORE(tags) means we ignore the specified `tags`.
            /// We ignore the metric name "__name__" even if it isn't specified in `tags`.
            Strings tags = operator_node->labels;
            if (std::find(tags.begin(), tags.end(), "__name__") == tags.end())
                tags.push_back("__name__");
            std::sort(tags.begin(), tags.end());
            tags.erase(std::unique(tags.begin(), tags.end()), tags.end());
            return {
                .ast = makeASTFunction("timeSeriesRemoveTags", std::move(group.ast), std::make_shared<ASTLiteral>(Array{tags.begin(), tags.end()})),
                .metric_name_dropped = true};
        }
        else
        {
            return std::move(group);
        }
    }

    ASTPtr makeJoinGroupAST(const PQT::BinaryOperator * operator_node, ASTPtr && group)
    {
        return makeJoinGroupAST(operator_node, GroupAST{.ast = std::move(group), .metric_name_dropped = false}).ast;
    }

    GroupAST makeResultGroupASTImpl(
        const PQT::BinaryOperator * operator_node, GroupAST && join_group, bool group_keyword_presents, GroupAST && group, GroupAST && other_group)
    {
        if (!group_keyword_presents)
        {
            if (join_group.metric_name_dropped || isComparisonWithoutBool(operator_node))
                return std::move(join_group);
            else
                return {
                    .ast = makeASTFunction("timeSeriesRemoveTag", std::move(join_group.ast), std::make_shared<ASTLiteral>("__name__")),
                    .metric_name_dropped = true};
        }

        if (operator_node->extra_labels.empty())
        {
            if (group.metric_name_dropped || isComparisonWithoutBool(operator_node))
                return std::move(group);
            else
                return {
                    .ast = makeASTFunction("timeSeriesRemoveTag", std::move(group.ast), std::make_shared<ASTLiteral>("__name__")),
                    .metric_name_dropped = true};
        }

        Strings tags_to_copy = operator_node->extra_labels;
        std::sort(tags_to_copy.begin(), tags_to_copy.end());
        tags_to_copy.erase(std::unique(tags_to_copy.begin(), tags_to_copy.end()), tags_to_copy.end());
        bool copy_metric_name = (std::find(tags_to_copy.begin(), tags_to_copy.end(), "__name__") != tags_to_copy.end());
        auto dest_group = std::move(join_group);
        if (!dest_group.metric_name_dropped && !copy_metric_name && !isComparisonWithoutBool(operator_node))
        {
            dest_group = {
                .ast = makeASTFunction("timeSeriesRemoveTag", std::move(dest_group.ast), std::make_shared<ASTLiteral>("__name__")),
                .metric_name_dropped = true};
        }

        return {
            .ast = makeASTFunction(
                "timeSeriesCopyTags",
                std::move(dest_group.ast),
                std::move(other_group.ast),
                std::make_shared<ASTLiteral>(Array{tags_to_copy.begin(), tags_to_copy.end()})),
            .metric_name_dropped = dest_group.metric_name_dropped && (!copy_metric_name || other_group.metric_name_dropped)};
    }

    /// Returns an AST to evaluate the group which will be set for the result of a binary operator on instant vectors.
    GroupAST makeResultGroupAST(
        const PQT::BinaryOperator * operator_node, GroupAST && join_group, GroupAST && left_group, GroupAST && right_group)
    {
        if (operator_node->group_left)
            return makeResultGroupASTImpl(operator_node, std::move(join_group), /* group_keyword_presents = */ true, std::move(left_group), std::move(right_group));
        else if (operator_node->group_right)
            return makeResultGroupASTImpl(operator_node, std::move(join_group), /* group_keyword_presents = */ true, std::move(right_group), std::move(left_group));
        else
            return makeResultGroupASTImpl(operator_node, std::move(join_group), /* group_keyword_presents = */ false, std::move(left_group), std::move(right_group));
    }


    /// Converts an argument of type INSTANT_VECTOR to StoreMethod::VECTOR_GRID.
    /// We use that for binary operators taking two instant vectors.
    SQLQueryPiece toVectorGrid(const PQT::BinaryOperator * operator_node, SQLQueryPiece && argument, ConverterContext & context)
    {
        chassert(argument.type == ResultType::INSTANT_VECTOR);
        switch (argument.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                SelectQueryParams params;
                params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
                params.select_list.back()->setAlias(ColumnNames::Group);

                params.select_list.push_back(makeASTFunction(
                    "arrayResize",
                    std::make_shared<ASTLiteral>(Array{}),
                    std::make_shared<ASTLiteral>(countTimeseriesSteps(argument.start_time, argument.end_time, argument.step)),
                    std::make_shared<ASTLiteral>(argument.scalar_value)));

                params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Values));

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
                params.from_table = context.subqueries.back().name;

                argument.select_query = buildSelectQuery(std::move(params));
                argument.store_method = StoreMethod::VECTOR_GRID;
                argument.metric_name_dropped = true;

                return std::move(argument);
            }

            case StoreMethod::SCALAR_GRID:
            {
                SelectQueryParams params;
                params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
                params.select_list.back()->setAlias(ColumnNames::Group);

                params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Values));

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
                params.from_table = context.subqueries.back().name;

                argument.select_query = buildSelectQuery(std::move(params));
                argument.store_method = StoreMethod::VECTOR_GRID;
                argument.metric_name_dropped = true;

                return std::move(argument);
            }

            case StoreMethod::VECTOR_GRID:
            {
                return std::move(argument);
            }

            case StoreMethod::EMPTY:
            {
                UNREACHABLE(); /// Can't get in here, case StoreMethod::EMPTY has been already handled.
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Argument of the operator '{}' in expression {} has unexpected type {} (store_method: {})",
                                operator_node->operator_name, getPromQLQuery(argument, context), argument.type, argument.store_method);
            }
        }

        UNREACHABLE();
    }


    /// Applies a binary operator if both of the arguments are instant vectors.
    SQLQueryPiece binaryOperatorWithVectors(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context)
    {
        left_argument = toVectorGrid(operator_node, std::move(left_argument), context);
        right_argument = toVectorGrid(operator_node, std::move(right_argument), context);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        String left = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        String right = context.subqueries.back().name;

        auto join_group = makeJoinGroupAST(
            operator_node,
            GroupAST{.ast = std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}), .metric_name_dropped = left_argument.metric_name_dropped});

        auto result_group = makeResultGroupAST(
            operator_node,
            GroupAST{.ast = std::make_shared<ASTIdentifier>(ColumnNames::JoinGroup), .metric_name_dropped = join_group.metric_name_dropped},
            GroupAST{.ast = std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}), .metric_name_dropped = left_argument.metric_name_dropped},
            GroupAST{.ast = std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Group}), .metric_name_dropped = right_argument.metric_name_dropped});

        /// SELECT timeSeriesCopyTags(join_group, left.group, tags_to_copy) AS group,
        ///        timeSeriesCoalesceGridValues('throw')(arrayMap(x, y -> x + y, left.values, right.values), group) AS values
        /// FROM left INNER ALL JOIN right
        /// ON (timeSeriesRemoveTags(left.group, on_tags) AS join_group) == timeSeriesRemoveTags(right.group, on_tags)
        /// GROUP BY group

        SelectQueryParams params;

        params.select_list.push_back(result_group.ast);
        params.select_list.back()->setAlias(ColumnNames::Group);

        params.select_list.push_back(addParameterToAggregateFunction(
            makeASTFunction(
                "timeSeriesCoalesceGridValues",
                makeOperatorASTForTwoArrays(
                    operator_node,
                    std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Values}),
                    /* left_to_right = */ true,
                    /* return_array_argument_if_match = */ true),
                std::make_shared<ASTIdentifier>(ColumnNames::Group)),
            "throw"));

        params.select_list.back()->setAlias(ColumnNames::Values);

        params.from_table = left;
        params.join_kind = JoinKind::Inner;
        params.join_strictness = JoinStrictness::All;
        params.join_table = right;

        ASTPtr join_group_with_alias = join_group.ast;
        join_group_with_alias->setAlias(ColumnNames::JoinGroup);
        params.join_on = makeASTFunction(
            "equals",
            join_group_with_alias,
            makeJoinGroupAST(operator_node, std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Group})));

        params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

        SQLQueryPiece res = left_argument;
        res.node = operator_node;
        res.select_query = buildSelectQuery(std::move(params));
        res.store_method = StoreMethod::VECTOR_GRID;
        res.metric_name_dropped = result_group.metric_name_dropped;

        return res;
    }


    /// Applies the binary operator "and".
    SQLQueryPiece binaryOperatorAnd(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context)
    {
        if ((left_argument.store_method == StoreMethod::EMPTY) || (right_argument.store_method == StoreMethod::EMPTY))
        {
            /// If one of the arguments has no data, the result also has no data.
            return SQLQueryPiece(operator_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY);
        }

        left_argument = toVectorGrid(operator_node, std::move(left_argument), context);
        right_argument = toVectorGrid(operator_node, std::move(right_argument), context);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        String left = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        String right = context.subqueries.back().name;

        /// Step 1:
        /// SELECT timeSeriesRemoveAllTagsExcept(right.group, on_tags) AS join_group,
        ///        timeSeriesCoalesce('any')(right.values) AS values
        /// GROUP BY join_group
        /// FROM right
        String step1;
        {
            SelectQueryParams params;

            params.select_list.push_back(makeJoinGroupAST(operator_node, std::make_shared<ASTIdentifier>(ColumnNames::Group)));
            params.select_list.back()->setAlias(ColumnNames::JoinGroup);

            params.select_list.push_back(addParameterToAggregateFunction(
                makeASTFunction("timeSeriesCoalesceGridValues", std::make_shared<ASTIdentifier>(ColumnNames::Values)), "any"));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::JoinGroup));

            params.from_table = right;

            ASTPtr step1_ast = buildSelectQuery(std::move(params));
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_ast), SQLSubqueryType::TABLE});
            step1 = context.subqueries.back().name;
        }

        /// Step 2:
        /// SELECT left.group AS group,
        ///        arrayMap(x, y -> if(isNull(y), NULL, x), left.values, step1.values) AS values
        /// FROM left LEFT SEMI JOIN step1
        /// ON timeSeriesRemoveAllTagsExcept(left.group, on_tags) == step1.join_group
        ASTPtr step2;
        {
            SelectQueryParams params;

            params.select_list.push_back(std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}));
            params.select_list.back()->setAlias(ColumnNames::Group);

            params.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("isNull", std::make_shared<ASTIdentifier>("y")),
                        std::make_shared<ASTLiteral>(Field{} /* NULL */),
                        std::make_shared<ASTIdentifier>("x")),
                    std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    std::make_shared<ASTIdentifier>(Strings{step1, ColumnNames::Values}))));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.from_table = left;
            params.join_kind = JoinKind::Left;
            params.join_strictness = JoinStrictness::Semi;
            params.join_table = step1;

            params.join_on = makeASTFunction(
                "equals",
                makeJoinGroupAST(operator_node, std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group})),
                std::make_shared<ASTIdentifier>(Strings{step1, ColumnNames::JoinGroup}));

            step2 = buildSelectQuery(std::move(params));
        }

        SQLQueryPiece res = left_argument;
        res.node = operator_node;
        res.select_query = std::move(step2);
        return res;
    }


    /// Applies the binary operator "unless".
    SQLQueryPiece binaryOperatorUnless(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context)
    {
        if (left_argument.store_method == StoreMethod::EMPTY)
            return SQLQueryPiece(operator_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY);

        if (right_argument.store_method == StoreMethod::EMPTY)
        {
            left_argument.node = operator_node;
            return std::move(left_argument);
        }

        left_argument = toVectorGrid(operator_node, std::move(left_argument), context);
        right_argument = toVectorGrid(operator_node, std::move(right_argument), context);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        String left = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        String right = context.subqueries.back().name;

        /// Step 1:
        /// SELECT timeSeriesRemoveAllTagsExcept(right.group, on_tags) AS join_group,
        ///        timeSeriesCoalesce('any')(right.values) AS values
        /// GROUP BY join_group
        /// FROM right
        String step1;
        {
            SelectQueryParams params;

            params.select_list.push_back(makeJoinGroupAST(operator_node, std::make_shared<ASTIdentifier>(ColumnNames::Group)));
            params.select_list.back()->setAlias(ColumnNames::JoinGroup);

            params.select_list.push_back(addParameterToAggregateFunction(
                makeASTFunction("timeSeriesCoalesceGridValues", std::make_shared<ASTIdentifier>(ColumnNames::Values)), "any"));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::JoinGroup));

            params.from_table = right;

            ASTPtr step1_ast = buildSelectQuery(std::move(params));
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_ast), SQLSubqueryType::TABLE});
            step1 = context.subqueries.back().name;
        }

        /// Step 2:
        /// SELECT left.group AS group,
        ///        if(empty(step1.values), left.values, arrayMap(x, y -> if(isNull(y), x, NULL), left.values, step1.values)) AS values
        /// FROM left LEFT ANY JOIN step1
        /// ON timeSeriesRemoveAllTagsExcept(left.group, on_tags) == step1.join_group
        ASTPtr step2;
        {
            SelectQueryParams params;

            params.select_list.push_back(std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}));
            params.select_list.back()->setAlias(ColumnNames::Group);

            params.select_list.push_back(makeASTFunction(
                "if",
                makeASTFunction("empty", std::make_shared<ASTIdentifier>(Strings{step1, ColumnNames::Values})),
                std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                makeASTFunction(
                    "arrayMap",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                        makeASTFunction(
                            "if",
                            makeASTFunction("isNull", std::make_shared<ASTIdentifier>("y")),
                            std::make_shared<ASTIdentifier>("x"),
                            std::make_shared<ASTLiteral>(Field{} /* NULL */)),
                        std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                        std::make_shared<ASTIdentifier>(Strings{step1, ColumnNames::Values})))));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.from_table = left;
            params.join_kind = JoinKind::Left;
            params.join_strictness = JoinStrictness::Any;
            params.join_table = step1;

            params.join_on = makeASTFunction(
                "equals",
                makeJoinGroupAST(operator_node, std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group})),
                std::make_shared<ASTIdentifier>(Strings{step1, ColumnNames::JoinGroup}));

            step2 = buildSelectQuery(std::move(params));
        }

        SQLQueryPiece res = left_argument;
        res.node = operator_node;
        res.select_query = std::move(step2);
        return res;
    }


    /// Applies the binary operator "or".
    SQLQueryPiece binaryOperatorOr(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context)
    {
        if (left_argument.store_method == StoreMethod::EMPTY)
        {
            right_argument.node = operator_node;
            return std::move(right_argument);
        }

        if (right_argument.store_method == StoreMethod::EMPTY)
        {
            left_argument.node = operator_node;
            return std::move(left_argument);
        }

        left_argument = toVectorGrid(operator_node, std::move(left_argument), context);
        right_argument = toVectorGrid(operator_node, std::move(right_argument), context);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        String left = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        String right = context.subqueries.back().name;

        /// Step 1:
        /// SELECT group,
        ///        arrayMap(x, y -> if(isNull(x), y, x), left.values, right.values) AS values
        /// FROM left LEFT SEMI JOIN right
        /// ON left.group == right.group
        String step1;
        {
            SelectQueryParams params;

            params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

            params.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("isNull", std::make_shared<ASTIdentifier>("x")),
                        std::make_shared<ASTIdentifier>("y"),
                        std::make_shared<ASTIdentifier>("x")),
                    std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Values}))));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.from_table = left;
            params.join_kind = JoinKind::Left;
            params.join_strictness = JoinStrictness::Semi;
            params.join_table = right;

            params.join_on = makeASTFunction(
                "equals",
                std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}),
                std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Group}));

            ASTPtr step1_ast = buildSelectQuery(std::move(params));
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_ast), SQLSubqueryType::TABLE});
            step1 = context.subqueries.back().name;
        }

        /// Step 2:
        /// SELECT timeSeriesRemoveAllTagsExcept(step1.group, on_tags) AS join_group,
        ///        timeSeriesCoalesce('any')(step1.values) AS values
        /// GROUP BY join_group
        /// FROM step1
        String step2;
        {
            SelectQueryParams params;

            params.select_list.push_back(makeJoinGroupAST(operator_node, std::make_shared<ASTIdentifier>(ColumnNames::Group)));
            params.select_list.back()->setAlias(ColumnNames::JoinGroup);

            params.select_list.push_back(addParameterToAggregateFunction(
                makeASTFunction("timeSeriesCoalesceGridValues", std::make_shared<ASTIdentifier>(ColumnNames::Values)), "any"));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::JoinGroup));

            params.from_table = step1;

            ASTPtr step2_ast = buildSelectQuery(std::move(params));
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step2_ast), SQLSubqueryType::TABLE});
            step2 = context.subqueries.back().name;
        }

        /// Step 3:
        /// SELECT right.group AS group,
        ///        if(empty(step2.values), right.values, arrayMap(x, y -> if(isNull(x), y, NULL), step2.values, right.values)) AS values
        /// FROM step2 RIGHT ANY JOIN right
        /// ON step2.join_group == timeSeriesRemoveAllTagsExcept(right.group, on_tags)
        String step3;
        {
            SelectQueryParams params;

            params.select_list.push_back(std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Group}));
            params.select_list.back()->setAlias(ColumnNames::Group);

            params.select_list.push_back(makeASTFunction(
                "if",
                makeASTFunction("empty", std::make_shared<ASTIdentifier>(Strings{step2, ColumnNames::Values})),
                std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Values}),
                makeASTFunction(
                    "arrayMap",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                        makeASTFunction(
                            "if",
                            makeASTFunction("isNull", std::make_shared<ASTIdentifier>("x")),
                            std::make_shared<ASTIdentifier>("y"),
                            std::make_shared<ASTLiteral>(Field{} /* NULL */)),
                        std::make_shared<ASTIdentifier>(Strings{step2, ColumnNames::Values}),
                        std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Values})))));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.from_table = step2;
            params.join_kind = JoinKind::Right;
            params.join_strictness = JoinStrictness::Any;
            params.join_table = right;

            params.join_on = makeASTFunction(
                "equals",
                std::make_shared<ASTIdentifier>(Strings{step2, ColumnNames::JoinGroup}),
                makeJoinGroupAST(operator_node, std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Group})));

            ASTPtr step3_ast = buildSelectQuery(std::move(params));
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step3_ast), SQLSubqueryType::TABLE});
            step3 = context.subqueries.back().name;
        }

        /// Step4:
        /// SELECT group, values FROM step1 UNION ALL SELECT group, values FROM step3
        ASTPtr step4;
        {
            SelectQueryParams params;
            params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));
            params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Values));
            params.from_table = step1;
            params.union_table = step3;
            step4 = buildSelectQuery(std::move(params));
        }

        SQLQueryPiece res = left_argument;
        res.node = operator_node;
        res.select_query = step4;
        res.metric_name_dropped &= right_argument.metric_name_dropped;
        return res;
    }
}


SQLQueryPiece applyBinaryOperator(
    const PQT::BinaryOperator * operator_node, SQLQueryPiece && left_argument, SQLQueryPiece && right_argument, ConverterContext & context)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    /// The binary operators for working on sets.
    std::string_view operator_name = operator_node->operator_name;
    if (operator_name == "and")
        return binaryOperatorAnd(operator_node, std::move(left_argument), std::move(right_argument), context);
    else if (operator_name == "or")
        return binaryOperatorOr(operator_node, std::move(left_argument), std::move(right_argument), context);
    else if (operator_name == "unless")
        return binaryOperatorUnless(operator_node, std::move(left_argument), std::move(right_argument), context);

    /// Other binary operators.

    if ((left_argument.store_method == StoreMethod::EMPTY) || (right_argument.store_method == StoreMethod::EMPTY))
    {
        /// If one of the arguments has no data, the result also has no data.
        return SQLQueryPiece(operator_node, ResultType::INSTANT_VECTOR, StoreMethod::EMPTY);
    }

    /// Check if one of the arguments is scalar.
    if ((left_argument.type == ResultType::SCALAR) && (left_argument.store_method == StoreMethod::CONST_SCALAR))
    {
        return binaryOperatorWithScalarByConstScalar(
            operator_node, std::move(left_argument), std::move(right_argument), /* left_to_right = */ true, context);
    }

    if ((right_argument.type == ResultType::SCALAR) && (right_argument.store_method == StoreMethod::CONST_SCALAR))
    {
        return binaryOperatorWithScalarByConstScalar(
            operator_node, std::move(right_argument), std::move(left_argument), /* left_to_right = */ false, context);
    }

    if ((left_argument.type == ResultType::SCALAR) && (left_argument.store_method == StoreMethod::SCALAR_GRID))
    {
        return binaryOperatorWithScalarByScalarGrid(
            operator_node, std::move(left_argument), std::move(right_argument), /* left_to_right = */ true, context);
    }

    if ((right_argument.type == ResultType::SCALAR) && (right_argument.store_method == StoreMethod::SCALAR_GRID))
    {
        return binaryOperatorWithScalarByScalarGrid(
            operator_node, std::move(right_argument), std::move(left_argument), /* left_to_right = */ false, context);
    }

    /// Both of the arguments are instant vectors.
    chassert(left_argument.type == ResultType::INSTANT_VECTOR);
    chassert(right_argument.type == ResultType::INSTANT_VECTOR);

    return binaryOperatorWithVectors(operator_node,
                                     toVectorGrid(operator_node, std::move(left_argument), context),
                                     toVectorGrid(operator_node, std::move(right_argument), context),
                                     context);
}

}
