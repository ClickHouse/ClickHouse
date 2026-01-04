#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
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

        if (isComparisonOperator(operator_node->operator_name))
        {
            if (operator_node->bool_modifier)
            {
                /// A comparison operator in ClickHouse returns UInt8, we need to convert it to Float64 for consistency with prometheus values.
                operator_expr = makeASTFunction("toFloat64", std::move(operator_expr));
            }
            else
            {
                /// E.g. arrayMap(x -> if (scalar_argument >= x, x, NULL), array_argument)
                ASTPtr expr_if_match = return_scalar_argument_if_match ? static_cast<ASTPtr>(std::make_shared<ASTLiteral>(scalar_argument))
                                                                       : std::make_shared<ASTIdentifier>("x");
                operator_expr = makeASTFunction("if", std::move(operator_expr), std::move(expr_if_match), std::make_shared<ASTLiteral>(Field{} /* NULL */));
            }
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

        if (isComparisonOperator(operator_node->operator_name))
        {
            if (operator_node->bool_modifier)
            {
                /// A comparison operator in ClickHouse returns UInt8, we need to convert it to Float64 for consistency with prometheus values.
                operator_expr = makeASTFunction("toFloat64", std::move(operator_expr));
            }
            else
            {
                /// E.g. arrayMap(x, y -> if (x >= y, y, NULL), array_argument, other_array_argument)
                operator_expr = makeASTFunction(
                    "if",
                    operator_expr,
                    std::make_shared<ASTIdentifier>(return_array_argument_if_match ? "x" : "y"),
                    std::make_shared<ASTLiteral>(Field{} /* NULL */));
            }
        }

        /// E.g. arrayMap(x, y -> x + y, array_argument, other_array_argument)
        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                operator_expr),
            array_argument,
            other_array_argument);
    }


    /// Applies a binary operator if one of the arguments is a SCALAR represented by StoreMethod::CONST_SCALAR,
    /// `argument_index` specifies which of the two arguments is such.
    /// Other argument can be either SCALAR or INSTANT_VECTOR with any store method of
    /// {StoreMethod::CONST_SCALAR, StoreMethod::SCALAR_GRID, StoreMethod::VECTOR_GRID}.
    SQLQueryPiece binaryOperatorWithScalarByConstScalar(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && scalar_argument,
        SQLQueryPiece && other_argument,
        bool left_to_right,
        ConverterContext & context)
    {
        chassert((scalar_argument.type == ResultType::SCALAR) && (scalar_argument.store_method == StoreMethod::CONST_SCALAR));

        const auto & operator_name = operator_node->operator_name;
        bool is_comparison_without_bool = isComparisonWithoutBool(operator_node);
        auto scalar_value = scalar_argument.scalar_value;
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
            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here, the store methods CONST_STRING & RAW_DATA are incompatible
                /// with the allowed argument types (see checkArgumentTypes()),
                /// and the store method EMPTY should be handled already.
                throw Exception(ErrorCodes::LOGICAL_ERROR,
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
        SQLQueryPiece && scalar_argument,
        SQLQueryPiece && other_argument,
        bool left_to_right,
        ConverterContext & context)
    {
        chassert((scalar_argument.type == ResultType::SCALAR) && (scalar_argument.store_method == StoreMethod::SCALAR_GRID));
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

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::TABLE});
                params.from_table = context.subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));

                if (!is_comparison_without_bool)
                    res = dropMetricName(std::move(res), context);

                return res;
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR});
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
            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here, the store methods CONST_STRING & RAW_DATA are incompatible
                /// with the allowed argument types (see checkArgumentTypes()),
                /// and the store method EMPTY should be handled already.
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Other argument of the operator '{}' in expression {} has unexpected type {} (store_method: {})",
                                operator_name, getPromQLQuery(other_argument, context), other_type, other_store_method);
            }
        }

        UNREACHABLE();
    }


    /// Returns an AST to evaluate the `join_group` column to join the sides of a binary operator on instant vectors.
    ASTPtr makeExpressionForJoinGroup(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && argument_group,
        bool metric_name_dropped_from_argument_group,
        bool & metric_name_dropped_from_join_group)
    {
        /// Group #0 always means a group with no tags.
        if (const auto * literal = argument_group->as<const ASTLiteral>(); literal && literal->value == Field{0u})
        {
            metric_name_dropped_from_join_group = true;
            return std::move(argument_group);
        }

        if (operator_node->on)
        {
            if (operator_node->labels.empty())
            {
                /// ON() means we ignore all tags.
                metric_name_dropped_from_join_group = true;
                return std::make_shared<ASTLiteral>(0u);
            }
            else
            {
                /// ON(tags) means we ignore all tags except the specified ones.
                /// If the metric name "__name__" is among the tags in ON(tags) we don't remove it from the join group.

                /// timeSeriesRemoveAllTagsExcept(argument_group, on_tags)
                Strings tags_to_keep = operator_node->labels;
                std::sort(tags_to_keep.begin(), tags_to_keep.end());
                tags_to_keep.erase(std::unique(tags_to_keep.begin(), tags_to_keep.end()), tags_to_keep.end());

                metric_name_dropped_from_join_group = !std::binary_search(tags_to_keep.begin(), tags_to_keep.end(), kMetricName);

                return makeASTFunction(
                           "timeSeriesRemoveAllTagsExcept",
                           std::move(argument_group),
                           std::make_shared<ASTLiteral>(Array{tags_to_keep.begin(), tags_to_keep.end()}));
            }
        }
        else if (operator_node->ignoring && !operator_node->labels.empty())
        {
            /// IGNORE(tags) means we ignore the specified tags, and also the metric name "__name__".

            /// timeSeriesRemoveTags(argument_group, ignoring_tags + ['__name__'])
            Strings tags_to_remove = operator_node->labels;
            if (!metric_name_dropped_from_argument_group && (std::find(tags_to_remove.begin(), tags_to_remove.end(), kMetricName) == tags_to_remove.end()))
                tags_to_remove.push_back(kMetricName);
            std::sort(tags_to_remove.begin(), tags_to_remove.end());
            tags_to_remove.erase(std::unique(tags_to_remove.begin(), tags_to_remove.end()), tags_to_remove.end());

            metric_name_dropped_from_join_group = true;

            return makeASTFunction(
                       "timeSeriesRemoveTags",
                       std::move(argument_group),
                       std::make_shared<ASTLiteral>(Array{tags_to_remove.begin(), tags_to_remove.end()}));
        }
        else
        {
            /// Neither ON() nor IGNORE() keywords are specified, we use all the tags except the metric name "__name__".
            metric_name_dropped_from_join_group = true;
            if (metric_name_dropped_from_argument_group)
                return std::move(argument_group);
            else
                return makeASTFunction("timeSeriesRemoveTag", std::move(argument_group), std::make_shared<ASTLiteral>(kMetricName));
        }
    }

    ASTPtr makeExpressionForJoinGroup(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && argument_group,
        bool metric_name_dropped_from_argument_group)
    {
        bool dummy;
        return makeExpressionForJoinGroup(operator_node, std::move(argument_group), metric_name_dropped_from_argument_group, dummy);
    }


    /// Returns an AST to evaluate the result group of a binary operator on instant vectors if neither "group_left" nor "group_right" is used.
    /// The function usually just returns `join_group`.
    /// There are two special cases:
    /// 1. If `join_group` contains the metric name "__name__", this function removes it
    ///    because the result of a binary operator shouldn't contain the metric name.
    ///    (`join_group` can contain the metric name if it's specified explicitly in ON(),
    ///    for example "http_errors + on (__name__) http_failures")
    /// 2. If it's a comparisons without the bool modifier the function doesn't remove the metric name, instead it copies it from the left side
    ///    in case the ignoring list doesn't contain the metric name; or neither on() nor ignore() is specified.
    ASTPtr makeExpressionForResultGroup_Default(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && left_argument_group,
        ASTPtr && /* right_argument_group */,
        ASTPtr && join_group,
        bool metric_name_dropped_from_left,
        bool /* metric_name_dropped_from_right */,
        bool metric_name_dropped_from_join_group,
        bool & metric_name_dropped_from_result)
    {
        chassert(!operator_node->group_left && !operator_node->group_right);

        if (isComparisonWithoutBool(operator_node))
        {
            /// For comparison operators without the bool modifier we add the metric name "__name__" to the result group from the left side by default
            /// unless it's explicitly said that it should be ignored.
            bool copy_metric_name;
            if (operator_node->ignoring)
                copy_metric_name = (std::find(operator_node->labels.begin(), operator_node->labels.end(), kMetricName) == operator_node->labels.end());
            else
                copy_metric_name = !operator_node->on;

            copy_metric_name &= !metric_name_dropped_from_left;
            
            if (copy_metric_name)
            {
                /// timeSeriesCopyTag(join_group, left_argument_group, "__name__")
                metric_name_dropped_from_result = false;
                return makeASTFunction(
                    "timeSeriesCopyTag", std::move(join_group), std::move(left_argument_group), std::make_shared<ASTLiteral>(kMetricName));
            }
            else
            {
                metric_name_dropped_from_result = metric_name_dropped_from_join_group;
                return std::move(join_group);
            }
        }
       
        /// If it's not a comparison operator or the bool modifier is specified,
        /// then we always remove the metric name "__name__" from the result group.
        metric_name_dropped_from_result = true;
        if (metric_name_dropped_from_join_group)
            return std::move(join_group);
        else
            return makeASTFunction("timeSeriesRemoveTag", std::move(join_group), std::make_shared<ASTLiteral>(kMetricName));
    }


    /// Returns an AST to evaluate the result group of a binary operator on instant vectors if "group_left" is specified.
    /// The function usually returns
    /// timeSeriesCopyTags(join_group, right_argument_group, extra_tags)
    /// where `extra_tags` are the tags copied from the right side and specified in expression "group_left(extra_tags)".
    /// Notes:
    /// 1. If `join_group` contains the metric name "__name__" then this function removes it
    ///    because the result of a binary operator shouldn't contain the metric name unless it's copied with `extra_tags`.
    ///    (`join_group` can contain the metric name if it's specified explicitly in ON(),
    ///    for example "http_errors + on (__name__) group_left(code) http_failures")
    /// 2. If "group_left" is specified without `extra_tags` then the function just takes the left argument (and removes the metric name from it).
    /// 3. If it's a comparison operator without bool modifier then the function doesn't remove the metric name from the result group.
    ASTPtr makeExpressionForResultGroup_GroupLeft(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && left_argument_group,
        ASTPtr && right_argument_group,
        ASTPtr && join_group,
        bool metric_name_dropped_from_left,
        bool metric_name_dropped_from_right,
        bool metric_name_dropped_from_join_group,
        bool & metric_name_dropped_from_result)
    {
        /// We use this function to implement both group_left() and group_right().
        chassert(operator_node->group_left || operator_node->group_right);

        Strings tags_to_copy = operator_node->extra_labels;

        if (tags_to_copy.empty())
        {
            /// group_left is used with an empty list of tags to copy.
            if (isComparisonWithoutBool(operator_node) || metric_name_dropped_from_left)
            {
                metric_name_dropped_from_result = metric_name_dropped_from_left;
                return std::move(left_argument_group);
            }
            else
            {
                /// If it's not a comparison operator or the bool modifier is specified,
                /// then we always remove the metric name "__name__" from the result group.
                metric_name_dropped_from_result = true;
                return makeASTFunction("timeSeriesRemoveTag", std::move(left_argument_group), std::make_shared<ASTLiteral>(kMetricName));
            }
        }

        std::sort(tags_to_copy.begin(), tags_to_copy.end());
        tags_to_copy.erase(std::unique(tags_to_copy.begin(), tags_to_copy.end()), tags_to_copy.end());
        bool copy_metric_name = std::binary_search(tags_to_copy.begin(), tags_to_copy.end(), kMetricName) && !metric_name_dropped_from_right;

        ASTPtr dest_group = join_group;
        bool metric_name_dropped_from_dest_group = metric_name_dropped_from_join_group;

        if (!metric_name_dropped_from_dest_group && !copy_metric_name && !isComparisonWithoutBool(operator_node))
        {
            /// If it's not a comparison operator or the bool modifier is specified,
            /// then we always remove the metric name "__name__" from the result group.
            dest_group = makeASTFunction("timeSeriesRemoveTag", std::move(dest_group), std::make_shared<ASTLiteral>(kMetricName));
            metric_name_dropped_from_dest_group = true;
        }

        metric_name_dropped_from_result = metric_name_dropped_from_dest_group && !copy_metric_name;

        return makeASTFunction(
            "timeSeriesCopyTags",
            std::move(dest_group),
            std::move(right_argument_group),
            std::make_shared<ASTLiteral>(Array{tags_to_copy.begin(), tags_to_copy.end()}));
    }


    ASTPtr makeExpressionForResultGroup_GroupRight(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && left_argument_group,
        ASTPtr && right_argument_group,
        ASTPtr && join_group,
        bool metric_name_dropped_from_left,
        bool metric_name_dropped_from_right,
        bool metric_name_dropped_from_join_group,
        bool & metric_name_dropped_from_result)
    {
        return makeExpressionForResultGroup_GroupLeft(operator_node,
                                                      std::move(right_argument_group), std::move(left_argument_group), std::move(join_group),
                                                      metric_name_dropped_from_right, metric_name_dropped_from_left, metric_name_dropped_from_join_group,
                                                      metric_name_dropped_from_result);
    }


    /// Returns an AST to evaluate the group which will be set for the result of a binary operator on instant vectors.
    ASTPtr makeExpressionForResultGroup(
        const PQT::BinaryOperator * operator_node,
        ASTPtr && left_argument_group,
        ASTPtr && right_argument_group,
        ASTPtr && join_group,
        bool metric_name_dropped_from_left,
        bool metric_name_dropped_from_right,
        bool metric_name_dropped_from_join_group,
        bool & metric_name_dropped_from_result)
    {
        chassert(!isSetOperator(operator_node->operator_name));
        if (operator_node->group_left)
        {
            return makeExpressionForResultGroup_GroupLeft(operator_node,
                                                          std::move(left_argument_group), std::move(right_argument_group), std::move(join_group),
                                                          metric_name_dropped_from_left, metric_name_dropped_from_right, metric_name_dropped_from_join_group,
                                                          metric_name_dropped_from_result);
        }
        else if (operator_node->group_right)
        {
            return makeExpressionForResultGroup_GroupRight(operator_node,
                                                           std::move(left_argument_group), std::move(right_argument_group), std::move(join_group),
                                                           metric_name_dropped_from_left, metric_name_dropped_from_right, metric_name_dropped_from_join_group,
                                                           metric_name_dropped_from_result);
        }
        else
        {
            return makeExpressionForResultGroup_Default(operator_node,
                                                        std::move(left_argument_group), std::move(right_argument_group), std::move(join_group),
                                                        metric_name_dropped_from_left, metric_name_dropped_from_right, metric_name_dropped_from_join_group,
                                                        metric_name_dropped_from_result);
        }
    }


    /// Applies a binary operator if both of the arguments are instant vectors.
    SQLQueryPiece binaryOperatorWithVectors(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context)
    {
        left_argument = toVectorGrid(std::move(left_argument), context);
        right_argument = toVectorGrid(std::move(right_argument), context);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        String left = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        String right = context.subqueries.back().name;

        bool metric_name_dropped_from_join_group;
        auto join_group = makeExpressionForJoinGroup(
            operator_node,
            std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}),
            left_argument.metric_name_dropped,
            metric_name_dropped_from_join_group);

        bool metric_name_dropped_from_result;
        auto result_group = makeExpressionForResultGroup(
            operator_node,
            std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}),
            std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Group}),
            std::make_shared<ASTIdentifier>(ColumnNames::JoinGroup),
            left_argument.metric_name_dropped,
            right_argument.metric_name_dropped,
            metric_name_dropped_from_join_group,
            metric_name_dropped_from_result);

        /// SELECT timeSeriesCopyTags(join_group, left.group, tags_to_copy) AS group,
        ///        timeSeriesCoalesceGridValues('throw')(arrayMap(x, y -> x + y, left.values, right.values), group) AS values
        /// FROM left INNER ALL JOIN right
        /// ON (timeSeriesRemoveTags(left.group, on_tags) AS join_group) == timeSeriesRemoveTags(right.group, on_tags)
        /// GROUP BY group

        SelectQueryParams params;

        params.select_list.push_back(result_group);
        params.select_list.back()->setAlias(ColumnNames::Group);

        params.select_list.push_back(addParametersToAggregateFunction(
            makeASTFunction(
                "timeSeriesCoalesceGridValues",
                makeOperatorASTForTwoArrays(
                    operator_node,
                    std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Values}),
                    /* left_to_right = */ true,
                    /* return_array_argument_if_match = */ true),
                std::make_shared<ASTIdentifier>(ColumnNames::Group)),
            std::make_shared<ASTLiteral>("throw")));

        params.select_list.back()->setAlias(ColumnNames::Values);

        params.from_table = left;
        params.join_kind = JoinKind::Inner;
        params.join_strictness = JoinStrictness::All;
        params.join_table = right;

        join_group->setAlias(ColumnNames::JoinGroup);
        params.join_on = makeASTFunction(
            "equals",
            std::move(join_group),
            makeExpressionForJoinGroup(
                operator_node, std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Group}), right_argument.metric_name_dropped));

        params.group_by.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

        SQLQueryPiece res = left_argument;
        res.node = operator_node;
        res.select_query = buildSelectQuery(std::move(params));
        res.store_method = StoreMethod::VECTOR_GRID;
        res.metric_name_dropped = metric_name_dropped_from_result;

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

        left_argument = toVectorGrid(std::move(left_argument), context);
        right_argument = toVectorGrid(std::move(right_argument), context);

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

            params.select_list.push_back(makeExpressionForJoinGroup(
                operator_node, std::make_shared<ASTIdentifier>(ColumnNames::Group), right_argument.metric_name_dropped));
            params.select_list.back()->setAlias(ColumnNames::JoinGroup);

            params.select_list.push_back(addParametersToAggregateFunction(
                makeASTFunction("timeSeriesCoalesceGridValues", std::make_shared<ASTIdentifier>(ColumnNames::Values)),
                std::make_shared<ASTLiteral>("any")));
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
                        std::make_shared<ASTIdentifier>("x"))),
                std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                std::make_shared<ASTIdentifier>(Strings{step1, ColumnNames::Values})));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.from_table = left;
            params.join_kind = JoinKind::Left;
            params.join_strictness = JoinStrictness::Semi;
            params.join_table = step1;

            params.join_on = makeASTFunction(
                "equals",
                makeExpressionForJoinGroup(
                    operator_node, std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}), left_argument.metric_name_dropped),
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

        left_argument = toVectorGrid(std::move(left_argument), context);
        right_argument = toVectorGrid(std::move(right_argument), context);

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

            params.select_list.push_back(makeExpressionForJoinGroup(
                operator_node, std::make_shared<ASTIdentifier>(ColumnNames::Group), right_argument.metric_name_dropped));
            params.select_list.back()->setAlias(ColumnNames::JoinGroup);

            params.select_list.push_back(addParametersToAggregateFunction(
                makeASTFunction("timeSeriesCoalesceGridValues", std::make_shared<ASTIdentifier>(ColumnNames::Values)),
                std::make_shared<ASTLiteral>("any")));
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
                            std::make_shared<ASTLiteral>(Field{} /* NULL */))),
                    std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    std::make_shared<ASTIdentifier>(Strings{step1, ColumnNames::Values}))));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.from_table = left;
            params.join_kind = JoinKind::Left;
            params.join_strictness = JoinStrictness::Any;
            params.join_table = step1;

            params.join_on = makeASTFunction(
                "equals",
                makeExpressionForJoinGroup(
                    operator_node, std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Group}), left_argument.metric_name_dropped),
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

        left_argument = toVectorGrid(std::move(left_argument), context);
        right_argument = toVectorGrid(std::move(right_argument), context);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        String left = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        String right = context.subqueries.back().name;

        /// Step 1:
        /// SELECT group,
        ///        if(empty(right.values), left.values, arrayMap(x, y -> if(isNull(x), y, x), left.values, right.values)) AS values
        /// FROM left LEFT ANY JOIN right
        /// ON left.group == right.group
        String step1;
        {
            SelectQueryParams params;

            params.select_list.push_back(std::make_shared<ASTIdentifier>(ColumnNames::Group));

            params.select_list.push_back(makeASTFunction(
                "if",
                makeASTFunction("empty", std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Values})),
                std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                makeASTFunction(
                    "arrayMap",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x"), std::make_shared<ASTIdentifier>("y")),
                        makeASTFunction(
                            "if",
                            makeASTFunction("isNull", std::make_shared<ASTIdentifier>("x")),
                            std::make_shared<ASTIdentifier>("y"),
                            std::make_shared<ASTIdentifier>("x"))),
                    std::make_shared<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Values}))));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.from_table = left;
            params.join_kind = JoinKind::Left;
            params.join_strictness = JoinStrictness::Any;
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

            params.select_list.push_back(makeExpressionForJoinGroup(
                operator_node, std::make_shared<ASTIdentifier>(ColumnNames::Group), left_argument.metric_name_dropped));
            params.select_list.back()->setAlias(ColumnNames::JoinGroup);

            params.select_list.push_back(addParametersToAggregateFunction(
                makeASTFunction("timeSeriesCoalesceGridValues", std::make_shared<ASTIdentifier>(ColumnNames::Values)), std::make_shared<ASTLiteral>("any")));
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
                            std::make_shared<ASTLiteral>(Field{} /* NULL */))),
                    std::make_shared<ASTIdentifier>(Strings{step2, ColumnNames::Values}),
                    std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Values}))));
            params.select_list.back()->setAlias(ColumnNames::Values);

            params.from_table = step2;
            params.join_kind = JoinKind::Right;
            params.join_strictness = JoinStrictness::Any;
            params.join_table = right;

            params.join_on = makeASTFunction(
                "equals",
                std::make_shared<ASTIdentifier>(Strings{step2, ColumnNames::JoinGroup}),
                makeExpressionForJoinGroup(
                    operator_node,
                    std::make_shared<ASTIdentifier>(Strings{right, ColumnNames::Group}),
                    right_argument.metric_name_dropped));

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

    return binaryOperatorWithVectors(operator_node, std::move(left_argument), std::move(right_argument), context);
}
}
