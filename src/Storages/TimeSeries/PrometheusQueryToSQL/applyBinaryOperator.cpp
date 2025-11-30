#if 0
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
        if (!(operator_name == "+" || operator_name == "-" || operator_name == ""))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Unknown unary operator with name {}", operator_name);
        }

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

    /// Binary operator when one of the arguments is of type ResultType::SCALAR with StoreMethod::CONST_SCALAR.
    /// Other argument can have any type.
    SQLQueryPiece binaryOperatorWithConstScalar(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && const_scalar_argument,
        size_t const_scalar_argument_index,
        SQLQueryPiece && other_argument,
        ConverterContext & context)
    {
        chassert(const_scalar_argument.store_method == StoreMethod::CONST_SCALAR);
        chassert(const_scalar_argument.type == ResultType::SCALAR || const_scalar_argument.store_method == StoreMethod::INSTANT_VECTOR);

        const auto & operator_name = operator_node->operator_name;

        switch (other_argument.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                SQLQueryPiece res{operator_node, other_argument.type, other_argument.store_method};
                res.start_time = other_argument.start_time;
                res.end_time = other_argument.end_time;
                res.step = other_argument.step;
                Float64 const_value = const_scalar_argument.scalar_value;
                Float64 other_value = other_argument.scalar_value;

                if (operator_name == "+")
                {
                    res.scalar_value = left + right;
                }
                else if (operator_name == "-")
                {
                    res.scalar_value = left - right;
                }
                else if (operator_name == "*")
                {
                    res.scalar_value = left * right;
                }
                else if (operator_name == "/")
                {
                    res.scalar_value = const_scalar_argument_index ? (other_value / const_value) : (const_scalar_argument.scalar_value / other_argument.scalar_value);
                }
                else if (operator_name == "%")
                {
                    res.scalar_value = const_scalar_argument_index ? fmod(other_argument.scalar_value, const_scalar_argument.scalar_value)
                                                                   : fmod(const_scalar_argument.scalar_value, other_argument.scalar_value);
                }
                else if (operator_name == "^")
                {
                    res.scalar_value = const_scalar_argument_index ? pow(other_argument.scalar_value, const_scalar_argument.scalar_value)
                                                                   : pow(const_scalar_argument.scalar_value, other_argument.scalar_value);
                }
                else if (operator_name == "atan2")
                {
                    res.scalar_value = const_scalar_argument_index ? atan2(other_argument.scalar_value, const_scalar_argument.scalar_value)
                                                                   : atan2(const_scalar_argument.scalar_value, other_argument.scalar_value);
                }
                else
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operator '{}' is not implemented", operator_name);
                }
                return res;
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                SQLQueryPiece res{operator_node, other_argument.type, other_argument.store_method};
                res.start_time = other_argument.start_time;
                res.end_time = other_argument.end_time;
                res.step = other_argument.step;

                SelectQueryParams params;
                if (other_argument.store_method == StoreMethod::VECTOR_GRID)
                    params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

                ASTPtr transform;
                if (operator_name == "+")
                {
                    transform = makeASTFunction("plus", x, scalar_value);
                }
                else if (operator_name == "-")
                {
                    res.scalar_value = const_scalar_argument_index ? (other_argument.scalar_value - const_scalar_argument.scalar_value)
                                                                   : (const_scalar_argument.scalar_value - other_argument.scalar_value);
                }
                else if (operator_name == "*")
                {
                    res.scalar_value = const_scalar_argument.scalar_value * other_argument.scalar_value;
                }
                else if (operator_name == "/")
                {
                    res.scalar_value = const_scalar_argument_index ? (other_argument.scalar_value / const_scalar_argument.scalar_value)
                                                                   : (const_scalar_argument.scalar_value / other_argument.scalar_value);
                }
                else if (operator_name == "%")
                {
                    res.scalar_value = const_scalar_argument_index ? fmod(other_argument.scalar_value, const_scalar_argument.scalar_value)
                                                                   : fmod(const_scalar_argument.scalar_value, other_argument.scalar_value);
                }
                else if (operator_name == "^")
                {
                    res.scalar_value = const_scalar_argument_index ? pow(other_argument.scalar_value - const_scalar_argument.scalar_value)
                                                                   : pow(const_scalar_argument.scalar_value - other_argument.scalar_value);
                }
                else if (operator_name == "atan2")
                {
                    res.scalar_value = const_scalar_argument_index ? atan2(other_argument.scalar_value - const_scalar_argument.scalar_value)
                                                                   : atan2(const_scalar_argument.scalar_value - other_argument.scalar_value);
                }
                else
                {
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Operator '{}' is not implemented", operator_name);
                }                
            }
        }

        return res;
    }

    void binaryOperatorWithScalarGridAsScalar(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && scalar_grid_argument,
        bool const_scalar_argument_is_right,
        SQLQueryPiece && other_argument,
        ConverterContext & context)

    void binaryOperatorWithConstScalarAsVector(

    void binaryOperatorWithScalarGridAsVector(

    void binaryOperatorWithVectorGrids()
    {

    }

}


SQLQueryPiece applyBinaryOperatorWithScalar(
    const PrometheusQueryTree::BinaryOperator & operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    if ((left_argument.type == ResultType::SCALAR) && (left_argument.store_method == StoreMethod::CONST_SCALAR))
        return binaryOperatorWithConstScalarAsScalar(operator_node, std::move(left_argument), 0, std::move(right_argument), context);
        
    if ((right_argument.type == ResultType::SCALAR) && (right_argument.store_method == StoreMethod::CONST_SCALAR))
        return binaryOperatorWithConstScalarAsScalar(operator_node, std::move(left_argument), 1, std::move(right_argument), context);

    if ((left_argument.type == ResultType::SCALAR) && (left_argument.store_method == StoreMethod::SCALAR_GRID))
        return binaryOperatorWithScalarGridAsScalar(operator_node, std::move(left_argument), 0, std::move(right_argument), context);

    if ((right_argument.type == ResultType::SCALAR) && (right_argument.store_method == StoreMethod::SCALAR_GRID))
        return binaryOperatorWithScalarGridAsScalar(operator_node, std::move(left_argument), 1, std::move(right_argument), context);

    throw Exception(ErrorCodes::LOGICAL_ERROR);
}
    

        if (left_argument.store_method == StoreMethod::SCALAR_GRID)


    }

    if (left_argument.store_method == StoreMethod::CONST_SCALAR)
    {
        if (left_argument.type == ResultType::SCALAR)
            return binaryOperatorWithConstScalarAsScalar(operator_node, std::move(left_argument), 0, std::move(right_argument), context);
        else
            return binaryOperatorWithConstScalarAsVector(operator_node, std::move(left_argument), 0, std::move(right_argument), context);
    }

    if (right_argument.store_method == StoreMethod::CONST_SCALAR)
    {
        if (right_argument.type == ResultType::SCALAR)
            return binaryOperatorWithConstScalarAsScalar(operator_node, std::move(right_argument), 1, std::move(left_argument), context);
        else
            return binaryOperatorWithConstScalarAsVector(operator_node, std::move(right_argument), 1, std::move(left_argument), context);
    }

    if (left_argument.store_method == StoreMethod::SCALAR_GRID)
    {
        if (left_argument.type == ResultType::SCALAR)
            return binaryOperatorWithScalarGridAsScalar(operator_node, std::move(left_argument), 0, std::move(right_argument), context);
        else
            return binaryOperatorWithScalarGridAsVector(operator_node, std::move(left_argument), 0, std::move(right_argument), context);
    }

    if (right_argument.store_method == StoreMethod::SCALAR_GRID)
    {
        if (right_argument.type == ResultType::SCALAR)
            return binaryOperatorWithScalarGridAsScalar(operator_node, std::move(right_argument), 1, std::move(left_argument), context);
        else
            return binaryOperatorWithScalarGridAsVector(operator_node, std::move(right_argument), 1, std::move(left_argument), context);
    }

    return binaryOperatorWithVectorGrids(operator_node, std::move(left_argument), std::move(right_argument), context);
}


namespace
{
    Float64 evaluateConstBinaryOperator(std::string_view operator_name, Float64 left, Float64 right)
    {
        const auto & operator_name = operator_node->operator_name;
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
        const SQLQueryPiece & scalar_argument = argument_index ? right_argument : left_argument;
        const SQLQueryPiece & other_argument = argument_index ? left_argument : right_argument;
        chassert((scalar_argument.type == ResultType::SCALAR) && (scalar_argument.store_method == StoreMethod::CONST_SCALAR));

        SQLQueryPiece res = other_argument;
        res.node = operator_node;

        switch (other_argument.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                Float64 float_result = evaluateConstBinaryOperator(operator_name, left_argument.scalar_value, right_argument.scalar_value);
                if ((other_argument.type == ResultType::INSTANT_VECTOR) && isComparisonWithoutBool(operator_node))
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
                /// For scalar grid:
                /// SELECT arrayMap(x -> x + <const_scalar>, values) AS values
                /// FROM <scalar_grid>
                ///
                /// For vector grid:
                /// SELECT group, arrayMap(x -> x + <const_scalar>, values) AS values
                /// FROM <vector_grid>
                ///
                /// For comparison operator without bool modifier, scalar grid:
                /// SELECT 0 AS group, arrayMap(x -> if (x == <const_scalar>, x, NULL), values) AS values
                /// FROM <scalar_grid>
                ///
                /// For comparison operator without bool modifier, vector grid:
                /// SELECT group, arrayMap(x -> if (x == <const_scalar>, x, NULL), values) AS values
                /// FROM <vector_grid>
                SelectQueryParams params;

                bool is_comparison_without_bool = isComparisonWithoutBool(operator_node);

                if (store_method == StoreMethod::VECTOR_GRID)
                {
                    params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
                }
                else if (is_comparison_without_bool)
                {
                    params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
                    params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);
                }

                ASTPtr left_ast = argument_index ? std::make_shared<ASTIdentifier>("x") : std::make_shared<ASTLiteral>(scalar_argument.scalar_value);
                ASTPtr right_ast = argument_index ? std::make_shared<ASTLiteral>(scalar_argument.scalar_value) : std::make_shared<ASTIdentifier>("x");

                ASTPtr transform_ast = makeTransformAST(operator_name, left_ast, right_ast);

            }

            {

            }
        }
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
        throw;
    }

    if (right.argument.store_method != StoreMethod::VECTOR_GRID)
    {
        throw;
    }

    return binaryOperatorWithVectorsByVectorGrids(operator_node, left_argument, right_argument, context);
}

}
