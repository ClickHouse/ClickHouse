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

#endif
