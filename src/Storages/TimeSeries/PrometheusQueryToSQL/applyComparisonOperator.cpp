#include <Storages/TimeSeries/PrometheusQueryToSQL/applyComparisonOperator.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <unordered_map>


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
        std::string_view operator_name = operator_node->operator_name;

        if ((left_argument.type != ResultType::SCALAR) && (left_argument.type != ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                            getPromQLText(left_argument, context), left_argument.type);
        }

        if ((right_argument.type != ResultType::SCALAR) && (right_argument.type != ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                            getPromQLText(right_argument, context), right_argument.type);
        }

        if ((left_argument.type == ResultType::SCALAR) && (right_argument.type == ResultType::SCALAR) && !operator_node->bool_modifier)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Comparison operator '{}' on scalars requires the bool modifier",
                            operator_name);
        }

        if ((left_argument.type != ResultType::INSTANT_VECTOR) || (right_argument.type != ResultType::INSTANT_VECTOR))
        {
            if (operator_node->group_left || operator_node->group_right)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Binary operator '{}' with the group modifier expects two arguments of type {}, got {} and {}",
                                operator_name, ResultType::INSTANT_VECTOR, left_argument.type, right_argument.type);
            }
        }
    }

    struct ImplInfo
    {
        std::string_view ch_function_name;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"==", {"equals"}},
            {"!=", {"notEquals"}},
            {">",  {"greater"}},
            {"<",  {"less"}},
            {">=", {"greaterOrEquals"}},
            {"<=", {"lessOrEquals"}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isComparisonOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyComparisonOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    const auto & operator_name = operator_node->operator_name;
    const auto * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    /// If the bool modifier is specified then the operator works like arithmetic operators
    /// in terms of label handling.
    if (operator_node->bool_modifier)
    {
        auto apply_function_to_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr
        {
            return timeSeriesScalarASTCast(
                makeASTFunction(impl_info->ch_function_name, std::move(x), std::move(y)), context.scalar_data_type);
        };

        return applySimpleBinaryOperator(
            operator_node,
            std::move(left_argument),
            std::move(right_argument),
            context,
            apply_function_to_ast,
            /* drop_metric_name = */ true,
            /* allow_grouping_modifier_copy_metric_name = */ false);
    }
    else
    {
        /// If the bool modifier isn't specified then the operator works like a filter.

        /// At least one argument should be an instant vector (checkArgumentTypes has already checked that).
        chassert((left_argument.type == ResultType::INSTANT_VECTOR) || (right_argument.type == ResultType::INSTANT_VECTOR));

        /// We filter either values from the left side or from the right side.
        bool filter_left = (left_argument.type == ResultType::INSTANT_VECTOR);

        /// One-step grid vs scalar: drop failing rows with WHERE instead of nulling via arrayMap.
        {
            SQLQueryPiece & vector_arg = filter_left ? left_argument : right_argument;
            const SQLQueryPiece & scalar_arg = filter_left ? right_argument : left_argument;

            bool scalar_is_const = (scalar_arg.store_method == StoreMethod::CONST_SCALAR);
            bool vector_is_grid = (vector_arg.store_method != StoreMethod::EMPTY);
            bool single_step = (vector_arg.type == ResultType::INSTANT_VECTOR)
                && (stepsInTimeSeriesRange(vector_arg.start_time, vector_arg.end_time, vector_arg.step) == 1);

            if (scalar_is_const && vector_is_grid && single_step)
            {
                ScalarType scalar_value = scalar_arg.scalar_value;

                SQLQueryPiece gridded = toVectorGrid(std::move(vector_arg), context);
                context.subqueries.emplace_back(
                    SQLSubquery{context.subqueries.size(), std::move(gridded.select_query), SQLSubqueryType::TABLE});

                SelectQueryBuilder builder;
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
                builder.from_table = context.subqueries.back().name;

                ASTPtr value_at_step = makeASTFunction(
                    "arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u));
                ASTPtr scalar_ast = timeSeriesScalarToAST(scalar_value, context.scalar_data_type);

                builder.where = filter_left
                    ? makeASTFunction(String{impl_info->ch_function_name}, value_at_step, scalar_ast)
                    : makeASTFunction(String{impl_info->ch_function_name}, scalar_ast, value_at_step);

                SQLQueryPiece res{operator_node, operator_node->result_type, StoreMethod::VECTOR_GRID};
                res.select_query = builder.getSelectQuery();
                res.start_time = gridded.start_time;
                res.end_time = gridded.end_time;
                res.step = gridded.step;
                res.metric_name_dropped = gridded.metric_name_dropped;
                return res;
            }
        }

        /// For example for the ">=" operator and `filter_left == true`
        /// here we build the following expression for filtering:
        /// if(x >= y, x, NULL)
        auto apply_function_to_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr
        {
            ASTPtr res = filter_left ? x->clone() : y->clone();
            return makeASTFunction("if",
                makeASTFunction(impl_info->ch_function_name, std::move(x), std::move(y)),
                std::move(res),
                make_intrusive<ASTLiteral>(Field{} /* NULL */));
        };

        /// Since our expression may convert values to NULL we always need VECTOR_GRID
        /// to represent the result. So we cast arguments to VECTOR_GRID to enforce that.
        if (left_argument.type == ResultType::INSTANT_VECTOR)
            left_argument = toVectorGrid(std::move(left_argument), context);

        if (right_argument.type == ResultType::INSTANT_VECTOR)
            right_argument = toVectorGrid(std::move(right_argument), context);

        return applySimpleBinaryOperator(
            operator_node,
            std::move(left_argument),
            std::move(right_argument),
            context,
            apply_function_to_ast,
            /* drop_metric_name = */ false,
            /* allow_grouping_modifier_copy_metric_name = */ true);
    }
}

}
