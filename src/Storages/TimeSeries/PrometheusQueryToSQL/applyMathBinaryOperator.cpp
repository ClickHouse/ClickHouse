#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>
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

        if (operator_node->bool_modifier)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' doesn't allow bool modifier",
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
            {"+",     {"plus"}},
            {"-",     {"minus"}},
            {"*",     {"multiply"}},
            {"/",     {"divide"}},
            {"%",     {"modulo"}},
            {"^",     {"pow"}},
            {"atan2", {"atan2"}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }

    /// A permanently-NULL histogram arm: the operation is never allowed for histogram operands
    /// (upstream's IncompatibleTypes error drops the sample, see `vectorElemBinop`).
    ASTPtr nullHistogramArm()
    {
        return make_intrusive<ASTLiteral>(Field{});
    }

    ASTPtr kindEquals(ASTPtr kind, Float64 value)
    {
        return makeASTFunction("equals", std::move(kind), make_intrusive<ASTLiteral>(value));
    }

    /// The histogram arm of an arithmetic binary operator, mirroring the `hlhs`/`hrhs` cases of
    /// `vectorElemBinop` in Prometheus promql/engine.go (see tmp/prom_engine.go).
    ASTPtr buildHistogramArm(std::string_view operator_name, const SimpleBinaryOperatorHistogramArm::Input & input)
    {
        /// A scalar side has no histogram samples, so `isNotNull` guards on its arm are unnecessary.
        if (operator_name == "+" || operator_name == "-")
        {
            if (input.left_is_scalar || input.right_is_scalar)
                return nullHistogramArm();

            const char * ch_function = (operator_name == "+") ? "timeSeriesHistogramAdd" : "timeSeriesHistogramSub";
            return makeASTFunction(
                "if",
                makeASTFunction(
                    "and",
                    kindEquals(input.left_kind->clone(), 1),
                    kindEquals(input.right_kind->clone(), 1)),
                makeASTFunction(ch_function, input.left_histogram->clone(), input.right_histogram->clone()),
                make_intrusive<ASTLiteral>(Field{}));
        }

        if (operator_name == "*")
        {
            /// float*histo and histo*float: histo.Copy().Mul(scalar) (Mul is commutative here).
            if (input.left_is_scalar)
                return makeASTFunction(
                    "if",
                    kindEquals(input.right_kind->clone(), 1),
                    makeASTFunction("timeSeriesHistogramMulByScalar", input.right_histogram->clone(), input.left_value->clone()),
                    make_intrusive<ASTLiteral>(Field{}));
            if (input.right_is_scalar)
                return makeASTFunction(
                    "if",
                    kindEquals(input.left_kind->clone(), 1),
                    makeASTFunction("timeSeriesHistogramMulByScalar", input.left_histogram->clone(), input.right_value->clone()),
                    make_intrusive<ASTLiteral>(Field{}));

            return makeASTFunction(
                "if",
                makeASTFunction(
                    "and",
                    kindEquals(input.left_kind->clone(), 1),
                    kindEquals(input.right_kind->clone(), 0)),
                makeASTFunction("timeSeriesHistogramMulByScalar", input.left_histogram->clone(), input.right_value->clone()),
                makeASTFunction(
                    "if",
                    makeASTFunction(
                        "and",
                        kindEquals(input.left_kind->clone(), 0),
                        kindEquals(input.right_kind->clone(), 1)),
                    makeASTFunction("timeSeriesHistogramMulByScalar", input.right_histogram->clone(), input.left_value->clone()),
                    make_intrusive<ASTLiteral>(Field{})));
        }

        if (operator_name == "/")
        {
            /// histo/float: histo.Copy().Div(scalar); float/histo is NOT allowed.
            if (input.left_is_scalar)
                return nullHistogramArm();
            if (input.right_is_scalar)
                return makeASTFunction(
                    "if",
                    kindEquals(input.left_kind->clone(), 1),
                    makeASTFunction("timeSeriesHistogramDivByScalar", input.left_histogram->clone(), input.right_value->clone()),
                    make_intrusive<ASTLiteral>(Field{}));

            return makeASTFunction(
                "if",
                makeASTFunction(
                    "and",
                    kindEquals(input.left_kind->clone(), 1),
                    kindEquals(input.right_kind->clone(), 0)),
                makeASTFunction("timeSeriesHistogramDivByScalar", input.left_histogram->clone(), input.right_value->clone()),
                make_intrusive<ASTLiteral>(Field{}));
        }

        /// `%`, `^`, `atan2`: never allowed for histogram operands.
        return nullHistogramArm();
    }
}

bool isMathBinaryOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyMathBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    const auto & operator_name = operator_node->operator_name;
    const auto * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    auto apply_function_to_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr
    {
        if (operator_name != "%")
            return makeASTFunction(impl_info->ch_function_name, std::move(x), std::move(y));

        ASTPtr result = makeASTFunction(impl_info->ch_function_name, x->clone(), y->clone());

        return makeASTFunction(
            "if",
            makeASTFunction(
                "and",
                makeASTFunction("isInfinite", y->clone()),
                makeASTFunction("isFinite", x->clone())),
            std::move(x),
            std::move(result));
    };

    /// The histogram arm is engaged only when at least one operand is a combined
    /// float+histogram grid (StoreMethod::HISTOGRAM_GRID, see applySimpleBinaryOperator).
    SimpleBinaryOperatorHistogramArm histogram_arm;
    histogram_arm.build_histogram_values_arm = [op_name = operator_node->operator_name](const SimpleBinaryOperatorHistogramArm::Input & input)
    {
        return buildHistogramArm(op_name, input);
    };

    return applySimpleBinaryOperator(
        operator_node,
        std::move(left_argument),
        std::move(right_argument),
        context,
        apply_function_to_ast,
        /* drop_metric_name = */ true,
        /* allow_grouping_modifier_copy_metric_name = */ true,
        &histogram_arm);
}

}
