#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Implements the CASE construction when it is
/// provided an expression. Users should not call this function.
class FunctionCaseWithExpression : public IFunction
{
public:
    static constexpr auto name = "caseWithExpression";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionCaseWithExpression>(context_); }

    explicit FunctionCaseWithExpression(ContextPtr context_) : context(context_) {}
    bool isVariadic() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const DataTypes & args) const override
    {
        if (args.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects at least 1 arguments", getName());

        /// We expect an even number of arguments, with the first argument being the expression,
        /// and the last argument being the ELSE branch, with the rest being pairs of WHEN and THEN branches.
        if (args.size() < 4 || args.size() % 2 != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} expects an even number of arguments: (expr, when1, then1, ..., else)", getName());

        /// See the comments in executeImpl() to understand why we actually have to
        /// get the return type of a transform function.

        /// Get the types of the arrays that we pass to the transform function.
        DataTypes dst_array_types;

        for (size_t i = 2; i < args.size() - 1; i += 2)
            dst_array_types.push_back(args[i]);

        // Type of the ELSE branch
        dst_array_types.push_back(args.back());

        return getLeastSupertype(dst_array_types);
    }

    /// Helper function to implement CASE WHEN equality semantics where NULL = NULL is true
    ColumnPtr caseWhenEquals(const ColumnWithTypeAndName & expr, const ColumnWithTypeAndName & when_value, size_t input_rows_count) const
    {
        // handle Nothing type - it's an empty type that can't contain any values
        // if either argument is Nothing, the result should be an empty column
        if (expr.type->onlyNull() || when_value.type->onlyNull())
        {
            // return a constant false column
            return DataTypeUInt8().createColumnConst(input_rows_count, 0u);
        }

        // for CASE WHEN semantics, NULL should match NULL
        // we need: if (isNull(expr)) then (isNull(when)) else if (isNull(when)) then 0 else (expr = when)

        auto is_null_func = FunctionFactory::instance().get("isNull", context);
        auto if_func = FunctionFactory::instance().get("if", context);
        auto equals_func = FunctionFactory::instance().get("equals", context);

        // isNull(expr)
        ColumnsWithTypeAndName is_null_expr_args{expr};
        auto is_null_expr = is_null_func->build(is_null_expr_args)
            ->execute(is_null_expr_args, std::make_shared<DataTypeUInt8>(), input_rows_count, false);

        // isNull(when)
        ColumnsWithTypeAndName is_null_when_args{when_value};
        auto is_null_when = is_null_func->build(is_null_when_args)
            ->execute(is_null_when_args, std::make_shared<DataTypeUInt8>(), input_rows_count, false);

        // expr = when
        ColumnsWithTypeAndName equals_args{expr, when_value};

        // determine return type for equals
        bool needs_nullable = expr.type->isNullable() || when_value.type->isNullable();
        DataTypePtr equals_return_type;
        if (needs_nullable)
            equals_return_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
        else
            equals_return_type = std::make_shared<DataTypeUInt8>();

        auto equals_result = equals_func->build(equals_args)
            ->execute(equals_args, equals_return_type, input_rows_count, false);

        // convert nullable equals result to non-nullable
        if (isColumnNullable(*equals_result))
        {
            auto if_null_func = FunctionFactory::instance().get("ifNull", context);
            auto zero_const = DataTypeUInt8().createColumnConst(input_rows_count, 0u);
            ColumnsWithTypeAndName if_null_args
            {
                {equals_result, equals_return_type, ""},
                {zero_const, std::make_shared<DataTypeUInt8>(), ""}
            };
            equals_result = if_null_func->build(if_null_args)
                ->execute(if_null_args, std::make_shared<DataTypeUInt8>(), input_rows_count, false);
        }

        // if (isNull(when)) then 0 else (expr = when)
        auto zero_const = DataTypeUInt8().createColumnConst(input_rows_count, 0u);
        ColumnsWithTypeAndName inner_if_args
        {
            {is_null_when, std::make_shared<DataTypeUInt8>(), ""},
            {zero_const, std::make_shared<DataTypeUInt8>(), ""},
            {equals_result, std::make_shared<DataTypeUInt8>(), ""}
        };
        auto inner_if_result = if_func->build(inner_if_args)
            ->execute(inner_if_args, std::make_shared<DataTypeUInt8>(), input_rows_count, false);

        // if (isNull(expr)) then (isNull(when)) else inner_if_result
        ColumnsWithTypeAndName outer_if_args
        {
            {is_null_expr, std::make_shared<DataTypeUInt8>(), ""},
            {is_null_when, std::make_shared<DataTypeUInt8>(), ""},
            {inner_if_result, std::make_shared<DataTypeUInt8>(), ""}
        };
        return if_func->build(outer_if_args)
            ->execute(outer_if_args, std::make_shared<DataTypeUInt8>(), input_rows_count, false);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (args.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects at least 1 argument", getName());

        bool all_when_then_values_constant = true;
        for (size_t i = 1; i < args.size() - 1; i += 2)
        {
            // i is the WHEN value
            if (!isColumnConst(*args[i].column))
            {
                all_when_then_values_constant = false;
                break;
            }
            // i+1 is the THEN value
            if (!isColumnConst(*args[i+1].column))
            {
                all_when_then_values_constant = false;
                break;
            }
        }

        /// In the following code, we turn the construction:
        /// CASE expr WHEN val[0] THEN branch[0] ... WHEN val[N-1] then branch[N-1] ELSE branchN
        /// into the construction transform(expr, src, dest, branchN)
        /// where:
        /// src = [val[0], val[1], ..., val[N-1]]
        /// dst = [branch[0], ..., branch[N-1]]
        /// then we perform it.

        /// Create the arrays required by the transform function.
        ColumnsWithTypeAndName src_array_elems;
        DataTypes src_array_types;

        ColumnsWithTypeAndName dst_array_elems;
        DataTypes dst_array_types;

        for (size_t i = 1; i < (args.size() - 1); ++i)
        {
            if (i % 2)
            {
                src_array_elems.push_back(args[i]);
                src_array_types.push_back(args[i].type);
            }
            else
            {
                dst_array_elems.push_back(args[i]);
                dst_array_types.push_back(args[i].type);
            }
        }

        DataTypePtr src_array_type = std::make_shared<DataTypeArray>(getLeastSupertype(src_array_types));
        DataTypePtr dst_array_type = std::make_shared<DataTypeArray>(getLeastSupertype(dst_array_types));

        ColumnWithTypeAndName src_array_col{nullptr, src_array_type, ""};
        ColumnWithTypeAndName dst_array_col{nullptr, dst_array_type, ""};

        auto fun_array = FunctionFactory::instance().get("array", context);

        src_array_col.column = fun_array->build(src_array_elems)->execute(src_array_elems, src_array_type, input_rows_count, /* dry_run = */ false);
        dst_array_col.column = fun_array->build(dst_array_elems)->execute(dst_array_elems, dst_array_type, input_rows_count, /* dry_run = */ false);

        /// If we have non-constant arguments, we execute the transform function, which is highly optimized, which uses precomputed lookup tables.
        /// Else we should use the multiIf implementation
        if (all_when_then_values_constant)
        {
            ColumnsWithTypeAndName transform_args{args.front(), src_array_col, dst_array_col, args.back()};
            return FunctionFactory::instance().get("transform", context)->build(transform_args)
                ->execute(transform_args, result_type, input_rows_count, /* dry_run = */ false);
        }
        else
        {
            ColumnsWithTypeAndName multi_if_args;

            // Convert CASE expression into multiIf(expr = when1, then1, expr = when2, then2, ..., else)
            for (size_t i = 1; i < args.size() - 1; i += 2)
            {
                // use CASE WHEN equality semantics (NULL = NULL is true)
                auto condition = caseWhenEquals(args.front(), args[i], input_rows_count);

                multi_if_args.push_back({condition, std::make_shared<DataTypeUInt8>(), ""});
                multi_if_args.push_back(args[i + 1]); // Then value
            }

            // Add an ELSE value
            multi_if_args.push_back(args.back());

            // Execute multiIf
            return FunctionFactory::instance().get("multiIf", context)
                ->build(multi_if_args)
                ->execute(multi_if_args, result_type, input_rows_count, false);
        }
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(CaseWithExpression)
{
    factory.registerFunction<FunctionCaseWithExpression>();

    /// These are obsolete function names.
    factory.registerAlias("caseWithExpr", "caseWithExpression");
}

}
