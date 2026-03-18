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
            bool needs_nullable = args.front().type->isNullable();
            for (size_t i = 1; i < args.size() - 1; i += 2)
            {
                if (args[i].type->isNullable())
                {
                    needs_nullable = true;
                    break;
                }
            }

            /// the correct return type for equals(), it should respect NULLs
            DataTypePtr equals_return_type;
            if (needs_nullable)
                equals_return_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
            else
                equals_return_type = std::make_shared<DataTypeUInt8>();

            ColumnsWithTypeAndName multi_if_args;

            // Convert CASE expression into multiIf(expr = when1, then1, expr = when2, then2, ..., else)
            for (size_t i = 1; i < args.size() - 1; i += 2)
            {
                // Add condition: expr_column = when_column
                auto equals_func = FunctionFactory::instance().get("equals", context);
                ColumnsWithTypeAndName equals_args{args.front(), args[i]};
                auto condition = equals_func->build(equals_args)
                    ->execute(equals_args, equals_return_type, input_rows_count, false);

                multi_if_args.push_back({condition, equals_return_type, ""});
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
