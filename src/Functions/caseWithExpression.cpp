#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>


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

        bool all_when_values_constant = true;
        for (size_t i = 1; i < args.size() - 1; i += 2)
        {
            if (!isColumnConst(*args[i].column))
            {
                all_when_values_constant = false;
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
        /// Else we should use the straightforward implementation, checking values row-by-row
        if (all_when_values_constant)
        {
            ColumnsWithTypeAndName transform_args{args.front(), src_array_col, dst_array_col, args.back()};
            return FunctionFactory::instance().get("transform", context)->build(transform_args)
                ->execute(transform_args, result_type, input_rows_count, /* dry_run = */ false);
        }
        else
        {
            auto expr_column = args[0].column;
            auto else_column = args.back().column;
            auto result_column = result_type->createColumn();

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                bool matched = false;
                Field expr_value = (*expr_column)[row];

                for (size_t i = 1; i < args.size() - 1; i += 2)
                {
                    Field when_value = (*args[i].column)[row];
                    if (expr_value == when_value)
                    {
                        result_column->insert((*args[i + 1].column)[row]);
                        matched = true;
                        break;
                    }
                }

                if (!matched)
                    result_column->insert((*else_column)[row]);
            }

            return result_column;
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
