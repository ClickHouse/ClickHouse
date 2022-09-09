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
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const DataTypes & args) const override
    {
        if (args.empty())
            throw Exception{"Function " + getName() + " expects at least 1 arguments",
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};

        /// See the comments in executeImpl() to understand why we actually have to
        /// get the return type of a transform function.

        /// Get the types of the arrays that we pass to the transform function.
        DataTypes dst_array_types;

        for (size_t i = 2; i < args.size() - 1; i += 2)
            dst_array_types.push_back(args[i]);

        return getLeastSupertype(dst_array_types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (args.empty())
            throw Exception{"Function " + getName() + " expects at least 1 argument",
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};

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

        src_array_col.column = fun_array->build(src_array_elems)->execute(src_array_elems, src_array_type, input_rows_count);
        dst_array_col.column = fun_array->build(dst_array_elems)->execute(dst_array_elems, dst_array_type, input_rows_count);

        /// Execute transform.
        ColumnsWithTypeAndName transform_args{args.front(), src_array_col, dst_array_col, args.back()};
        return FunctionFactory::instance().get("transform", context)->build(transform_args)
            ->execute(transform_args, result_type, input_rows_count);
    }

private:
    ContextPtr context;
};

}

void registerFunctionCaseWithExpression(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCaseWithExpression>();

    /// These are obsolete function names.
    factory.registerFunction<FunctionCaseWithExpression>("caseWithExpr");
}

}


