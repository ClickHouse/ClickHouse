#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <ext/map.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

/// Implements the CASE construction when it is
/// provided an expression. Users should not call this function.
class FunctionCaseWithExpression : public IFunction
{
public:
    static constexpr auto name = "caseWithExpression";
    static FunctionPtr create(const Context & context_) { return std::make_shared<FunctionCaseWithExpression>(context_); }

    explicit FunctionCaseWithExpression(const Context & context_) : context(context_) {}
    bool isVariadic() const override { return true; }
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

    void executeImpl(Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count) const override
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
        ColumnNumbers src_array_args;
        ColumnsWithTypeAndName src_array_elems;
        DataTypes src_array_types;

        ColumnNumbers dst_array_args;
        ColumnsWithTypeAndName dst_array_elems;
        DataTypes dst_array_types;

        for (size_t i = 1; i < (args.size() - 1); ++i)
        {
            if (i % 2)
            {
                src_array_args.push_back(args[i]);
                src_array_elems.push_back(block.getByPosition(args[i]));
                src_array_types.push_back(block.getByPosition(args[i]).type);
            }
            else
            {
                dst_array_args.push_back(args[i]);
                dst_array_elems.push_back(block.getByPosition(args[i]));
                dst_array_types.push_back(block.getByPosition(args[i]).type);
            }
        }

        DataTypePtr src_array_type = std::make_shared<DataTypeArray>(getLeastSupertype(src_array_types));
        DataTypePtr dst_array_type = std::make_shared<DataTypeArray>(getLeastSupertype(dst_array_types));

        Block temp_block = block;

        size_t src_array_pos = temp_block.columns();
        temp_block.insert({nullptr, src_array_type, ""});

        size_t dst_array_pos = temp_block.columns();
        temp_block.insert({nullptr, dst_array_type, ""});

        auto fun_array = FunctionFactory::instance().get("array", context);

        fun_array->build(src_array_elems)->execute(temp_block, src_array_args, src_array_pos, input_rows_count);
        fun_array->build(dst_array_elems)->execute(temp_block, dst_array_args, dst_array_pos, input_rows_count);

        /// Execute transform.
        ColumnNumbers transform_args{args.front(), src_array_pos, dst_array_pos, args.back()};
        FunctionFactory::instance().get("transform", context)->build(
            ext::map<ColumnsWithTypeAndName>(transform_args, [&](auto i){ return temp_block.getByPosition(i); }))
            ->execute(temp_block, transform_args, result, input_rows_count);

        /// Put the result into the original block.
        block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);
    }

private:
    const Context & context;
};

void registerFunctionCaseWithExpression(FunctionFactory & factory)
{
    factory.registerFunction<FunctionCaseWithExpression>();

    /// These are obsolete function names.
    factory.registerFunction<FunctionCaseWithExpression>("caseWithExpr");
}

}


