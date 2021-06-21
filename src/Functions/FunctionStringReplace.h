#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename Impl, typename Name>
class FunctionStringReplace : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionStringReplace>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isStringOrFixedString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!isStringOrFixedString(arguments[2]))
            throw Exception(
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column_src = arguments[0].column;
        const ColumnPtr column_needle = arguments[1].column;
        const ColumnPtr column_replacement = arguments[2].column;

        if (!isColumnConst(*column_needle) || !isColumnConst(*column_replacement))
            throw Exception("2nd and 3rd arguments of function " + getName() + " must be constants.", ErrorCodes::ILLEGAL_COLUMN);

        const IColumn * c1 = arguments[1].column.get();
        const IColumn * c2 = arguments[2].column.get();
        const ColumnConst * c1_const = typeid_cast<const ColumnConst *>(c1);
        const ColumnConst * c2_const = typeid_cast<const ColumnConst *>(c2);
        String needle = c1_const->getValue<String>();
        String replacement = c2_const->getValue<String>();

        if (needle.empty())
            throw Exception("Length of the second argument of function replace must be greater than 0.", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(col->getChars(), col->getOffsets(), needle, replacement, col_res->getChars(), col_res->getOffsets());
            return col_res;
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column_src.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), needle, replacement, col_res->getChars(), col_res->getOffsets());
            return col_res;
        }
        else
            throw Exception(
                "Illegal column " + arguments[0].column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
