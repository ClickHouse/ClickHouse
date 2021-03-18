#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int TOO_LARGE_STRING_SIZE;
}

template <typename Impl, typename Name>
class FunctionsTextClassification : public IFunction
{
public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionsTextClassification>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        using ResultType = typename Impl::ResultType;

        const ColumnPtr & column = arguments[0].column;

        const ColumnConst * col_const = typeid_cast<const ColumnConst *>(&*column);

        if (col_const)
        {
            ResultType res;
            Impl::constant(col_const->getValue<String>(), res);
            return result_type->createColumnConst(col_const->size(), toField(res));
        }


        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            ColumnString::Chars & vec_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            Impl::vector(col->getChars(), col->getOffsets(), vec_res, offsets_res);
            return col_res;
        }
        else
        {
            throw Exception(
                "Illegal columns " + arguments[0].column->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
    }
};

}
