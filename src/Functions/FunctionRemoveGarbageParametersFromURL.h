#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <base/StringRef.h>


namespace DB
{
/** For removeGarbageParametersFromURL function
  *
  * removeGarbageParametersFromURL(URL, length)
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


template <typename Impl, typename Name>
class FunctionRemoveGarbageParametersFromURL : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRemoveGarbageParametersFromURL>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"string", &isString<IDataType>, nullptr, "String"},
            {"uint", &isUnsignedInteger<IDataType>, nullptr, "Unsigned integer"}
        };

        validateFunctionArgumentTypes(*this, arguments, args);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr URL = arguments[0].column;
        const ColumnPtr max_len = arguments[1].column;
        const ColumnConst * max_len_const = typeid_cast<const ColumnConst *>(&*max_len);
        if (!max_len_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function {} must be constant unsigned integer", getName());

        uint64_t max_len_value = max_len_const->getValue<UInt64>();
        if (const ColumnString * URL_column = checkAndGetColumn<ColumnString>(URL.get()))
        {
            auto col_res = ColumnString::create();

            ColumnString::Chars & URL_res = col_res->getChars();
            ColumnString::Offsets & offsets_res = col_res->getOffsets();
            Impl::vector(URL_column->getChars(), URL_column->getOffsets(), max_len_value, URL_res, offsets_res);

            return col_res;
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());
    }
};

}
