#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionDateOrDateTime.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>
#include "Columns/IColumn.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** formatReadableSize - prints the transferred size in bytes in form `123.45 GiB`.
  * formatReadableQuantity - prints the quantity in form of 123 million.
  */

template <typename Impl>
class FunctionFormatReadable : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatReadable<Impl>>(); }

    String getName() const override
    {
        return name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t number_of_arguments = arguments.size();
        const IDataType & type = *arguments[0];

        if (number_of_arguments != 1 && number_of_arguments != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1 or 2",
                getName(),
                number_of_arguments);

        if (!isNativeNumber(type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot format {} because it's not a native numeric type", type.getName());

        if (number_of_arguments == 2)
        {
            const IDataType & precision = *arguments[1];
            if (!isNativeNumber(precision))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot format {} because it's not a native numeric type", precision.getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        ColumnPtr res;
        if (!((res = executeType<UInt8>(arguments))
            || (res = executeType<UInt16>(arguments))
            || (res = executeType<UInt32>(arguments))
            || (res = executeType<UInt64>(arguments))
            || (res = executeType<Int8>(arguments))
            || (res = executeType<Int16>(arguments))
            || (res = executeType<Int32>(arguments))
            || (res = executeType<Int64>(arguments))
            || (res = executeType<Float32>(arguments))
            || (res = executeType<Float64>(arguments))))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                            arguments[0].column->getName(), getName());

        return res;
    }

private:
    template <typename T>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments) const
    {
        //Get precision constant
        int precision_val = 2;
        if (arguments.size() == 2)
        {
            const ColumnConst * precision_const = nullptr;
            precision_const = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get());
            if (!precision_const)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot format {} because it's not an UInt8 constant", getName());
            if (precision_const)
                precision_val = precision_const->getValue<UInt8>();
        }

        const ColumnConst * col_from_const = checkAndGetColumnConst<ColumnVector<T>>(arguments[0].column.get());
        const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get());
        ColumnWithTypeAndName new_arg;
        if (col_from_const)
        {
            new_arg = {col_from_const->convertToFullColumn(), arguments[0].type, arguments[0].name};
            col_from = checkAndGetColumn<ColumnVector<T>>(new_arg.column.get());
        }


        if (col_from)
        {
            auto col_to = ColumnString::create();
            ColumnString::Chars & data_to = col_to->getChars();
            ColumnString::Offsets & offsets_to = col_to->getOffsets();
            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            data_to.resize(size * 2);
            offsets_to.resize(size);

            WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);
            for (size_t i = 0; i < size; ++i)
            {
                if (arguments.size() == 1)
                    Impl::format(static_cast<double>(vec_from[i]), buf_to);
                else
                    Impl::format(static_cast<double>(vec_from[i]), buf_to, precision_val);
                writeChar(0, buf_to);
                offsets_to[i] = buf_to.count();
            }

            buf_to.finalize();
            return col_to;
        }
        return nullptr;
    }
};

}
