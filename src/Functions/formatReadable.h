#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


/** formatReadableSize - prints the transferred size in bytes in form `123.45 GiB`.
  * formatReadableQuantity - prints the quantity in form of 123 million.
  */

class FunctionFormatReadable : public IFunction
{
public:
    static constexpr UInt8 DEFAULT_PRECISION = 2;

    using FormatFunc = void (*)(double, WriteBuffer &, int);

    FunctionFormatReadable(const char * name_, FormatFunc format_func_)
        : function_name(name_), format_func(format_func_) {}

    static FunctionPtr create(const char * name, FormatFunc format_func)
    {
        return std::make_shared<FunctionFormatReadable>(name, format_func);
    }

    String getName() const override
    {
        return function_name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_arguments{
            {"x", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
        };

        FunctionArgumentDescriptors optional_arguments{
            {"precision", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), &isColumnConst, "const UInt8"},
        };

        validateFunctionArguments(getName(), arguments, mandatory_arguments, optional_arguments);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        UInt8 precision = DEFAULT_PRECISION;
        /// Optional precision argument is present
        if (arguments.size() == 2)
        {
            const auto * col_precision = checkAndGetColumnConst<ColumnUInt8>(arguments[1].column.get());
            if (!col_precision)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of second argument of function {}",
                    arguments[1].column->getName(), getName());
            precision = col_precision->getValue<UInt8>();
        }

        auto col_to = ColumnString::create();

        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        data_to.resize(input_rows_count * 2);
        offsets_to.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// The cost of the virtual call for getFloat64 is negligible compared with the format calls
            format_func(arguments[0].column->getFloat64(i), buf_to, precision);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
        return col_to;
    }

private:
    const char * function_name;
    FormatFunc format_func;
};

}
