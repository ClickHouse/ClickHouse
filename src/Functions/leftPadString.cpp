#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>

#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    struct LeftPadStringImpl
    {
        static void vector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            const size_t length,
            const String & padstr,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets)
        {
            size_t size = offsets.size();
            res_data.resize((length + 1 /* zero terminator */) * size);
            res_offsets.resize(size);

            const size_t padstr_size = padstr.size();

            ColumnString::Offset prev_offset = 0;
            ColumnString::Offset res_prev_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                size_t data_length = offsets[i] - prev_offset - 1 /* zero terminator */;
                if (data_length < length)
                {
                    for (size_t j = 0; j < length - data_length; ++j)
                        res_data[res_prev_offset + j] = padstr[j % padstr_size];
                    memcpy(&res_data[res_prev_offset + length - data_length], &data[prev_offset], data_length);
                }
                else
                {
                    for (size_t j = 0; j < length; ++j)
                        res_data[res_prev_offset + j] = data[prev_offset + j];
                }
                res_data[res_prev_offset + length] = 0;
                res_prev_offset += length + 1;
                res_offsets[i] = res_prev_offset;
            }
        }

        static void vectorFixed(
            const ColumnFixedString::Chars & data,
            const size_t n,
            const size_t length,
            const String & padstr,
            ColumnFixedString::Chars & res_data)
        {
            const size_t padstr_size = padstr.size();
            const size_t size = data.size() / n;
            res_data.resize(length * size);
            for (size_t i = 0; i < size; ++i)
            {
                if (length < n)
                {
                    memcpy(&res_data[i * length], &data[i * n], length);
                }
                else
                {
                    for (size_t j = 0; j < length - n; ++j)
                        res_data[i * length + j] = padstr[j % padstr_size];
                    memcpy(&res_data[i * length + length - n], &data[i * n], n);
                }
            }
        }
    };

    class FunctionLeftPadString : public IFunction
    {
    public:
        static constexpr auto name = "leftPadString";
        static FunctionPtr create(const ContextPtr) { return std::make_shared<FunctionLeftPadString>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }

        bool useDefaultImplementationForConstants() const override { return true; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            size_t number_of_arguments = arguments.size();

            if (number_of_arguments != 2 && number_of_arguments != 3)
                throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(number_of_arguments)
                        + ", should be 2 or 3",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (!isStringOrFixedString(arguments[0]))
                throw Exception(
                    "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (!isNativeNumber(arguments[1]))
                throw Exception(
                    "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if (number_of_arguments == 3 && !isStringOrFixedString(arguments[2]))
                throw Exception(
                    "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            return arguments[0];
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
        {
            const ColumnConst * len_column = checkAndGetColumnConst<ColumnUInt32>(arguments[1].column.get());
            if (!len_column)
                throw Exception(
                    "Illegal column " + arguments[1].column->getName() + " of first ('len') argument of function " + getName()
                        + ". Must be a number.", // FIXME
                    ErrorCodes::ILLEGAL_COLUMN);
            size_t len = len_column->getValue<size_t>();

            String padstr = " ";
            if (arguments.size() == 3)
            {
                const ColumnConst * pad_column = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
                if (!pad_column)
                    throw Exception(
                        "Illegal column " + arguments[2].column->getName() + " of second ('pad') argument of function " + getName()
                            + ". Must be constant string.",
                        ErrorCodes::ILLEGAL_COLUMN);

                padstr = pad_column->getValue<String>();
            }

            const ColumnPtr str_column = arguments[0].column;
            if (const ColumnString * strings = checkAndGetColumn<ColumnString>(str_column.get()))
            {
                auto col_res = ColumnString::create();
                LeftPadStringImpl::vector(
                    strings->getChars(), strings->getOffsets(), len, padstr, col_res->getChars(), col_res->getOffsets());
                return col_res;
            }
            else if (const ColumnFixedString * strings_fixed = checkAndGetColumn<ColumnFixedString>(str_column.get()))
            {
                auto col_res = ColumnFixedString::create(strings_fixed->getN());
                LeftPadStringImpl::vectorFixed(strings_fixed->getChars(), strings_fixed->getN(), len, padstr, col_res->getChars());
                return col_res;
            }
            else
                throw Exception(
                    "Illegal column " + arguments[0].column->getName() + " of first ('str') argument of function " + getName()
                        + ". Must be a string or fixed string.",
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    };
}

void registerFunctionLeftPadString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeftPadString>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("lpad", "leftPadString", FunctionFactory::CaseInsensitive);
}

}
