#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>

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
    extern const int BAD_ARGUMENTS;
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
                    memcpy(&res_data[res_prev_offset], &data[prev_offset], length);
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
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {}, should be 2 or 3",
                    getName(),
                    toString(number_of_arguments));

            if (!isStringOrFixedString(arguments[0]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

            if (!isNativeNumber(arguments[1]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of second argument of function {}",
                    arguments[1]->getName(),
                    getName());

            if (number_of_arguments == 3 && !isStringOrFixedString(arguments[2]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of third argument of function {}",
                    arguments[2]->getName(),
                    getName());

            return arguments[0];
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
        {
            const ColumnPtr str_column = arguments[0].column;
            String padstr = " ";
            if (arguments.size() == 3)
            {
                const ColumnConst * pad_column = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
                if (!pad_column)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Illegal column {} of third ('pad') argument of function {}. Must be constant string.",
                        arguments[2].column->getName(),
                        getName());

                padstr = pad_column->getValue<String>();
            }

            const ColumnConst * len_column = checkAndGetColumnConst<ColumnConst>(arguments[1].column.get());
            if (!len_column)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of second ('len') argument of function {}. Must be a positive integer.",
                    arguments[1].column->getName(),
                    getName());
            Int64 len = len_column->getInt(0);
            if (len <= 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Illegal value {} of second ('len') argument of function {}. Must be a positive integer.",
                    arguments[1].column->getName(),
                    getName());

            if (const ColumnString * strings = checkAndGetColumn<ColumnString>(str_column.get()))
            {
                auto col_res = ColumnString::create();
                LeftPadStringImpl::vector(
                    strings->getChars(), strings->getOffsets(), len, padstr, col_res->getChars(), col_res->getOffsets());
                return col_res;
            }
            else if (const ColumnFixedString * strings_fixed = checkAndGetColumn<ColumnFixedString>(str_column.get()))
            {
                auto col_res = ColumnFixedString::create(len);
                LeftPadStringImpl::vectorFixed(strings_fixed->getChars(), strings_fixed->getN(), len, padstr, col_res->getChars());
                return col_res;
            }
            else
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column {} of first ('str') argument of function {}. Must be a string or fixed string.",
                    arguments[0].column->getName(),
                    getName());
            }
        }
    };
}

void registerFunctionLeftPadString(FunctionFactory & factory)
{
    factory.registerFunction<FunctionLeftPadString>(FunctionFactory::CaseInsensitive);
    factory.registerAlias("lpad", "leftPadString", FunctionFactory::CaseInsensitive);
}

}
