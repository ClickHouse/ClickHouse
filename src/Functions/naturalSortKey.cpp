#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_LARGE_STRING_SIZE;
}

class FunctionNaturalSortKey : public IFunction
{
public:
    static constexpr auto name = "naturalSortKey";

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionNaturalSortKey>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        return arguments[0];
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & /*result_type*/,
        size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            executeNaturalSortKey(col, col_res->getChars(), col_res->getOffsets(), input_rows_count);
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
private:
    static void executeNaturalSortKey(
        const ColumnString * src,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (input_rows_count == 0)
            return;

        res_data.reserve(src->getChars().size());
        res_offsets.reserve_exact(input_rows_count);

        for (size_t row = 0; row != input_rows_count; ++row)
        {
            encodeString(src->getDataAt(row), res_data);
            res_offsets.push_back(res_data.size());
        }
    }

    /**
     * Encode the input string by replacing sequences of digits with a length prefix followed by the digits themselves.
     * Leading zeros are trimmed.
     */
    static void encodeString(const std::string_view str, ColumnString::Chars & res_data)
    {
        if (str.empty())
            return;

        const UInt8 * data_curr = reinterpret_cast<const UInt8 *>(str.data());
        const UInt8 * data_end = data_curr + str.size();

        const UInt8 * data_digits_start = data_curr;
        const UInt8 * data_non_digits_start = data_curr;

        size_t digits = 0; // Length of the current sequence of digits (not counting leading zeros).
        bool leading_zero = false;

        for (; data_curr != data_end; ++data_curr)
        {
            const UInt8 c = *data_curr;
            const bool is_digit = '0' <= c && c <= '9';

            if (!is_digit)
            {
                if (digits) // End of a sequence of digits.
                {
                    appendNumberToResult(data_digits_start, digits, res_data);
                    digits = 0;
                    leading_zero = false;
                    data_non_digits_start = data_curr;
                }
                else if (leading_zero) // End of a sequence of zeros.
                {
                    appendNumberToResult(data_curr - 1, 1, res_data);
                    data_non_digits_start = data_curr;
                    leading_zero = false;
                }
            }
            else
            {
                if (digits) // Continuation of a sequence of digits.
                    ++digits;
                else // Start of a new sequence of digits or zeros.
                {
                    if (!leading_zero) // End of a sequence of non-digit characters.
                        res_data.insert(data_non_digits_start, data_curr);

                    if (c != '0') // Start of a new sequence of digits (non-zero).
                    {
                        digits = 1;
                        data_digits_start = data_curr;
                    }
                    else
                        leading_zero = true; // Start or continuation of a sequence of zeros.
                }
            }
        }

        if (digits) // String ends with a sequence of digits.
            appendNumberToResult(data_digits_start, digits, res_data);
        else if (leading_zero) // String ends with a sequence of zeros.
            appendNumberToResult(data_curr - 1, 1, res_data);
        else // String ends with non-digit characters.
            res_data.insert(data_non_digits_start, data_curr);
    }

    /**
     * Append the encoded number (with its length prefix) to the response.
     */
    static void appendNumberToResult(const UInt8 * num_start, const size_t digits, ColumnString::Chars & res_data)
    {
        appendNumberPrefixToResult(digits, res_data);
        res_data.insert(num_start, num_start + digits);
    }

    /**
     * Encode the length of the digit sequence.
     */
    static void appendNumberPrefixToResult(const size_t digits, ColumnString::Chars & res_data)
    {
        chassert(digits != 0);

        auto p = digits - 1;

        if (likely(p < 8))
        {
            // 1 byte encoding: '0' ... '7' for 1..8 digits.
            res_data.push_back(static_cast<UInt8>('0' + p));
        }
        else if (p < 20)
        {
            // 2 bytes encoding: '80' ... '91' for 9..20 digits.
            p += 72;

            res_data.push_back(static_cast<UInt8>('0' + p / 10));
            res_data.push_back(static_cast<UInt8>('0' + p % 10));
        }
        else if (likely(p < 99))
        {
            // 3 bytes encoding: '920' ... '998' for 21..99 digits.
            p += 900;

            res_data.push_back(static_cast<UInt8>('0' + p / 100));
            res_data.push_back(static_cast<UInt8>('0' + (p / 10) % 10));
            res_data.push_back(static_cast<UInt8>('0' + p % 10));
        }
        else
        {
            // Encode as '999' + length(to_string(digits)) + to_string(digits)

            size_t digits_len = 3; // Length is at least 3 for digits >= 100
            size_t digits_power10 = 100;
            for (auto t = digits / 1000; t > 0; t /= 10)
            {
                ++digits_len;
                digits_power10 *= 10;
            }

            if (digits_len > 9)
                throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too long sequence of digits to encode for natural sorting, current length is {}, maximum length is 999999999", digits);

            res_data.push_back(UInt8{'9'});
            res_data.push_back(UInt8{'9'});
            res_data.push_back(UInt8{'9'});

            res_data.push_back(static_cast<UInt8>('0' + digits_len % 10)); // Encode 'length(to_string(digits))' as a single digit

            while (digits_len-- > 0)
            {
                res_data.push_back(static_cast<UInt8>('0' + (digits / digits_power10) % 10));
                digits_power10 /= 10;
            }
        }
    }
};

REGISTER_FUNCTION(NaturalSortKey)
{
    FunctionDocumentation::Description description = "The function is used for natural sorting.";
    FunctionDocumentation::Syntax syntax = "naturalSortKey(s)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "A string to convert to natural sort key.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a natural sort key string from `s`.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT s FROM t ORDER BY naturalSortKey(s)",
        R"(
┌─s───┐
│ a1  │
| a02 │
└─────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNaturalSortKey>(documentation);

    factory.registerAlias("NATURAL_SORT_KEY", "naturalSortKey", FunctionFactory::Case::Insensitive);
}

}
