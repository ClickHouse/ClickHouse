#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int TOO_LARGE_STRING_SIZE;
}

class FunctionNaturalSortKey final : public IFunction
{
public:
    static constexpr auto name = "naturalSortKey";

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionNaturalSortKey>();
    }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }

    [[nodiscard]] bool isVariadic() const override { return true; }

    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(T : String, [const mode UInt]) -> T";
    }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    [[nodiscard]] ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & /*result_type*/,
        size_t input_rows_count) const override
    {
        const ColumnPtr column = arguments[0].column;
        Mode mode = Mode::Default;
        if (arguments.size() == 2)
        {
            const UInt64 mode_value = arguments[1].column->getUInt(0);
            switch (mode_value)
            {
                case 0: mode = Mode::Default; break;
                case 1: mode = Mode::TextAsChunks; break;
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Illegal value {} of the second argument of function {}, expected 0 or 1", mode_value, getName());
            }
        }
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            executeNaturalSortKey(col, col_res->getChars(), col_res->getOffsets(), input_rows_count, mode);
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
private:
    /// The order that the key gives to strings with non-digit characters.
    /// Numbers are compared by value in both modes.
    enum class Mode : UInt8
    {
        /// The behavior of the function without the second argument.
        /// The characters are compared one by one with the characters or the number at the same position
        /// of the other string. So `host-1` is before `host2`: `-` is compared with the number 2.
        Default = 0,
        /// A sequence of non-digit characters is compared as a whole chunk with the chunk at the same position
        /// of the other string, and a number is compared only if the chunks before it are equal.
        /// So `host2` is before `host-1`: `host` is compared with `host-`.
        /// This is the order of the PromQL function `sort_by_label`.
        TextAsChunks = 1,
    };

    /**
     * Writes the natural sort key of each row of `src` to the result column.
     * `res_data` and `res_offsets` are the characters and the offsets of the result column, and must be empty.
     * `mode` selects the order of the keys, see `Mode`.
     */
    static void executeNaturalSortKey(
        const ColumnString * src,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count,
        Mode mode)
    {
        if (input_rows_count == 0)
            return;

        res_data.reserve(src->getChars().size());
        res_offsets.reserve_exact(input_rows_count);

        for (size_t row = 0; row != input_rows_count; ++row)
        {
            encodeString(src->getDataAt(row), res_data, mode);
            res_offsets.push_back(res_data.size());
        }
    }

    /**
     * Encode the input string by replacing sequences of digits with a length prefix followed by the digits themselves.
     * Leading zeros are trimmed.
     * Sequences of non-digit characters are encoded according to `mode`, see `appendTextToResult`.
     */
    static void encodeString(const std::string_view str, ColumnString::Chars & res_data, Mode mode)
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
                        appendTextToResult(data_non_digits_start, data_curr, res_data, mode);

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
            appendTextToResult(data_non_digits_start, data_curr, res_data, mode);
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
     * Append a sequence of non-digit characters to the result.
     *
     * In `Mode::Default` the sequence is copied as is.
     *
     * In `Mode::TextAsChunks` the keys are still compared byte by byte, so the end of a chunk must be visible in the key:
     * - The sequence is followed by an end mark of two spaces. The mark is less than any character of a longer chunk,
     *   so a chunk sorts before every longer chunk that starts with it. Without the mark, the next byte would be
     *   the length prefix of the following number, which is greater than characters such as `-` and `.`.
     * - A character `c` with code 32 (space) or less is written as a space followed by `'!' + c`. So it cannot be taken for the end mark,
     *   it sorts after the end mark and before all other characters, and such characters keep their order.
     *
     * Examples:
     *   `host2`   ->  `host<space><space>02`
     *   `host-1`  ->  `host-<space><space>01`
     *   `host 3`  ->  `host<space>A<space><space>03`
     * They sort as `host2`, `host 3`, `host-1`.
     */
    static void appendTextToResult(const UInt8 * text_start, const UInt8 * text_end, ColumnString::Chars & res_data, Mode mode)
    {
        if (text_start == text_end)
            return;

        switch (mode)
        {
            case Mode::Default:
                res_data.insert(text_start, text_end);
                break;
            case Mode::TextAsChunks:
            {
                static constexpr UInt8 end_mark[] = {' ', ' '};
                /// Characters with code 32 (space) or less; each of them takes one more byte in the key.
                const size_t chars_to_escape = static_cast<size_t>(std::count_if(text_start, text_end, [](UInt8 c) { return c <= ' '; }));
                const size_t init_size = res_data.size();
                const size_t bytes_to_write = static_cast<size_t>(text_end - text_start) + chars_to_escape + std::size(end_mark);

                if (chars_to_escape == 0)
                {
                    res_data.reserve(init_size + bytes_to_write);
                    res_data.insert_assume_reserved(text_start, text_end);
                    res_data.insert_assume_reserved(std::begin(end_mark), std::end(end_mark));
                }
                else
                {
                    /// The pointer is taken after `resize`, which can move the data.
                    res_data.resize(init_size + bytes_to_write);
                    UInt8 * out = res_data.data() + init_size;
                    for (const UInt8 * p = text_start; p != text_end; ++p)
                    {
                        const UInt8 c = *p;
                        if (c > ' ')
                            *out++ = c;
                        else
                        {
                            /// A character with code 32 (space) or less would sort before the end mark or look like a part of it,
                            /// so it is written as two bytes that start with a space.
                            *out++ = ' ';
                            /// The second byte is from '!' to 'A': greater than the second space of the end mark,
                            /// in the order of the original characters, and printable.
                            *out++ = static_cast<UInt8>('!' + c);
                        }
                    }
                    for (const UInt8 c : end_mark)
                        *out++ = c;
                    chassert(out == res_data.end());
                }
                break;
            }
        }
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

            while (digits_len > 0)
            {
                --digits_len;
                res_data.push_back(static_cast<UInt8>('0' + (digits / digits_power10) % 10));
                digits_power10 /= 10;
            }
        }
    }
};

REGISTER_FUNCTION(NaturalSortKey)
{
    FunctionDocumentation::Description description = R"(
Returns a key for natural sorting: strings ordered by their keys are in natural order, in which a sequence of digits is compared as a number, so `a2` is before `a10`.

The optional argument `mode` selects how the characters that are not digits are compared:
- `0` (default): the characters are compared one by one with the characters or the number at the same position of the other string. So `host-1` is before `host2`, because `-` is compared with the number `2`.
- `1`: a sequence of non-digit characters is compared as a whole with the sequence at the same position of the other string, and a number is compared only if the sequences before it are equal. So `host2` is before `host-1`, because `host` is compared with `host-`. This is the order of the PromQL function `sort_by_label`.
)";
    FunctionDocumentation::Syntax syntax = "naturalSortKey(s[, mode])";
    FunctionDocumentation::Arguments arguments = {
        {"s", "A string to convert to natural sort key.", {"String"}},
        {"mode", "Optional. How non-digit characters are compared: `0` (default) or `1`.", {"const UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a natural sort key string from `s`.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE t (s String) ENGINE = Memory;
INSERT INTO t VALUES ('a1'), ('a02');

SELECT s FROM t ORDER BY naturalSortKey(s);
        )",
        R"(
┌─s───┐
│ a1  │
│ a02 │
└─────┘
        )"
    },
    {
        "The two modes",
        R"(
SELECT
    arraySort(s -> naturalSortKey(s), ['host-1', 'host10', 'host2']) AS default_mode,
    arraySort(s -> naturalSortKey(s, 1), ['host-1', 'host10', 'host2']) AS mode_1;
        )",
        R"(
┌─default_mode────────────────┬─mode_1──────────────────────┐
│ ['host-1','host2','host10'] │ ['host2','host10','host-1'] │
└─────────────────────────────┴─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::String;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNaturalSortKey>(documentation);

    factory.registerAlias("NATURAL_SORT_KEY", "naturalSortKey", FunctionFactory::Case::Insensitive);
}

}
