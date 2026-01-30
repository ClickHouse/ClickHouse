#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
            executeNaturalSortKey(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), input_rows_count);
            return col_res;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
private:
    static void executeNaturalSortKey(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (input_rows_count == 0)
            return;

        res_data.reserve(data.size());
        res_data.resize_exact(data.size());
        res_offsets.resize(input_rows_count);

        const UInt8 * data_curr = data.data();

        size_t data_row_curr = 0;
        const UInt8 *  data_curr_row_end = data.data() + offsets[0];

        const UInt8 * const data_end = data_curr + data.size();

        UInt8 * res_curr = res_data.data();

        const UInt8 * digits_start = data_curr;
        size_t digits = 0;
        UInt8 prev_byte = 0;

        while (data_curr < data_end)
        {
            const UInt8 b = *data_curr;
            const bool is_digit = '0' <= b && b <= '9';
            bool is_curr_row_end = data_curr == data_curr_row_end;

            if ((!is_digit || is_curr_row_end) && (digits || prev_byte == '0'))
            {
                if (!digits)
                    digits = 1;

                res_curr = appendNumToRes(digits_start, digits, res_data, res_curr);
                digits = 0;
            }

            if (is_curr_row_end)
            {
                // Current byte is in the new row.
                // We need to "close" the previous row and "close" all empty/null rows if any.
                do
                {
                    res_offsets[data_row_curr] = res_curr - res_data.data();
                    data_curr_row_end = data.data() + offsets[++data_row_curr];
                    is_curr_row_end = data_curr == data_curr_row_end;
                } while (is_curr_row_end);
            }

            if (!is_digit)
                *res_curr++ = b;
            else if (digits)
                ++digits;
            else
            {
                digits_start = data_curr;
                if (b != '0')
                    digits = 1;
            }

            prev_byte = b;
            ++data_curr;
        }

        // Handle last row if it doesn't end non-digit.
        if (digits || prev_byte == '0')
        {
            if (!digits)
                digits = 1;
            res_curr = appendNumToRes(digits_start, digits, res_data, res_curr);
        }

        const size_t final_res_size = res_curr - res_data.data();

        // Fill offsets for current (possible last) row and all remaining empty rows.
        while (data_row_curr < input_rows_count)
        {
            res_offsets[data_row_curr] = final_res_size;
            ++data_row_curr;
        }

        res_data.resize_exact(final_res_size);
    }

    /**
     * Append the encoded number (with its length prefix) to the response.
     * Returns the updated res_curr pointer.
     */
    static UInt8 * appendNumToRes(const UInt8 * num_start, const size_t digits, ColumnString::Chars & res_data, UInt8 * res_curr)
    {
        res_curr = appendDigitsPartToRes(digits, res_data, res_curr);

        memcpy(res_curr, num_start, digits);
        res_curr += digits;

        return res_curr;
    }

    /**
     * Encode the length of the digit sequence (max supporter len is 79).
     * Returns the updated res_curr pointer.
     */
    static UInt8 * appendDigitsPartToRes(const size_t digits, ColumnString::Chars & res_data, UInt8 * res_curr)
    {
        const size_t enc_val = digits - 1;

        constexpr UInt8 max_byte_code = '~'; // Highest printable ASCII character.
        constexpr UInt8 min_byte_code = '0'; // Lowest printable ASCII character used for encoding.
        constexpr size_t max_enc_size = max_byte_code - min_byte_code + 1; // Max number of digits that can be encoded in one byte.

        if (enc_val < max_enc_size)
        {
            res_curr = ensureGrowth(res_data, res_curr, 1);
            *res_curr++ = '0' + enc_val;
        }

        return res_curr;
    }

    /**
     * Ensure that res_data has enough capacity to accommodate needed_size more bytes.
     * If not, it reserves additional space and returns the updated res_curr pointer.
     * Returns the (possibly updated) res_curr pointer.
     */
    static UInt8 * ensureGrowth(ColumnString::Chars & res_data, UInt8 * res_curr, int needed_size)
    {
        const size_t new_size = res_data.size() + needed_size;
        if (likely(new_size <= res_data.capacity()))
        {
            res_data.resize_exact(new_size);
            return res_curr;
        }

        const size_t res_curr_pos = res_curr - res_data.data();

        res_data.reserve(new_size);
        res_data.resize_exact(new_size);

        return res_data.data() + res_curr_pos;
    }
};

REGISTER_FUNCTION(NaturalSortKey)
{
    FunctionDocumentation::Description description = R"(
The function is used for natural sorting.
)";
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
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNaturalSortKey>(documentation);
}

}
