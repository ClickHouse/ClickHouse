#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <pcg_random.hpp>
#include <Common/UTF8Helpers.h>
#include <Common/randomSeed.h>

#include <base/defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace
{

/* Generate string with a UTF-8 encoded text.
 * Take a single argument - length of result string in Unicode code points.
 * ATTENTION: Method generate only assignable code points (excluded 4-13 planes).
 * See https://en.wikipedia.org/wiki/Plane_(Unicode) */

class FunctionRandomStringUTF8 : public IFunction
{
public:
    static constexpr auto name = "randomStringUTF8";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionRandomStringUTF8>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isNumber(*arguments[0]))
            throw Exception("First argument of function " + getName() + " must have numeric type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnString::create();
        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();

        if (input_rows_count == 0)
            return col_to;

        offsets_to.resize(input_rows_count);

        const IColumn & length_column = *arguments[0].column;
        size_t summary_utf8_len = 0;
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t utf8_len = length_column.getUInt(row_num);
            summary_utf8_len += utf8_len;
        }

        /* As we generate only assigned planes, the mathematical expectation of the number of bytes
         * per generated code point ~= 3.85. So, reserving for coefficient 4 will not be an overhead
         */

        if (summary_utf8_len > (1 << 29))
            throw Exception("Too large string size in function " + getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);

        size_t size_in_bytes_with_margin = summary_utf8_len * 4 + input_rows_count;
        data_to.resize(size_in_bytes_with_margin);
        pcg64_fast rng(randomSeed()); // TODO It is inefficient. We should use SIMD PRNG instead.

        const auto generate_code_point = [](UInt32 rand) -> UInt32 {
            /// We want to generate number in [0x0, 0x70000) and shift it if need

            /// Generate highest byte in [0, 6]
            /// https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
            UInt32 code_point = (rand >> 16) * 7u;
            code_point &= ~0xFFFF;
            code_point |= rand & 0xFFFF; // and other bytes obtaining in a simple way

            if (code_point >= 0x40000)
            {
                code_point += 0xa0000; // shift if it is in 14-16 plane
                return code_point;
            }

            if (0xD7FF < code_point && code_point < 0xE000) // this range will not be valid in isValidUTF8
            {
                /// The distribution will be slightly non-uniform but we don't care.
                return 0u;
            }

            return code_point;
        };

        IColumn::Offset offset = 0;
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t utf8_len = length_column.getUInt(row_num);
            auto * pos = data_to.data() + offset;

            size_t last_writen_bytes = 0;
            size_t i = 0;
            for (; i < utf8_len; i += 2)
            {
                UInt64 rand = rng();

                UInt32 code_point1 = generate_code_point(rand);
                UInt32 code_point2 = generate_code_point(rand >> 32);

                /// We have padding in column buffers that we can overwrite.
                size_t length1 = UTF8::convertCodePointToUTF8(code_point1, pos, sizeof(int));
                assert(length1 <= 4);
                pos += length1;

                size_t length2 = UTF8::convertCodePointToUTF8(code_point2, pos, sizeof(int));
                assert(length2 <= 4);
                last_writen_bytes = length2;
                pos += last_writen_bytes;
            }
            offset = pos - data_to.data() + 1;
            if (i > utf8_len)
            {
                offset -= last_writen_bytes;
            }
            offsets_to[row_num] = offset;
        }

        /// Put zero bytes in between.
        auto * pos = data_to.data();
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
            pos[offsets_to[row_num] - 1] = 0;

        return col_to;
    }
};

}

void registerFunctionRandomStringUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRandomStringUTF8>();
}
}
