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
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} must have numeric type", getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
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

        const IColumn & col_length = *arguments[0].column;
        size_t total_codepoints = 0;
        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t codepoints = col_length.getUInt(row_num);
            total_codepoints += codepoints;
        }

        /* As we generate only assigned planes, the mathematical expectation of the number of bytes
         * per generated code point ~= 3.85. So, reserving for coefficient 4 will not be an overhead
         */

        if (total_codepoints > (1 << 29))
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size in function {}", getName());

        size_t max_byte_size = total_codepoints * 4 + input_rows_count;
        data_to.resize(max_byte_size);

        const auto generate_code_point = [](UInt32 rand)
        {
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

        pcg64_fast rng(randomSeed());
        IColumn::Offset offset = 0;

        for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        {
            size_t codepoints = col_length.getUInt(row_num);
            auto * pos = data_to.data() + offset;

            for (size_t i = 0; i < codepoints; i +=2)
            {
                UInt64 rand = rng(); /// that's the bottleneck

                UInt32 code_point1 = generate_code_point(static_cast<UInt32>(rand));

                size_t bytes1 = UTF8::convertCodePointToUTF8(code_point1, reinterpret_cast<char *>(pos), 4);
                chassert(bytes1 <= 4);
                pos += bytes1;

                if (i + 1 != codepoints)
                {
                    UInt32 code_point2 = generate_code_point(static_cast<UInt32>(rand >> 32u));
                    size_t bytes2 = UTF8::convertCodePointToUTF8(code_point2, reinterpret_cast<char *>(pos), 4);
                    chassert(bytes2 <= 4);
                    pos += bytes2;
                }
            }

            *pos = 0;
            ++pos;

            offset = pos - data_to.data();
            offsets_to[row_num] = offset;
        }

        data_to.resize(offset);

        return col_to;
    }
};

}

REGISTER_FUNCTION(RandomStringUTF8)
{
    factory.registerFunction<FunctionRandomStringUTF8>();
}
}
