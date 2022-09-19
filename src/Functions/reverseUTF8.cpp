#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/** Reverse the sequence of code points in a UTF-8 encoded string.
  * The result may not match the expected result, because modifying code points (for example, diacritics) may be applied to another symbols.
  * If the string is not encoded in UTF-8, then the behavior is undefined.
  */
struct ReverseUTF8Impl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            ColumnString::Offset j = prev_offset;
            while (j < offsets[i] - 1)
            {
                if (data[j] < 0xBF)
                {
                    res_data[offsets[i] + prev_offset - 2 - j] = data[j];
                    j += 1;
                }
                else if (data[j] < 0xE0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 1], &data[j], 2);
                    j += 2;
                }
                else if (data[j] < 0xF0)
                {
                    memcpy(&res_data[offsets[i] + prev_offset - 2 - j - 2], &data[j], 3);
                    j += 3;
                }
                else
                {
                    res_data[offsets[i] + prev_offset - 2 - j] = data[j];
                    j += 1;
                }
            }

            res_data[offsets[i] - 1] = 0;
            prev_offset = offsets[i];
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception("Cannot apply function reverseUTF8 to fixed string.", ErrorCodes::ILLEGAL_COLUMN);
    }
};

struct NameReverseUTF8
{
    static constexpr auto name = "reverseUTF8";
};
using FunctionReverseUTF8 = FunctionStringToString<ReverseUTF8Impl, NameReverseUTF8, true>;

}

void registerFunctionReverseUTF8(FunctionFactory & factory)
{
    factory.registerFunction<FunctionReverseUTF8>();
}

}
