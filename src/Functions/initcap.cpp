#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Common/StringUtils.h>

namespace DB
{
namespace
{

struct InitcapImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (data.empty())
            return;
        res_data.resize(data.size());
        res_offsets.assign(offsets);

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset next_offset = offsets[i];
            array(&data[prev_offset], &data[next_offset], &res_data[prev_offset]);
            prev_offset = next_offset;
        }
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t n, ColumnString::Chars & res_data, size_t input_rows_count)
    {
        if (data.empty())
            return;
        res_data.resize(data.size());

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            ColumnString::Offset next_offset = prev_offset + n;
            array(&data[prev_offset], &data[next_offset], &res_data[prev_offset]);
            prev_offset = next_offset;
        }
    }

private:
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        bool prev_alphanum = false;

        for (; src < src_end; ++src, ++dst)
        {
            char c = *src;
            bool alphanum = isAlphaNumericASCII(c);
            if (alphanum && !prev_alphanum)
            {
                if (isAlphaASCII(c))
                    *dst = toUpperIfAlphaASCII(c);
                else
                    *dst = c;
            }
            else if (isAlphaASCII(c))
                *dst = toLowerIfAlphaASCII(c);
            else
                *dst = c;
            prev_alphanum = alphanum;
        }
    }
};

struct NameInitcap
{
    static constexpr auto name = "initcap";
};
using FunctionInitcap = FunctionStringToString<InitcapImpl, NameInitcap>;

}

REGISTER_FUNCTION(Initcap)
{
    factory.registerFunction<FunctionInitcap>({}, FunctionFactory::Case::Insensitive);
}

}
