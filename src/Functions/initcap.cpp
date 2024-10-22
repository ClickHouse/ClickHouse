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
        size_t /*input_rows_count*/)
    {
        if (data.empty())
            return;
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t /*n*/, ColumnString::Chars & res_data, size_t)
    {
        res_data.resize(data.size());
        array(data.data(), data.data() + data.size(), res_data.data());
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
                if (isAlphaASCII(c))
                    *dst = toUpperIfAlphaASCII(c);
                else
                    *dst = c;
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
