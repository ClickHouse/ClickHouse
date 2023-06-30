#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

namespace DB
{
namespace
{

struct InitcapImpl
{
    static void vector(const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        res_data.resize(data.size());
        res_offsets.assign(offsets);
        array(data.data(), data.data() + data.size(), res_data.data());
    }

    static void vectorFixed(const ColumnString::Chars & data, size_t /*n*/, ColumnString::Chars & res_data)
    {
        res_data.resize(data.size());
        array(data.data(), data.data() + data.size(), res_data.data());
    }

private:
    static void array(const UInt8 * src, const UInt8 * src_end, UInt8 * dst)
    {
        const auto flip_case_mask = 'A' ^ 'a';

        auto is_lower_alpha = [](UInt8 c) { return c >= 'a' && c <= 'z'; };
        auto is_upper_alpha = [](UInt8 c) { return c >= 'A' && c <= 'Z'; };
        //auto is_digit = [](UInt8 c) { return c >= '0' && c <= '9'; };

        bool prev_is_alpha = false;

        for (; src < src_end; ++src, ++dst)
        {
            bool lower = is_lower_alpha(*src);
            bool is_alpha = lower || is_upper_alpha(*src);
            if (!is_alpha)
            {
                *dst = *src;
            }
            else if (!prev_is_alpha)
            {
                if (lower)
                    *dst = *src ^ flip_case_mask;
                else
                    *dst = *src;
            }
            else
            {
                if (!lower)
                    *dst = *src ^ flip_case_mask;
                else
                    *dst = *src;
            }
            prev_is_alpha = is_alpha;
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
    factory.registerFunction<FunctionInitcap>({}, FunctionFactory::CaseInsensitive);
}

}
