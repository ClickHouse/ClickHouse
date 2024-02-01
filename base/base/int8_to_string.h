#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <fmt/format.h>

template <>
struct fmt::formatter<Int8>
{
    constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const Int8 & value, FormatContext & ctx) -> decltype(ctx.out())
    {
        return format<FormatContext>(int8_t{value}, ctx);
    }
};


namespace std
{
std::string to_string(Int8 v);
}
