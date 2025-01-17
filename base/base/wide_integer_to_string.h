#pragma once

#include <base/wide_integer.h>

#include <string>
#include <fmt/format.h>

namespace wide
{

template <size_t Bits, typename Signed>
std::string to_string(const integer<Bits, Signed> & n);

extern template std::string to_string(const integer<128, signed> & n);
extern template std::string to_string(const integer<128, unsigned> & n);
extern template std::string to_string(const integer<256, signed> & n);
extern template std::string to_string(const integer<256, unsigned> & n);
}


template <size_t Bits, typename Signed>
std::ostream & operator<<(std::ostream & out, const wide::integer<Bits, Signed> & value);

extern std::ostream & operator<<(std::ostream & out, const wide::integer<128, signed> & value);
extern std::ostream & operator<<(std::ostream & out, const wide::integer<128, unsigned> & value);
extern std::ostream & operator<<(std::ostream & out, const wide::integer<256, signed> & value);
extern std::ostream & operator<<(std::ostream & out, const wide::integer<256, unsigned> & value);


/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <size_t Bits, typename Signed>
struct fmt::formatter<wide::integer<Bits, Signed>>
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
    auto format(const wide::integer<Bits, Signed> & value, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", to_string(value));
    }
};

extern template struct fmt::formatter<wide::integer<128, signed>>;
extern template struct fmt::formatter<wide::integer<128, unsigned>>;
extern template struct fmt::formatter<wide::integer<256, signed>>;
extern template struct fmt::formatter<wide::integer<256, unsigned>>;
