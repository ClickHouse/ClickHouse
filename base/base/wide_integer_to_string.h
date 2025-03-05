#pragma once

import fmt;
#include <string>

namespace wide
{
template <size_t Bits, typename Signed>
class integer;
}

using Int128 = wide::integer<128, signed>;
using UInt128 = wide::integer<128, unsigned>;
using Int256 = wide::integer<256, signed>;
using UInt256 = wide::integer<256, unsigned>;

namespace wide
{

template <size_t Bits, typename Signed>
std::string to_string(const integer<Bits, Signed> & n);

extern template std::string to_string(const Int128 & n);
extern template std::string to_string(const UInt128 & n);
extern template std::string to_string(const Int256 & n);
extern template std::string to_string(const UInt256 & n);
}

template <size_t Bits, typename Signed>
std::ostream & operator<<(std::ostream & out, const wide::integer<Bits, Signed> & value);

extern std::ostream & operator<<(std::ostream & out, const Int128 & value);
extern std::ostream & operator<<(std::ostream & out, const UInt128 & value);
extern std::ostream & operator<<(std::ostream & out, const Int256 & value);
extern std::ostream & operator<<(std::ostream & out, const UInt256 & value);

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <size_t Bits, typename Signed>
struct fmt::formatter<wide::integer<Bits, Signed>>
{
    constexpr auto parse(format_parse_context& ctx) -> format_parse_context::iterator;
    auto format(const wide::integer<Bits, Signed> & value, format_context & ctx) const -> format_context::iterator;
};

extern template struct fmt::formatter<Int128>;
extern template struct fmt::formatter<UInt128>;
extern template struct fmt::formatter<Int256>;
extern template struct fmt::formatter<UInt256>;
