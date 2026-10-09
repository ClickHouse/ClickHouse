#include <base/wide_integer.h>
#include <base/wide_integer_to_string.h>

#include <string>
#include <fmt/format.h>


namespace wide
{

template <size_t Bits, typename Signed>
inline std::string to_string(const integer<Bits, Signed> & n)
{
    std::string res;
    if (integer<Bits, Signed>::_impl::operator_eq(n, 0U))
        return "0";

    integer<Bits, unsigned> t;
    bool is_neg = integer<Bits, Signed>::_impl::is_negative(n);
    if (is_neg)
        t = integer<Bits, Signed>::_impl::operator_unary_minus(n);
    else
        t = n;

    while (!integer<Bits, unsigned>::_impl::operator_eq(t, 0U))
    {
        res.insert(res.begin(), '0' + char(integer<Bits, unsigned>::_impl::operator_percent(t, 10U)));
        t = integer<Bits, unsigned>::_impl::operator_slash(t, 10U);
    }

    if (is_neg)
        res.insert(res.begin(), '-');
    return res;
}

template std::string to_string(const integer<128, signed> & n);
template std::string to_string(const integer<128, unsigned> & n);
template std::string to_string(const integer<256, signed> & n);
template std::string to_string(const integer<256, unsigned> & n);

}

template <size_t Bits, typename Signed>
std::ostream & operator<<(std::ostream & out, const wide::integer<Bits, Signed> & value)
{
    return out << to_string(value);
}

std::ostream & operator<<(std::ostream & out, const wide::integer<128, signed> & value);
std::ostream & operator<<(std::ostream & out, const wide::integer<128, unsigned> & value);
std::ostream & operator<<(std::ostream & out, const wide::integer<256, signed> & value);
std::ostream & operator<<(std::ostream & out, const wide::integer<256, unsigned> & value);

template struct fmt::formatter<wide::integer<128, signed>>;
template struct fmt::formatter<wide::integer<128, unsigned>>;
template struct fmt::formatter<wide::integer<256, signed>>;
template struct fmt::formatter<wide::integer<256, unsigned>>;
