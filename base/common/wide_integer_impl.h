/// Original is here https://github.com/cerevra/int
#pragma once

#include "throwError.h"

#ifndef CHAR_BIT
#define CHAR_BIT 8
#endif

namespace wide
{

template <typename T>
struct IsWideInteger
{
    static const constexpr bool value = false;
};

template <size_t Bits, typename Signed>
struct IsWideInteger<wide::integer<Bits, Signed>>
{
    static const constexpr bool value = true;
};

template <typename T>
static constexpr bool ArithmeticConcept() noexcept
{
    return std::is_arithmetic_v<T> || IsWideInteger<T>::value;
}

template <typename T>
static constexpr bool IntegralConcept() noexcept
{
    return std::is_integral_v<T> || IsWideInteger<T>::value;
}

}

namespace std
{

// numeric limits
template <size_t Bits, typename Signed>
class numeric_limits<wide::integer<Bits, Signed>>
{
public:
    static constexpr bool is_specialized = true;
    static constexpr bool is_signed = is_same<Signed, signed>::value;
    static constexpr bool is_integer = true;
    static constexpr bool is_exact = true;
    static constexpr bool has_infinity = false;
    static constexpr bool has_quiet_NaN = false;
    static constexpr bool has_signaling_NaN = true;
    static constexpr std::float_denorm_style has_denorm = std::denorm_absent;
    static constexpr bool has_denorm_loss = false;
    static constexpr std::float_round_style round_style = std::round_toward_zero;
    static constexpr bool is_iec559 = false;
    static constexpr bool is_bounded = true;
    static constexpr bool is_modulo = true;
    static constexpr int digits = Bits - (is_same<Signed, signed>::value ? 1 : 0);
    static constexpr int digits10 = digits * 0.30103 /*std::log10(2)*/;
    static constexpr int max_digits10 = 0;
    static constexpr int radix = 2;
    static constexpr int min_exponent = 0;
    static constexpr int min_exponent10 = 0;
    static constexpr int max_exponent = 0;
    static constexpr int max_exponent10 = 0;
    static constexpr bool traps = true;
    static constexpr bool tinyness_before = false;

    static constexpr wide::integer<Bits, Signed> min() noexcept
    {
        if (is_same<Signed, signed>::value)
        {
            using T = wide::integer<Bits, signed>;
            T res{};
            res.m_arr[T::_impl::big(0)] = std::numeric_limits<typename wide::integer<Bits, Signed>::signed_base_type>::min();
            return res;
        }
        return 0;
    }

    static constexpr wide::integer<Bits, Signed> max() noexcept
    {
        using T = wide::integer<Bits, Signed>;
        T res{};
        res.m_arr[T::_impl::big(0)] = is_same<Signed, signed>::value
            ? std::numeric_limits<typename wide::integer<Bits, Signed>::signed_base_type>::max()
            : std::numeric_limits<typename wide::integer<Bits, Signed>::base_type>::max();
        for (int i = 1; i < wide::integer<Bits, Signed>::_impl::arr_size; ++i)
        {
            res.m_arr[T::_impl::big(i)] = std::numeric_limits<typename wide::integer<Bits, Signed>::base_type>::max();
        }
        return res;
    }

    static constexpr wide::integer<Bits, Signed> lowest() noexcept { return min(); }
    static constexpr wide::integer<Bits, Signed> epsilon() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> round_error() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> infinity() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> quiet_NaN() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> signaling_NaN() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> denorm_min() noexcept { return 0; }
};

// type traits
template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
struct common_type<wide::integer<Bits, Signed>, wide::integer<Bits2, Signed2>>
{
    using type = std::conditional_t < Bits == Bits2,
          wide::integer<
              Bits,
              std::conditional_t<(std::is_same_v<Signed, Signed2> && std::is_same_v<Signed2, signed>), signed, unsigned>>,
          std::conditional_t<Bits2<Bits, wide::integer<Bits, Signed>, wide::integer<Bits2, Signed2>>>;
};

template <size_t Bits, typename Signed, typename Arithmetic>
struct common_type<wide::integer<Bits, Signed>, Arithmetic>
{
    static_assert(wide::ArithmeticConcept<Arithmetic>());

    using type = std::conditional_t<
        std::is_floating_point_v<Arithmetic>,
        Arithmetic,
        std::conditional_t<
            sizeof(Arithmetic) < Bits * sizeof(long),
            wide::integer<Bits, Signed>,
            std::conditional_t<
                Bits * sizeof(long) < sizeof(Arithmetic),
                Arithmetic,
                std::conditional_t<
                    Bits * sizeof(long) == sizeof(Arithmetic) && (std::is_same_v<Signed, signed> || std::is_signed_v<Arithmetic>),
                    Arithmetic,
                    wide::integer<Bits, Signed>>>>>;
};

template <typename Arithmetic, size_t Bits, typename Signed>
struct common_type<Arithmetic, wide::integer<Bits, Signed>> : common_type<wide::integer<Bits, Signed>, Arithmetic>
{
};

}

namespace wide
{

template <size_t Bits, typename Signed>
struct integer<Bits, Signed>::_impl
{
    static_assert(Bits % CHAR_BIT == 0, "=)");

    // utils
    static const int base_bits = sizeof(base_type) * CHAR_BIT;
    static const int arr_size = Bits / base_bits;
    static constexpr size_t _Bits = Bits;
    static constexpr bool _is_wide_integer = true;

    // The original implementation is big-endian. We need little one.
    static constexpr unsigned little(unsigned idx) { return idx; }
    static constexpr unsigned big(unsigned idx) { return arr_size - 1 - idx; }
    static constexpr unsigned any(unsigned idx) { return idx; }

    template <size_t B, class T>
    constexpr static bool is_negative(const integer<B, T> & n) noexcept
    {
        if constexpr (std::is_same_v<T, signed>)
            return static_cast<signed_base_type>(n.m_arr[big(0)]) < 0;
        else
            return false;
    }

    template <size_t B, class S>
    constexpr static integer<B, S> make_positive(const integer<B, S> & n) noexcept
    {
        return is_negative(n) ? operator_unary_minus(n) : n;
    }

    template <typename T>
    constexpr static auto to_Integral(T f) noexcept
    {
        if constexpr (std::is_same_v<T, __int128>)
            return f;
        else if constexpr (std::is_signed_v<T>)
            return static_cast<int64_t>(f);
        else
            return static_cast<uint64_t>(f);
    }

    template <typename Integral>
    constexpr static void wide_integer_from_bultin(integer<Bits, Signed> & self, Integral rhs) noexcept
    {
        auto r = _impl::to_Integral(rhs);

        int r_idx = 0;
        for (; static_cast<size_t>(r_idx) < sizeof(Integral) && r_idx < arr_size; ++r_idx)
        {
            base_type & curr = self.m_arr[little(r_idx)];
            base_type curr_rhs = (r >> (r_idx * CHAR_BIT)) & std::numeric_limits<base_type>::max();
            curr = curr_rhs;
        }

        for (; r_idx < arr_size; ++r_idx)
        {
            base_type & curr = self.m_arr[little(r_idx)];
            curr = r < 0 ? std::numeric_limits<base_type>::max() : 0;
        }
    }

    constexpr static void wide_integer_from_bultin(integer<Bits, Signed> & self, double rhs) noexcept
    {
        if ((rhs > 0 && rhs < std::numeric_limits<uint64_t>::max()) || (rhs < 0 && rhs > std::numeric_limits<int64_t>::min()))
        {
            self = to_Integral(rhs);
            return;
        }

        long double r = rhs;
        if (r < 0)
            r = -r;

        size_t count = r / std::numeric_limits<uint64_t>::max();
        self = count;
        self *= std::numeric_limits<uint64_t>::max();
        long double to_diff = count;
        to_diff *= std::numeric_limits<uint64_t>::max();

        self += to_Integral(r - to_diff);

        if (rhs < 0)
            self = -self;
    }

    template <size_t Bits2, typename Signed2>
    constexpr static void
    wide_integer_from_wide_integer(integer<Bits, Signed> & self, const integer<Bits2, Signed2> & rhs) noexcept
    {
        //        int Bits_to_copy = std::min(arr_size, rhs.arr_size);
        auto rhs_arr_size = integer<Bits2, Signed2>::_impl::arr_size;
        int base_elems_to_copy = _impl::arr_size < rhs_arr_size ? _impl::arr_size : rhs_arr_size;
        for (int i = 0; i < base_elems_to_copy; ++i)
        {
            self.m_arr[little(i)] = rhs.m_arr[little(i)];
        }
        for (int i = 0; i < arr_size - base_elems_to_copy; ++i)
        {
            self.m_arr[big(i)] = is_negative(rhs) ? std::numeric_limits<base_type>::max() : 0;
        }
    }

    template <typename T>
    constexpr static bool should_keep_size()
    {
        return sizeof(T) * CHAR_BIT <= Bits;
    }

    constexpr static integer<Bits, unsigned> shift_left(const integer<Bits, unsigned> & rhs, int n) noexcept
    {
        if (static_cast<size_t>(n) >= base_bits * arr_size)
            return 0;
        if (n <= 0)
            return rhs;

        integer<Bits, Signed> lhs = rhs;
        int bit_shift = n % base_bits;
        unsigned n_bytes = n / base_bits;
        if (bit_shift)
        {
            lhs.m_arr[big(0)] <<= bit_shift;
            for (int i = 1; i < arr_size; ++i)
            {
                lhs.m_arr[big(i - 1)] |= lhs.m_arr[big(i)] >> (base_bits - bit_shift);
                lhs.m_arr[big(i)] <<= bit_shift;
            }
        }
        if (n_bytes)
        {
            for (unsigned i = 0; i < arr_size - n_bytes; ++i)
            {
                lhs.m_arr[big(i)] = lhs.m_arr[big(i + n_bytes)];
            }
            for (unsigned i = arr_size - n_bytes; i < arr_size; ++i)
                lhs.m_arr[big(i)] = 0;
        }
        return lhs;
    }

    constexpr static integer<Bits, signed> shift_left(const integer<Bits, signed> & rhs, int n) noexcept
    {
        return integer<Bits, signed>(shift_left(integer<Bits, unsigned>(rhs), n));
    }

    constexpr static integer<Bits, unsigned> shift_right(const integer<Bits, unsigned> & rhs, int n) noexcept
    {
        if (static_cast<size_t>(n) >= base_bits * arr_size)
            return 0;
        if (n <= 0)
            return rhs;

        integer<Bits, Signed> lhs = rhs;
        int bit_shift = n % base_bits;
        unsigned n_bytes = n / base_bits;
        if (bit_shift)
        {
            lhs.m_arr[little(0)] >>= bit_shift;
            for (int i = 1; i < arr_size; ++i)
            {
                lhs.m_arr[little(i - 1)] |= lhs.m_arr[little(i)] << (base_bits - bit_shift);
                lhs.m_arr[little(i)] >>= bit_shift;
            }
        }
        if (n_bytes)
        {
            for (unsigned i = 0; i < arr_size - n_bytes; ++i)
            {
                lhs.m_arr[little(i)] = lhs.m_arr[little(i + n_bytes)];
            }
            for (unsigned i = arr_size - n_bytes; i < arr_size; ++i)
                lhs.m_arr[little(i)] = 0;
        }
        return lhs;
    }

    constexpr static integer<Bits, signed> shift_right(const integer<Bits, signed> & rhs, int n) noexcept
    {
        if (static_cast<size_t>(n) >= base_bits * arr_size)
            return 0;
        if (n <= 0)
            return rhs;

        bool is_neg = is_negative(rhs);
        if (!is_neg)
            return shift_right(integer<Bits, unsigned>(rhs), n);

        integer<Bits, Signed> lhs = rhs;
        int bit_shift = n % base_bits;
        unsigned n_bytes = n / base_bits;
        if (bit_shift)
        {
            lhs = shift_right(integer<Bits, unsigned>(lhs), bit_shift);
            lhs.m_arr[big(0)] |= std::numeric_limits<base_type>::max() << (base_bits - bit_shift);
        }
        if (n_bytes)
        {
            for (unsigned i = 0; i < arr_size - n_bytes; ++i)
            {
                lhs.m_arr[little(i)] = lhs.m_arr[little(i + n_bytes)];
            }
            for (unsigned i = arr_size - n_bytes; i < arr_size; ++i)
            {
                lhs.m_arr[little(i)] = std::numeric_limits<base_type>::max();
            }
        }
        return lhs;
    }

    template <typename T>
    constexpr static integer<Bits, Signed>
    operator_plus_T(const integer<Bits, Signed> & lhs, T rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        if (rhs < 0)
            return _operator_minus_T(lhs, -rhs);
        else
            return _operator_plus_T(lhs, rhs);
    }

private:
    template <typename T>
    constexpr static integer<Bits, Signed>
    _operator_minus_T(const integer<Bits, Signed> & lhs, T rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        integer<Bits, Signed> res = lhs;

        bool is_underflow = false;
        int r_idx = 0;
        for (; static_cast<size_t>(r_idx) < sizeof(T) && r_idx < arr_size; ++r_idx)
        {
            base_type & res_i = res.m_arr[little(r_idx)];
            base_type curr_rhs = (rhs >> (r_idx * CHAR_BIT)) & std::numeric_limits<base_type>::max();

            if (is_underflow)
            {
                --res_i;
                is_underflow = res_i == std::numeric_limits<base_type>::max();
            }

            if (res_i < curr_rhs)
                is_underflow = true;
            res_i -= curr_rhs;
        }

        if (is_underflow && r_idx < arr_size)
        {
            --res.m_arr[little(r_idx)];
            for (int i = arr_size - 1 - r_idx - 1; i >= 0; --i)
            {
                if (res.m_arr[big(i + 1)] == std::numeric_limits<base_type>::max())
                    --res.m_arr[big(i)];
                else
                    break;
            }
        }

        return res;
    }

    template <typename T>
    constexpr static integer<Bits, Signed>
    _operator_plus_T(const integer<Bits, Signed> & lhs, T rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        integer<Bits, Signed> res = lhs;

        bool is_overflow = false;
        int r_idx = 0;
        for (; static_cast<size_t>(r_idx) < sizeof(T) && r_idx < arr_size; ++r_idx)
        {
            base_type & res_i = res.m_arr[little(r_idx)];
            base_type curr_rhs = (rhs >> (r_idx * CHAR_BIT)) & std::numeric_limits<base_type>::max();

            if (is_overflow)
            {
                ++res_i;
                is_overflow = res_i == 0;
            }

            res_i += curr_rhs;
            if (res_i < curr_rhs)
                is_overflow = true;
        }

        if (is_overflow && r_idx < arr_size)
        {
            ++res.m_arr[little(r_idx)];
            for (int i = arr_size - 1 - r_idx - 1; i >= 0; --i)
            {
                if (res.m_arr[big(i + 1)] == 0)
                    ++res.m_arr[big(i)];
                else
                    break;
            }
        }

        return res;
    }

public:
    constexpr static integer<Bits, Signed> operator_unary_tilda(const integer<Bits, Signed> & lhs) noexcept
    {
        integer<Bits, Signed> res{};

        for (int i = 0; i < arr_size; ++i)
            res.m_arr[any(i)] = ~lhs.m_arr[any(i)];
        return res;
    }

    constexpr static integer<Bits, Signed>
    operator_unary_minus(const integer<Bits, Signed> & lhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        return operator_plus_T(operator_unary_tilda(lhs), 1);
    }

    template <typename T>
    constexpr static auto operator_plus(const integer<Bits, Signed> & lhs, const T & rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> t = rhs;
            if (is_negative(t))
                return _operator_minus_wide_integer(lhs, operator_unary_minus(t));
            else
                return _operator_plus_wide_integer(lhs, t);
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_Bits, Signed>>::_impl::operator_plus(
                integer<T::_impl::_Bits, Signed>(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_minus(const integer<Bits, Signed> & lhs, const T & rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> t = rhs;
            if (is_negative(t))
                return _operator_plus_wide_integer(lhs, operator_unary_minus(t));
            else
                return _operator_minus_wide_integer(lhs, t);
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_Bits, Signed>>::_impl::operator_minus(
                integer<T::_impl::_Bits, Signed>(lhs), rhs);
        }
    }

private:
    constexpr static integer<Bits, Signed> _operator_minus_wide_integer(
        const integer<Bits, Signed> & lhs, const integer<Bits, Signed> & rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        integer<Bits, Signed> res = lhs;

        bool is_underflow = false;
        for (int idx = 0; idx < arr_size; ++idx)
        {
            base_type & res_i = res.m_arr[little(idx)];
            const base_type rhs_i = rhs.m_arr[little(idx)];

            if (is_underflow)
            {
                --res_i;
                is_underflow = res_i == std::numeric_limits<base_type>::max();
            }

            if (res_i < rhs_i)
                is_underflow = true;

            res_i -= rhs_i;
        }

        return res;
    }

    constexpr static integer<Bits, Signed> _operator_plus_wide_integer(
        const integer<Bits, Signed> & lhs, const integer<Bits, Signed> & rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        integer<Bits, Signed> res = lhs;

        bool is_overflow = false;
        for (int idx = 0; idx < arr_size; ++idx)
        {
            base_type & res_i = res.m_arr[little(idx)];
            const base_type rhs_i = rhs.m_arr[little(idx)];

            if (is_overflow)
            {
                ++res_i;
                is_overflow = res_i == 0;
            }

            res_i += rhs_i;

            if (res_i < rhs_i)
                is_overflow = true;
        }

        return res;
    }

public:
    template <typename T>
    constexpr static auto operator_star(const integer<Bits, Signed> & lhs, const T & rhs)
    {
        if constexpr (should_keep_size<T>())
        {
            const integer<Bits, unsigned> a = make_positive(lhs);
            integer<Bits, unsigned> t = make_positive(integer<Bits, Signed>(rhs));

            integer<Bits, Signed> res = 0;

            for (size_t i = 0; i < arr_size * base_bits; ++i)
            {
                if (t.m_arr[little(0)] & 1)
                    res = operator_plus(res, shift_left(a, i));

                t = shift_right(t, 1);
            }

            if (std::is_same_v<Signed, signed> && is_negative(integer<Bits, Signed>(rhs)) != is_negative(lhs))
                res = operator_unary_minus(res);

            return res;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_star(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_more(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            // static_assert(Signed == std::is_signed<T>::value,
            //               "warning: operator_more: comparison of integers of different signs");

            integer<Bits, Signed> t = rhs;

            if (std::numeric_limits<T>::is_signed && (is_negative(lhs) != is_negative(t)))
                return is_negative(t);

            for (int i = 0; i < arr_size; ++i)
            {
                if (lhs.m_arr[big(i)] != t.m_arr[big(i)])
                    return lhs.m_arr[big(i)] > t.m_arr[big(i)];
            }

            return false;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_more(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_less(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            // static_assert(Signed == std::is_signed<T>::value,
            //               "warning: operator_less: comparison of integers of different signs");

            integer<Bits, Signed> t = rhs;

            if (std::numeric_limits<T>::is_signed && (is_negative(lhs) != is_negative(t)))
                return is_negative(lhs);

            for (int i = 0; i < arr_size; ++i)
                if (lhs.m_arr[big(i)] != t.m_arr[big(i)])
                    return lhs.m_arr[big(i)] < t.m_arr[big(i)];

            return false;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_less(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_eq(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> t = rhs;

            for (int i = 0; i < arr_size; ++i)
                if (lhs.m_arr[any(i)] != t.m_arr[any(i)])
                    return false;

            return true;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_eq(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_pipe(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> t = rhs;
            integer<Bits, Signed> res = lhs;

            for (int i = 0; i < arr_size; ++i)
                res.m_arr[any(i)] |= t.m_arr[any(i)];
            return res;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_pipe(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_amp(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> t = rhs;
            integer<Bits, Signed> res = lhs;

            for (int i = 0; i < arr_size; ++i)
                res.m_arr[any(i)] &= t.m_arr[any(i)];
            return res;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_amp(T(lhs), rhs);
        }
    }

private:
    template <typename T>
    constexpr static void divide(const T & lhserator, const T & denominator, T & quotient, T & remainder)
    {
        bool is_zero = true;
        for (auto c : denominator.m_arr)
        {
            if (c != 0)
            {
                is_zero = false;
                break;
            }
        }

        if (is_zero)
            throwError("divide by zero");

        T n = lhserator;
        T d = denominator;
        T x = 1;
        T answer = 0;

        while (!operator_more(d, n) && operator_eq(operator_amp(shift_right(d, base_bits * arr_size - 1), 1), 0))
        {
            x = shift_left(x, 1);
            d = shift_left(d, 1);
        }

        while (!operator_eq(x, 0))
        {
            if (!operator_more(d, n))
            {
                n = operator_minus(n, d);
                answer = operator_pipe(answer, x);
            }

            x = shift_right(x, 1);
            d = shift_right(d, 1);
        }

        quotient = answer;
        remainder = n;
    }

public:
    template <typename T>
    constexpr static auto operator_slash(const integer<Bits, Signed> & lhs, const T & rhs)
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> o = rhs;
            integer<Bits, Signed> quotient{}, remainder{};
            divide(make_positive(lhs), make_positive(o), quotient, remainder);

            if (std::is_same_v<Signed, signed> && is_negative(o) != is_negative(lhs))
                quotient = operator_unary_minus(quotient);

            return quotient;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_Bits, Signed>>::operator_slash(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_percent(const integer<Bits, Signed> & lhs, const T & rhs)
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> o = rhs;
            integer<Bits, Signed> quotient{}, remainder{};
            divide(make_positive(lhs), make_positive(o), quotient, remainder);

            if (std::is_same_v<Signed, signed> && is_negative(lhs))
                remainder = operator_unary_minus(remainder);

            return remainder;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_Bits, Signed>>::operator_percent(T(lhs), rhs);
        }
    }

    // ^
    template <typename T>
    constexpr static auto operator_circumflex(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> t(rhs);
            integer<Bits, Signed> res = lhs;

            for (int i = 0; i < arr_size; ++i)
                res.m_arr[any(i)] ^= t.m_arr[any(i)];
            return res;
        }
        else
        {
            static_assert(T::_impl::_is_wide_integer, "");
            return T::operator_circumflex(T(lhs), rhs);
        }
    }

    constexpr static integer<Bits, Signed> from_str(const char * c)
    {
        integer<Bits, Signed> res = 0;

        bool is_neg = std::is_same_v<Signed, signed> && *c == '-';
        if (is_neg)
            ++c;

        if (*c == '0' && (*(c + 1) == 'x' || *(c + 1) == 'X'))
        { // hex
            ++c;
            ++c;
            while (*c)
            {
                if (*c >= '0' && *c <= '9')
                {
                    res = operator_star(res, 16U);
                    res = operator_plus_T(res, *c - '0');
                    ++c;
                }
                else if (*c >= 'a' && *c <= 'f')
                {
                    res = operator_star(res, 16U);
                    res = operator_plus_T(res, *c - 'a' + 10U);
                    ++c;
                }
                else if (*c >= 'A' && *c <= 'F')
                { // tolower must be used, but it is not constexpr
                    res = operator_star(res, 16U);
                    res = operator_plus_T(res, *c - 'A' + 10U);
                    ++c;
                }
                else
                    throwError("invalid char from");
            }
        }
        else
        { // dec
            while (*c)
            {
                if (*c < '0' || *c > '9')
                    throwError("invalid char from");

                res = operator_star(res, 10U);
                res = operator_plus_T(res, *c - '0');
                ++c;
            }
        }

        if (is_neg)
            res = operator_unary_minus(res);

        return res;
    }
};

// Members

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>::integer(T rhs) noexcept
    : m_arr{}
{
    if constexpr (IsWideInteger<T>::value)
        _impl::wide_integer_from_wide_integer(*this, rhs);
    else
        _impl::wide_integer_from_bultin(*this, rhs);
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>::integer(std::initializer_list<T> il) noexcept
    : m_arr{}
{
    if (il.size() == 1)
    {
        if constexpr (IsWideInteger<T>::value)
            _impl::wide_integer_from_wide_integer(*this, *il.begin());
        else
            _impl::wide_integer_from_bultin(*this, *il.begin());
    }
    else
        _impl::wide_integer_from_bultin(*this, 0);
}

template <size_t Bits, typename Signed>
template <size_t Bits2, typename Signed2>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator=(const integer<Bits2, Signed2> & rhs) noexcept
{
    _impl::wide_integer_from_wide_integer(*this, rhs);
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator=(T rhs) noexcept
{
    _impl::wide_integer_from_bultin(*this, rhs);
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator*=(const T & rhs)
{
    *this = *this * rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator/=(const T & rhs)
{
    *this = *this / rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator+=(const T & rhs) noexcept(std::is_same_v<Signed, unsigned>)
{
    *this = *this + rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator-=(const T & rhs) noexcept(std::is_same_v<Signed, unsigned>)
{
    *this = *this - rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator%=(const T & rhs)
{
    *this = *this % rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator&=(const T & rhs) noexcept
{
    *this = *this & rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator|=(const T & rhs) noexcept
{
    *this = *this | rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator^=(const T & rhs) noexcept
{
    *this = *this ^ rhs;
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator<<=(int n) noexcept
{
    *this = _impl::shift_left(*this, n);
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator>>=(int n) noexcept
{
    *this = _impl::shift_right(*this, n);
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator++() noexcept(std::is_same_v<Signed, unsigned>)
{
    *this = _impl::operator_plus(*this, 1);
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> integer<Bits, Signed>::operator++(int) noexcept(std::is_same_v<Signed, unsigned>)
{
    auto tmp = *this;
    *this = _impl::operator_plus(*this, 1);
    return tmp;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator--() noexcept(std::is_same_v<Signed, unsigned>)
{
    *this = _impl::operator_minus(*this, 1);
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> integer<Bits, Signed>::operator--(int) noexcept(std::is_same_v<Signed, unsigned>)
{
    auto tmp = *this;
    *this = _impl::operator_minus(*this, 1);
    return tmp;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>::operator bool() const noexcept
{
    return !_impl::operator_eq(*this, 0);
}

template <size_t Bits, typename Signed>
template <class T, class>
constexpr integer<Bits, Signed>::operator T() const noexcept
{
    static_assert(std::numeric_limits<T>::is_integer, "");
    T res = 0;
    for (size_t r_idx = 0; r_idx < _impl::arr_size && r_idx < sizeof(T); ++r_idx)
    {
        res |= (T(m_arr[_impl::little(r_idx)]) << (_impl::base_bits * r_idx));
    }
    return res;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>::operator long double() const noexcept
{
    if (_impl::operator_eq(*this, 0))
        return 0;

    integer<Bits, Signed> tmp = *this;
    if (_impl::is_negative(*this))
        tmp = -tmp;

    long double res = 0;
    for (size_t idx = 0; idx < _impl::arr_size; ++idx)
    {
        long double t = res;
        res *= std::numeric_limits<base_type>::max();
        res += t;
        res += tmp.m_arr[_impl::big(idx)];
    }

    if (_impl::is_negative(*this))
        res = -res;

    return res;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>::operator double() const noexcept
{
    return static_cast<long double>(*this);
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>::operator float() const noexcept
{
    return static_cast<long double>(*this);
}

// Unary operators
template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator~(const integer<Bits, Signed> & lhs) noexcept
{
    return integer<Bits, Signed>::_impl::operator_unary_tilda(lhs);
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator-(const integer<Bits, Signed> & lhs) noexcept(std::is_same_v<Signed, unsigned>)
{
    return integer<Bits, Signed>::_impl::operator_unary_minus(lhs);
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator+(const integer<Bits, Signed> & lhs) noexcept(std::is_same_v<Signed, unsigned>)
{
    return lhs;
}

#define CT(x) \
    std::common_type_t<std::decay_t<decltype(rhs)>, std::decay_t<decltype(lhs)>> { x }

// Binary operators
template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr
operator*(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_star(lhs, rhs);
}

template <typename Arithmetic, typename Arithmetic2, class>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator*(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) * CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr
operator/(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_slash(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator/(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) / CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr
operator+(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_plus(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator+(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) + CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr
operator-(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_minus(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator-(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) - CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr
operator%(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_percent(lhs, rhs);
}
template <typename Integral, typename Integral2, class>
std::common_type_t<Integral, Integral2> constexpr operator%(const Integral & lhs, const Integral2 & rhs)
{
    return CT(lhs) % CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr
operator&(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_amp(lhs, rhs);
}
template <typename Integral, typename Integral2, class>
std::common_type_t<Integral, Integral2> constexpr operator&(const Integral & lhs, const Integral2 & rhs)
{
    return CT(lhs) & CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr
operator|(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_pipe(lhs, rhs);
}
template <typename Integral, typename Integral2, class>
std::common_type_t<Integral, Integral2> constexpr operator|(const Integral & lhs, const Integral2 & rhs)
{
    return CT(lhs) | CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr
operator^(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_circumflex(lhs, rhs);
}
template <typename Integral, typename Integral2, class>
std::common_type_t<Integral, Integral2> constexpr operator^(const Integral & lhs, const Integral2 & rhs)
{
    return CT(lhs) ^ CT(rhs);
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator<<(const integer<Bits, Signed> & lhs, int n) noexcept
{
    return integer<Bits, Signed>::_impl::shift_left(lhs, n);
}
template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator>>(const integer<Bits, Signed> & lhs, int n) noexcept
{
    return integer<Bits, Signed>::_impl::shift_right(lhs, n);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator<(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_less(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator<(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) < CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator>(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_more(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator>(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) > CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator<=(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_less(lhs, rhs)
        || std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_eq(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator<=(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) <= CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator>=(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_more(lhs, rhs)
        || std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_eq(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator>=(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) >= CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator==(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_eq(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator==(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) == CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator!=(const integer<Bits, Signed> & lhs, const integer<Bits2, Signed2> & rhs)
{
    return !std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_eq(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator!=(const Arithmetic & lhs, const Arithmetic2 & rhs)
{
    return CT(lhs) != CT(rhs);
}

#undef CT

}

namespace std
{

template <size_t Bits, typename Signed>
struct hash<wide::integer<Bits, Signed>>
{
    std::size_t operator()(const wide::integer<Bits, Signed> & lhs) const
    {
        static_assert(Bits % (sizeof(size_t) * 8) == 0);

        const auto * ptr = reinterpret_cast<const size_t *>(lhs.m_arr);
        unsigned count = Bits / (sizeof(size_t) * 8);

        size_t res = 0;
        for (unsigned i = 0; i < count; ++i)
            res ^= ptr[i];
        return res;
    }
};

}
