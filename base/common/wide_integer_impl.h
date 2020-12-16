#pragma once

/// Original is here https://github.com/cerevra/int
/// Distributed under the Boost Software License, Version 1.0.
/// (See at http://www.boost.org/LICENSE_1_0.txt)

#include "throwError.h"
#include <cfloat>
#include <limits>
#include <cassert>

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
            res.items[T::_impl::big(0)] = std::numeric_limits<typename wide::integer<Bits, Signed>::signed_base_type>::min();
            return res;
        }
        return 0;
    }

    static constexpr wide::integer<Bits, Signed> max() noexcept
    {
        using T = wide::integer<Bits, Signed>;
        T res{};
        res.items[T::_impl::big(0)] = is_same<Signed, signed>::value
            ? std::numeric_limits<typename wide::integer<Bits, Signed>::signed_base_type>::max()
            : std::numeric_limits<typename wide::integer<Bits, Signed>::base_type>::max();
        for (unsigned i = 1; i < wide::integer<Bits, Signed>::_impl::item_count; ++i)
        {
            res.items[T::_impl::big(i)] = std::numeric_limits<typename wide::integer<Bits, Signed>::base_type>::max();
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
    static constexpr size_t _Bits = Bits;
    static constexpr const unsigned byte_count = Bits / 8;
    static constexpr const unsigned item_count = byte_count / sizeof(base_type);
    static constexpr const unsigned base_bits = sizeof(base_type) * 8;

    static_assert(Bits % base_bits == 0);

    /// Simple iteration in both directions
    static constexpr unsigned little(unsigned idx) { return idx; }
    static constexpr unsigned big(unsigned idx) { return item_count - 1 - idx; }
    static constexpr unsigned any(unsigned idx) { return idx; }

    template <class T>
    constexpr static bool is_negative(const T & n) noexcept
    {
        if constexpr (std::is_signed_v<T>)
            return n < 0;
        else
            return false;
    }

    template <size_t B, class T>
    constexpr static bool is_negative(const integer<B, T> & n) noexcept
    {
        if constexpr (std::is_same_v<T, signed>)
            return static_cast<signed_base_type>(n.items[big(0)]) < 0;
        else
            return false;
    }

    template <typename T>
    constexpr static auto make_positive(const T & n) noexcept
    {
        if constexpr (std::is_signed_v<T>)
            return n < 0 ? -n : n;
        else
            return n;
    }

    template <size_t B, class S>
    constexpr static integer<B, S> make_positive(const integer<B, S> & n) noexcept
    {
        return is_negative(n) ? operator_unary_minus(n) : n;
    }

    template <typename T>
    __attribute__((no_sanitize("undefined"))) constexpr static auto to_Integral(T f) noexcept
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
        self.items[0] = _impl::to_Integral(rhs);
        if constexpr (std::is_same_v<Integral, __int128>)
            self.items[1] = rhs >> base_bits;

        constexpr const unsigned start = (sizeof(Integral) == 16) ? 2 : 1;

        if constexpr (std::is_signed_v<Integral>)
        {
            if (rhs < 0)
            {
                for (unsigned i = start; i < item_count; ++i)
                    self.items[i] = -1;
                return;
            }
        }

        for (unsigned i = start; i < item_count; ++i)
            self.items[i] = 0;
    }

    /**
     * N.B. t is constructed from double, so max(t) = max(double) ~ 2^310
     * the recursive call happens when t / 2^64 > 2^64, so there won't be more than 5 of them.
     *
     * t = a1 * max_int + b1,   a1 > max_int, b1 < max_int
     * a1 = a2 * max_int + b2,  a2 > max_int, b2 < max_int
     * a_(n - 1) = a_n * max_int + b2, a_n <= max_int <- base case.
     */
    template <class T>
    constexpr static void set_multiplier(integer<Bits, Signed> & self, T t) noexcept {
        constexpr uint64_t max_int = std::numeric_limits<uint64_t>::max();
        const T alpha = t / max_int;

        if (alpha <= max_int)
            self = static_cast<uint64_t>(alpha);
        else // max(double) / 2^64 will surely contain less than 52 precision bits, so speed up computations.
            set_multiplier<double>(self, alpha);

        self *= max_int;
        self += static_cast<uint64_t>(t - alpha * max_int); // += b_i
    }

    constexpr static void wide_integer_from_bultin(integer<Bits, Signed>& self, double rhs) noexcept {
        constexpr int64_t max_int = std::numeric_limits<int64_t>::max();
        constexpr int64_t min_int = std::numeric_limits<int64_t>::min();

        /// There are values in int64 that have more than 53 significant bits (in terms of double
        /// representation). Such values, being promoted to double, are rounded up or down. If they are rounded up,
        /// the result may not fit in 64 bits.
        /// The example of such a number is 9.22337e+18.
        /// As to_Integral does a static_cast to int64_t, it may result in UB.
        /// The necessary check here is that long double has enough significant (mantissa) bits to store the
        /// int64_t max value precisely.
        static_assert(LDBL_MANT_DIG >= 64,
            "On your system long double has less than 64 precision bits,"
            "which may result in UB when initializing double from int64_t");

        if ((rhs > 0 && rhs < max_int) || (rhs < 0 && rhs > min_int))
        {
            self = static_cast<int64_t>(rhs);
            return;
        }

        const long double rhs_long_double = (static_cast<long double>(rhs) < 0)
            ? -static_cast<long double>(rhs)
            : rhs;

        set_multiplier(self, rhs_long_double);

        if (rhs < 0)
            self = -self;
    }

    template <size_t Bits2, typename Signed2>
    constexpr static void
    wide_integer_from_wide_integer(integer<Bits, Signed> & self, const integer<Bits2, Signed2> & rhs) noexcept
    {
        constexpr const unsigned min_bits = (Bits < Bits2) ? Bits : Bits2;
        constexpr const unsigned to_copy = min_bits / base_bits;

        for (unsigned i = 0; i < to_copy; ++i)
            self.items[i] = rhs.items[i];

        if constexpr (Bits > Bits2)
        {
            if constexpr (std::is_signed_v<Signed2>)
            {
                if (rhs < 0)
                {
                    for (unsigned i = to_copy; i < item_count; ++i)
                        self.items[i] = -1;
                    return;
                }
            }

            for (unsigned i = to_copy; i < item_count; ++i)
                self.items[i] = 0;
        }
    }

    template <typename T>
    constexpr static bool should_keep_size()
    {
        return sizeof(T) <= byte_count;
    }

    constexpr static integer<Bits, Signed> shift_left(const integer<Bits, Signed> & rhs, unsigned n) noexcept
    {
        integer<Bits, Signed> lhs;
        unsigned items_shift = n / base_bits;

        if (unsigned bit_shift = n % base_bits)
        {
            unsigned overflow_shift = base_bits - bit_shift;

            lhs.items[big(0)] = rhs.items[big(items_shift)] << bit_shift;
            for (unsigned i = 1; i < item_count - items_shift; ++i)
            {
                lhs.items[big(i - 1)] |= rhs.items[big(items_shift + i)] >> overflow_shift;
                lhs.items[big(i)] = rhs.items[big(items_shift + i)] << bit_shift;
            }
        }
        else
        {
            for (unsigned i = 0; i < item_count - items_shift; ++i)
                lhs.items[big(i)] = rhs.items[big(items_shift + i)];
        }

        for (unsigned i = 0; i < items_shift; ++i)
            lhs.items[little(i)] = 0;
        return lhs;
    }

    constexpr static integer<Bits, Signed> shift_right(const integer<Bits, Signed> & rhs, unsigned n) noexcept
    {
        integer<Bits, Signed> lhs;
        unsigned items_shift = n / base_bits;
        unsigned bit_shift = n % base_bits;

        if (bit_shift)
        {
            unsigned overflow_shift = base_bits - bit_shift;

            lhs.items[little(0)] = rhs.items[little(items_shift)] >> bit_shift;
            for (unsigned i = 1; i < item_count - items_shift; ++i)
            {
                lhs.items[little(i - 1)] |= rhs.items[little(items_shift + i)] << overflow_shift;
                lhs.items[little(i)] = rhs.items[little(items_shift + i)] >> bit_shift;
            }
        }
        else
        {
            for (unsigned i = 0; i < item_count - items_shift; ++i)
                lhs.items[little(i)] = rhs.items[little(items_shift + i)];
        }

        if (is_negative(rhs))
        {
            if (bit_shift)
                lhs.items[big(items_shift)] |= std::numeric_limits<base_type>::max() << (base_bits - bit_shift);

            for (unsigned i = item_count - items_shift; i < items_shift; ++i)
                lhs.items[little(i)] = std::numeric_limits<base_type>::max();
        }
        else
        {
            for (unsigned i = item_count - items_shift; i < items_shift; ++i)
                lhs.items[little(i)] = 0;
        }

        return lhs;
    }

private:
    template <typename T>
    constexpr static base_type get_item(const T & x, unsigned number)
    {
        if constexpr (IsWideInteger<T>::value)
        {
            if (number < T::_impl::item_count)
                return x.items[number];
            return 0;
        }
        else
        {
            if constexpr (sizeof(T) <= sizeof(base_type))
            {
                if (!number)
                    return x;
            }
            else if (number * sizeof(base_type) < sizeof(T))
                return x >> (number * base_bits); // & std::numeric_limits<base_type>::max()
            return 0;
        }
    }

    template <typename T>
    constexpr static integer<Bits, Signed>
    minus(const integer<Bits, Signed> & lhs, T rhs)
    {
        constexpr const unsigned rhs_items = (sizeof(T) > sizeof(base_type)) ? (sizeof(T) / sizeof(base_type)) : 1;
        constexpr const unsigned op_items = (item_count < rhs_items) ? item_count : rhs_items;

        integer<Bits, Signed> res(lhs);
        bool underflows[item_count] = {};

        for (unsigned i = 0; i < op_items; ++i)
        {
            base_type rhs_item = get_item(rhs, i);
            base_type & res_item = res.items[little(i)];

            underflows[i] = res_item < rhs_item;
            res_item -= rhs_item;
        }

        for (unsigned i = 1; i < item_count; ++i)
        {
            if (underflows[i-1])
            {
                base_type & res_item = res.items[little(i)];
                if (res_item == 0)
                    underflows[i] = true;
                --res_item;
            }
        }

        return res;
    }

    template <typename T>
    constexpr static integer<Bits, Signed>
    plus(const integer<Bits, Signed> & lhs, T rhs)
    {
        constexpr const unsigned rhs_items = (sizeof(T) > sizeof(base_type)) ? (sizeof(T) / sizeof(base_type)) : 1;
        constexpr const unsigned op_items = (item_count < rhs_items) ? item_count : rhs_items;

        integer<Bits, Signed> res(lhs);
        bool overflows[item_count] = {};

        for (unsigned i = 0; i < op_items; ++i)
        {
            base_type rhs_item = get_item(rhs, i);
            base_type & res_item = res.items[little(i)];

            res_item += rhs_item;
            overflows[i] = res_item < rhs_item;
        }

        for (unsigned i = 1; i < item_count; ++i)
        {
            if (overflows[i-1])
            {
                base_type & res_item = res.items[little(i)];
                ++res_item;
                if (res_item == 0)
                    overflows[i] = true;
            }
        }

        return res;
    }

    template <typename T>
    constexpr static integer<Bits, Signed>
    multiply(const integer<Bits, Signed> & lhs, const T & rhs)
    {
        if constexpr (Bits == 256 && sizeof(base_type) == 8)
        {
            /// @sa https://github.com/abseil/abseil-cpp/blob/master/absl/numeric/int128.h
            using HalfType = unsigned __int128;

            HalfType a01 = (HalfType(lhs.items[little(1)]) << 64) + lhs.items[little(0)];
            HalfType a23 = (HalfType(lhs.items[little(3)]) << 64) + lhs.items[little(2)];
            HalfType a0 = lhs.items[little(0)];
            HalfType a1 = lhs.items[little(1)];

            HalfType b01 = rhs;
            uint64_t b0 = b01;
            uint64_t b1 = 0;
            HalfType b23 = 0;
            if constexpr (sizeof(T) > 8)
                b1 = b01 >> 64;
            if constexpr (sizeof(T) > 16)
                b23 = (HalfType(rhs.items[little(3)]) << 64) + rhs.items[little(2)];

            HalfType r23 = a23 * b01 + a01 * b23 + a1 * b1;
            HalfType r01 = a0 * b0;
            HalfType r12 = (r01 >> 64) + (r23 << 64);
            HalfType r12_x = a1 * b0;

            integer<Bits, Signed> res;
            res.items[little(0)] = r01;
            res.items[little(3)] = r23 >> 64;

            if constexpr (sizeof(T) > 8)
            {
                HalfType r12_y = a0 * b1;
                r12_x += r12_y;
                if (r12_x < r12_y)
                    ++res.items[little(3)];
            }

            r12 += r12_x;
            if (r12 < r12_x)
                ++res.items[little(3)];

            res.items[little(1)] = r12;
            res.items[little(2)] = r12 >> 64;
            return res;
        }
        else
        {
            integer<Bits, Signed> res{};
#if 1
            integer<Bits, Signed> lhs2 = plus(lhs, shift_left(lhs, 1));
            integer<Bits, Signed> lhs3 = plus(lhs2, shift_left(lhs, 2));
#endif
            for (unsigned i = 0; i < item_count; ++i)
            {
                base_type rhs_item = get_item(rhs, i);
                unsigned pos = i * base_bits;

                while (rhs_item)
                {
#if 1 /// optimization
                    if ((rhs_item & 0x7) == 0x7)
                    {
                        res = plus(res, shift_left(lhs3, pos));
                        rhs_item >>= 3;
                        pos += 3;
                        continue;
                    }

                    if ((rhs_item & 0x3) == 0x3)
                    {
                        res = plus(res, shift_left(lhs2, pos));
                        rhs_item >>= 2;
                        pos += 2;
                        continue;
                    }
#endif
                    if (rhs_item & 1)
                        res = plus(res, shift_left(lhs, pos));

                    rhs_item >>= 1;
                    ++pos;
                }
            }

            return res;
        }
    }

public:
    constexpr static integer<Bits, Signed> operator_unary_tilda(const integer<Bits, Signed> & lhs) noexcept
    {
        integer<Bits, Signed> res;

        for (unsigned i = 0; i < item_count; ++i)
            res.items[any(i)] = ~lhs.items[any(i)];
        return res;
    }

    constexpr static integer<Bits, Signed>
    operator_unary_minus(const integer<Bits, Signed> & lhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        return plus(operator_unary_tilda(lhs), 1);
    }

    template <typename T>
    constexpr static auto operator_plus(const integer<Bits, Signed> & lhs, const T & rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        if constexpr (should_keep_size<T>())
        {
            if (is_negative(rhs))
                return minus(lhs, -rhs);
            else
                return plus(lhs, rhs);
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_Bits, Signed>>::_impl::operator_plus(
                integer<T::_impl::_Bits, Signed>(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_minus(const integer<Bits, Signed> & lhs, const T & rhs) noexcept(std::is_same_v<Signed, unsigned>)
    {
        if constexpr (should_keep_size<T>())
        {
            if (is_negative(rhs))
                return plus(lhs, -rhs);
            else
                return minus(lhs, rhs);
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_Bits, Signed>>::_impl::operator_minus(
                integer<T::_impl::_Bits, Signed>(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_star(const integer<Bits, Signed> & lhs, const T & rhs)
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> res;

            if constexpr (std::is_signed_v<Signed>)
            {
                res = multiply((is_negative(lhs) ? make_positive(lhs) : lhs),
                                  (is_negative(rhs) ? make_positive(rhs) : rhs));
            }
            else
            {
                res = multiply(lhs, (is_negative(rhs) ? make_positive(rhs) : rhs));
            }

            if (std::is_same_v<Signed, signed> && is_negative(lhs) != is_negative(rhs))
                res = operator_unary_minus(res);

            return res;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_star(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_more(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            if (std::numeric_limits<T>::is_signed && (is_negative(lhs) != is_negative(rhs)))
                return is_negative(rhs);

            for (unsigned i = 0; i < item_count; ++i)
            {
                base_type rhs_item = get_item(rhs, big(i));

                if (lhs.items[big(i)] != rhs_item)
                    return lhs.items[big(i)] > rhs_item;
            }

            return false;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_more(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_less(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            if (std::numeric_limits<T>::is_signed && (is_negative(lhs) != is_negative(rhs)))
                return is_negative(lhs);

            for (unsigned i = 0; i < item_count; ++i)
            {
                base_type rhs_item = get_item(rhs, big(i));

                if (lhs.items[big(i)] != rhs_item)
                    return lhs.items[big(i)] < rhs_item;
            }

            return false;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_less(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_eq(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            for (unsigned i = 0; i < item_count; ++i)
            {
                base_type rhs_item = get_item(rhs, any(i));

                if (lhs.items[any(i)] != rhs_item)
                    return false;
            }

            return true;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_eq(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_pipe(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> res;

            for (unsigned i = 0; i < item_count; ++i)
                res.items[little(i)] = lhs.items[little(i)] | get_item(rhs, i);
            return res;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_pipe(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_amp(const integer<Bits, Signed> & lhs, const T & rhs) noexcept
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> res;

            for (unsigned i = 0; i < item_count; ++i)
                res.items[little(i)] = lhs.items[little(i)] & get_item(rhs, i);
            return res;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_amp(T(lhs), rhs);
        }
    }

private:
    template <typename T>
    constexpr static bool is_zero(const T & x)
    {
        bool is_zero = true;
        for (auto item : x.items)
        {
            if (item != 0)
            {
                is_zero = false;
                break;
            }
        }
        return is_zero;
    }

    /// returns quotient as result and remainder in numerator.
    template <typename T>
    constexpr static T divide(T & numerator, T && denominator)
    {
        if (is_zero(denominator))
            throwError("divide by zero");

        T & n = numerator;
        T & d = denominator;
        T x = 1;
        T quotient = 0;

        while (!operator_more(d, n) && operator_eq(operator_amp(shift_right(d, base_bits * item_count - 1), 1), 0))
        {
            x = shift_left(x, 1);
            d = shift_left(d, 1);
        }

        while (!operator_eq(x, 0))
        {
            if (!operator_more(d, n))
            {
                n = operator_minus(n, d);
                quotient = operator_pipe(quotient, x);
            }

            x = shift_right(x, 1);
            d = shift_right(d, 1);
        }

        return quotient;
    }

public:
    template <typename T>
    constexpr static auto operator_slash(const integer<Bits, Signed> & lhs, const T & rhs)
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> numerator = make_positive(lhs);
            integer<Bits, Signed> quotient = divide(numerator, make_positive(integer<Bits, Signed>(rhs)));

            if (std::is_same_v<Signed, signed> && is_negative(rhs) != is_negative(lhs))
                quotient = operator_unary_minus(quotient);
            return quotient;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_Bits, Signed>>::operator_slash(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_percent(const integer<Bits, Signed> & lhs, const T & rhs)
    {
        if constexpr (should_keep_size<T>())
        {
            integer<Bits, Signed> remainder = make_positive(lhs);
            divide(remainder, make_positive(integer<Bits, Signed>(rhs)));

            if (std::is_same_v<Signed, signed> && is_negative(lhs))
                remainder = operator_unary_minus(remainder);
            return remainder;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
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

            for (unsigned i = 0; i < item_count; ++i)
                res.items[any(i)] ^= t.items[any(i)];
            return res;
        }
        else
        {
            static_assert(IsWideInteger<T>::value);
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
                    res = multiply(res, 16U);
                    res = plus(res, *c - '0');
                    ++c;
                }
                else if (*c >= 'a' && *c <= 'f')
                {
                    res = multiply(res, 16U);
                    res = plus(res, *c - 'a' + 10U);
                    ++c;
                }
                else if (*c >= 'A' && *c <= 'F')
                { // tolower must be used, but it is not constexpr
                    res = multiply(res, 16U);
                    res = plus(res, *c - 'A' + 10U);
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

                res = multiply(res, 10U);
                res = plus(res, *c - '0');
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
    : items{}
{
    if constexpr (IsWideInteger<T>::value)
        _impl::wide_integer_from_wide_integer(*this, rhs);
    else
        _impl::wide_integer_from_bultin(*this, rhs);
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>::integer(std::initializer_list<T> il) noexcept
    : items{}
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
    if (static_cast<size_t>(n) >= Bits)
        *this = 0;
    else if (n > 0)
        *this = _impl::shift_left(*this, n);
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> & integer<Bits, Signed>::operator>>=(int n) noexcept
{
    if (static_cast<size_t>(n) >= Bits)
    {
        if (is_negative(*this))
            *this = -1;
        else
            *this = 0;
    }
    else if (n > 0)
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
    if constexpr (std::is_same_v<T, __int128>)
    {
        static_assert(Bits >= 128);
        return (__int128(items[1]) << 64) | items[0];
    }
    else
    {
        static_assert(std::numeric_limits<T>::is_integer);
        return items[0];
    }
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
    for (unsigned i = 0; i < _impl::item_count; ++i)
    {
        long double t = res;
        res *= std::numeric_limits<base_type>::max();
        res += t;
        res += tmp.items[_impl::big(i)];
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
    if (static_cast<size_t>(n) >= Bits)
        return 0;
    if (n <= 0)
        return lhs;
    return integer<Bits, Signed>::_impl::shift_left(lhs, n);
}
template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator>>(const integer<Bits, Signed> & lhs, int n) noexcept
{
    if (static_cast<size_t>(n) >= Bits)
        return 0;
    if (n <= 0)
        return lhs;
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

        const auto * ptr = reinterpret_cast<const size_t *>(lhs.items);
        unsigned count = Bits / (sizeof(size_t) * 8);

        size_t res = 0;
        for (unsigned i = 0; i < count; ++i)
            res ^= ptr[i];
        return res;
    }
};

}
