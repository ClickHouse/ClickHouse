#pragma once

#include <Functions/FunctionsArithmetic.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>

#include <cmath>
#include <type_traits>
#include <array>

#if __SSE4_1__
    #include <smmintrin.h>
#endif

/** If you want negative zeros will be replaced by zeros in result of calculations.
  * Disabled by performance reasons.
#define NO_NEGATIVE_ZEROS
  */


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** Rounding Functions:
    * roundToExp2 - down to the nearest power of two;
    * roundDuration - down to the nearest of: 0, 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000;
    * roundAge - down to the nearest of: 0, 18, 25, 35, 45, 55.
    *
    * round(x, N) - arithmetic rounding (N = 0 by default).
    * ceil(x, N) is the smallest number that is at least x (N = 0 by default).
    * floor(x, N) is the largest number that is not greater than x (N = 0 by default).
    *
    * The value of the parameter N:
    * - N > 0: round to the number with N decimal places after the decimal point
    * - N < 0: round to an integer with N zero characters
    * - N = 0: round to an integer
    */

template <typename A>
struct RoundToExp2Impl
{
    using ResultType = A;

    static inline A apply(A x)
    {
        return x <= 0 ? static_cast<A>(0) : (static_cast<A>(1) << static_cast<UInt64>(log2(static_cast<double>(x))));
    }
};

template <>
struct RoundToExp2Impl<Float32>
{
    using ResultType = Float32;

    static inline Float32 apply(Float32 x)
    {
        return static_cast<Float32>(x < 1 ? 0. : pow(2., floor(log2(x))));
    }
};

template <>
struct RoundToExp2Impl<Float64>
{
    using ResultType = Float64;

    static inline Float64 apply(Float64 x)
    {
        return x < 1 ? 0. : pow(2., floor(log2(x)));
    }
};

template <typename A>
struct RoundDurationImpl
{
    using ResultType = UInt16;

    static inline ResultType apply(A x)
    {
        return x < 1 ? 0
            : (x < 10 ? 1
            : (x < 30 ? 10
            : (x < 60 ? 30
            : (x < 120 ? 60
            : (x < 180 ? 120
            : (x < 240 ? 180
            : (x < 300 ? 240
            : (x < 600 ? 300
            : (x < 1200 ? 600
            : (x < 1800 ? 1200
            : (x < 3600 ? 1800
            : (x < 7200 ? 3600
            : (x < 18000 ? 7200
            : (x < 36000 ? 18000
            : 36000))))))))))))));
    }
};

template <typename A>
struct RoundAgeImpl
{
    using ResultType = UInt8;

    static inline ResultType apply(A x)
    {
        return x < 1 ? 0
            : (x < 18 ? 17
            : (x < 25 ? 18
            : (x < 35 ? 25
            : (x < 45 ? 35
            : (x < 55 ? 45
            : 55)))));
    }
};


/** This parameter controls the behavior of the rounding functions.
  */
enum class ScaleMode
{
    Positive,   // round to a number with N decimal places after the decimal point
    Negative,   // round to an integer with N zero characters
    Zero,       // round to an integer
    Null        // return zero value
};

enum class RoundingMode
{
#if __SSE4_1__
    Round   = _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC,
    Floor   = _MM_FROUND_TO_NEG_INF | _MM_FROUND_NO_EXC,
    Ceil    = _MM_FROUND_TO_POS_INF | _MM_FROUND_NO_EXC,
    Trunc   = _MM_FROUND_TO_ZERO | _MM_FROUND_NO_EXC,
#else
    Round   = 8,    /// Values are correspond to above just in case.
    Floor   = 9,
    Ceil    = 10,
    Trunc   = 11,
#endif
};


/** Implementing low-level rounding functions for integer values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct IntegerRoundingComputation
{
    template <size_t scale>
    static inline T computeImpl(T x)
    {
        switch (rounding_mode)
        {
            case RoundingMode::Trunc:
                return x / scale * scale;
            case RoundingMode::Floor:
                if (x < 0)
                    x -= scale - 1;
                return x / scale * scale;
            case RoundingMode::Ceil:
                if (x >= 0)
                    x += scale - 1;
                return x / scale * scale;
            case RoundingMode::Round:
                return (x + scale / 2) / scale * scale;
        }
    }

    static inline T compute(T x, size_t scale)
    {
        switch (scale_mode)
        {
            case ScaleMode::Null:
                return 0;
            case ScaleMode::Zero:
                return x;
            case ScaleMode::Positive:
                return x;
            case ScaleMode::Negative:
            {
                switch (scale)
                {
                    case 10ULL: return computeImpl<10ULL>(x);
                    case 100ULL: return computeImpl<100ULL>(x);
                    case 1000ULL: return computeImpl<1000ULL>(x);
                    case 10000ULL: return computeImpl<10000ULL>(x);
                    case 100000ULL: return computeImpl<100000ULL>(x);
                    case 1000000ULL: return computeImpl<1000000ULL>(x);
                    case 10000000ULL: return computeImpl<10000000ULL>(x);
                    case 100000000ULL: return computeImpl<100000000ULL>(x);
                    case 1000000000ULL: return computeImpl<1000000000ULL>(x);
                    case 10000000000ULL: return computeImpl<10000000000ULL>(x);
                    case 100000000000ULL: return computeImpl<100000000000ULL>(x);
                    case 1000000000000ULL: return computeImpl<1000000000000ULL>(x);
                    case 10000000000000ULL: return computeImpl<10000000000000ULL>(x);
                    case 100000000000000ULL: return computeImpl<100000000000000ULL>(x);
                    case 1000000000000000ULL: return computeImpl<1000000000000000ULL>(x);
                    case 10000000000000000ULL: return computeImpl<10000000000000000ULL>(x);
                    case 100000000000000000ULL: return computeImpl<100000000000000000ULL>(x);
                    case 1000000000000000000ULL: return computeImpl<1000000000000000000ULL>(x);
                    case 10000000000000000000ULL: return computeImpl<10000000000000000000ULL>(x);
                    default:
                        throw Exception("Logical error: unexpected 'scale' parameter passed to function IntegerRoundingComputation::compute",
                            ErrorCodes::LOGICAL_ERROR);
                }
            }
        }
    }
};


#if __SSE4_1__
template <typename T>
class BaseFloatRoundingComputation;

template <>
class BaseFloatRoundingComputation<Float32>
{
public:
    using Scale = __m128;
    static const size_t data_count = 4;

protected:
    static inline const __m128 & getZero()
    {
        static const __m128 zero = _mm_set1_ps(0.0);
        return zero;
    }

    static inline const __m128 & getOne()
    {
        static const __m128 one = _mm_set1_ps(1.0);
        return one;
    }

    static inline const __m128 & getTwo()
    {
        static const __m128 two = _mm_set1_ps(2.0);
        return two;
    }
};

template <>
class BaseFloatRoundingComputation<Float64>
{
public:
    using Scale = __m128d;
    static const size_t data_count = 2;

protected:
    static inline const __m128d & getZero()
    {
        static const __m128d zero = _mm_set1_pd(0.0);
        return zero;
    }

    static inline const __m128d & getOne()
    {
        static const __m128d one = _mm_set1_pd(1.0);
        return one;
    }

    static inline const __m128d & getTwo()
    {
        static const __m128d two = _mm_set1_pd(2.0);
        return two;
    }
};

/** Implementation of low-level round-off functions for floating-point values.
    */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
class FloatRoundingComputation;

template <RoundingMode rounding_mode>
class FloatRoundingComputation<Float32, rounding_mode, ScaleMode::Positive>
    : public BaseFloatRoundingComputation<Float32>
{
public:
    static inline void prepare(size_t scale, Scale & mm_scale)
    {
        Float32 fscale = static_cast<Float32>(scale);
        mm_scale = _mm_load1_ps(&fscale);
    }

    static inline void compute(const Float32 * __restrict in, const Scale & scale, Float32 * __restrict out)
    {
        __m128 val = _mm_loadu_ps(in);

        /// Rounding algorithm.
        val = _mm_mul_ps(val, scale);
        val = _mm_round_ps(val, int(rounding_mode));
        val = _mm_div_ps(val, scale);

        _mm_storeu_ps(out, val);
    }
};

template <RoundingMode rounding_mode>
class FloatRoundingComputation<Float32, rounding_mode, ScaleMode::Negative>
    : public BaseFloatRoundingComputation<Float32>
{
public:
    static inline void prepare(size_t scale, Scale & mm_scale)
    {
        Float32 fscale = static_cast<Float32>(scale);
        mm_scale = _mm_load1_ps(&fscale);
    }

    static inline void compute(const Float32 * __restrict in, const Scale & scale, Float32 * __restrict out)
    {
        __m128 val = _mm_loadu_ps(in);

        /// Turn negative values into positive values.
        __m128 sign = _mm_cmpge_ps(val, getZero());
        sign = _mm_min_ps(sign, getTwo());
        sign = _mm_sub_ps(sign, getOne());
        val = _mm_mul_ps(val, sign);

        /// Rounding algorithm.
        val = _mm_div_ps(val, scale);
        __m128 res = _mm_cmpge_ps(val, getOneTenth());
        val = _mm_round_ps(val, int(rounding_mode));
        val = _mm_mul_ps(val, scale);
        val = _mm_and_ps(val, res);

        /// Return the real signs of all values.
        val = _mm_mul_ps(val, sign);

        normalize(val, mask);
        _mm_storeu_ps(out, val);
    }

private:
    static inline const __m128 & getOneTenth()
    {
        static const __m128 one_tenth = _mm_set1_ps(0.1);
        return one_tenth;
    }
};

template <RoundingMode rounding_mode>
class FloatRoundingComputation<Float32, rounding_mode, ScaleMode::Zero>
    : public BaseFloatRoundingComputation<Float32>
{
public:
    static inline void prepare(size_t scale, Scale & mm_scale)
    {
    }

    static inline void compute(const Float32 * __restrict in, const Scale & scale, Float32 * __restrict out)
    {
        __m128 val = _mm_loadu_ps(in);
        val = _mm_round_ps(val, int(rounding_mode));
        _mm_storeu_ps(out, val);
    }
};

template <RoundingMode rounding_mode>
class FloatRoundingComputation<Float64, rounding_mode, ScaleMode::Positive>
    : public BaseFloatRoundingComputation<Float64>
{
public:
    static inline void prepare(size_t scale, Scale & mm_scale)
    {
        Float64 fscale = static_cast<Float64>(scale);
        mm_scale = _mm_load1_pd(&fscale);
    }

    static inline void compute(const Float64 * __restrict in, const Scale & scale, Float64 * __restrict out)
    {
        __m128d val = _mm_loadu_pd(in);

        /// Rounding algorithm.
        val = _mm_mul_pd(val, scale);
        val = _mm_round_pd(val, int(rounding_mode));
        val = _mm_div_pd(val, scale);

        _mm_storeu_pd(out, val);
    }
};

template <RoundingMode rounding_mode>
class FloatRoundingComputation<Float64, rounding_mode, ScaleMode::Negative>
    : public BaseFloatRoundingComputation<Float64>
{
public:
    static inline void prepare(size_t scale, Scale & mm_scale)
    {
        Float64 fscale = static_cast<Float64>(scale);
        mm_scale = _mm_load1_pd(&fscale);
    }

    static inline void compute(const Float64 * __restrict in, const Scale & scale, Float64 * __restrict out)
    {
        __m128d val = _mm_loadu_pd(in);

        /// Turn negative values into positive values.
        __m128d sign = _mm_cmpge_pd(val, getZero());
        sign = _mm_min_pd(sign, getTwo());
        sign = _mm_sub_pd(sign, getOne());
        val = _mm_mul_pd(val, sign);

        /// Rounding algorithm.
        val = _mm_div_pd(val, scale);
        __m128d res = _mm_cmpge_pd(val, getOneTenth());
        val = _mm_round_pd(val, int(rounding_mode));
        val = _mm_mul_pd(val, scale);
        val = _mm_and_pd(val, res);

        /// Return the real signs of all values.
        val = _mm_mul_pd(val, sign);

        _mm_storeu_pd(out, val);
    }

private:
    static inline const __m128d & getOneTenth()
    {
        static const __m128d one_tenth = _mm_set1_pd(0.1);
        return one_tenth;
    }
};

template <RoundingMode rounding_mode>
class FloatRoundingComputation<Float64, rounding_mode, ScaleMode::Zero>
    : public BaseFloatRoundingComputation<Float64>
{
public:
    static inline void prepare(size_t scale, Scale & mm_scale)
    {
    }

    static inline void compute(const Float64 * __restrict in, const Scale & scale, Float64 * __restrict out)
    {
        __m128d val = _mm_loadu_pd(in);
        val = _mm_round_pd(val, int(rounding_mode));
        _mm_storeu_pd(out, val);
    }
};
#else

/// Implementation for ARM. Not vectorized. Does not fix negative zeros.

template <RoundingMode rounding_mode> float roundWithMode(float x);
template <> float roundWithMode<RoundingMode::Round>(float x) { return roundf(x); }
template <> float roundWithMode<RoundingMode::Floor>(float x) { return floorf(x); }
template <> float roundWithMode<RoundingMode::Ceil>(float x) { return ceilf(x); }
template <> float roundWithMode<RoundingMode::Trunc>(float x) { return truncf(x); }

template <RoundingMode rounding_mode> double roundWithMode(double x);
template <> double roundWithMode<RoundingMode::Round>(double x) { return round(x); }
template <> double roundWithMode<RoundingMode::Floor>(double x) { return floor(x); }
template <> double roundWithMode<RoundingMode::Ceil>(double x) { return ceil(x); }
template <> double roundWithMode<RoundingMode::Trunc>(double x) { return trunc(x); }


template <typename T>
class BaseFloatRoundingComputation
{
public:
    using Scale = T;
    static const size_t data_count = 1;

    static inline void prepare(size_t scale, Scale & mm_scale)
    {
        mm_scale = static_cast<T>(scale);
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
class FloatRoundingComputation;

template <typename T, RoundingMode rounding_mode>
class FloatRoundingComputation<T, rounding_mode, ScaleMode::Positive>
    : public BaseFloatRoundingComputation<T>
{
public:
    static inline void compute(const T * __restrict in, const T & scale, T * __restrict out)
    {
        out[0] = roundWithMode<rounding_mode>(in[0] * scale) / scale;
    }
};

template <typename T, RoundingMode rounding_mode>
class FloatRoundingComputation<T, rounding_mode, ScaleMode::Negative>
    : public BaseFloatRoundingComputation<T>
{
public:
    static inline void compute(const T * __restrict in, const T & scale, T * __restrict out)
    {
        out[0] = roundWithMode<rounding_mode>(in[0] / scale) * scale;
    }
};

template <typename T, RoundingMode rounding_mode>
class FloatRoundingComputation<T, rounding_mode, ScaleMode::Zero>
    : public BaseFloatRoundingComputation<T>
{
public:
    static inline void prepare(size_t scale, T & mm_scale)
    {
    }

    static inline void compute(const T * __restrict in, const T & scale, T * __restrict out)
    {
        out[0] = roundWithMode<rounding_mode>(in[0]);
    }
};
#endif


/** Implementing high-level rounding functions.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode, typename Enable = void>
struct FunctionRoundingImpl;

/** Implement high-level rounding functions for integer values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
    typename std::enable_if<std::is_integral<T>::value && (scale_mode != ScaleMode::Null)>::type>
{
private:
    using Op = IntegerRoundingComputation<T, rounding_mode, scale_mode>;

public:
    static inline void apply(const PaddedPODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
    {
        const T* begin_in = &in[0];
        const T* end_in = begin_in + in.size();

        T* __restrict p_out = &out[0];
        for (const T* __restrict p_in = begin_in; p_in != end_in; ++p_in)
        {
            *p_out = Op::compute(*p_in, scale);
            ++p_out;
        }
    }

    static inline T apply(T val, size_t scale)
    {
        return Op::compute(val, scale);
    }
};

/** Implement high-level round-off functions for floating-point values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
    typename std::enable_if<std::is_floating_point<T>::value && (scale_mode != ScaleMode::Null)>::type>
{
private:
    using Op = FloatRoundingComputation<T, rounding_mode, scale_mode>;
    using Data = std::array<T, Op::data_count>;
    using Scale = typename Op::Scale;

public:
    static inline void apply(const PaddedPODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
    {
        Scale mm_scale;
        Op::prepare(scale, mm_scale);

        const size_t data_count = std::tuple_size<Data>();

        const T* begin_in = &in[0];
        const T* end_in = begin_in + in.size();

        T* begin_out = &out[0];
        const T* end_out = begin_out + out.size();

        const T* limit = begin_in + in.size() / data_count * data_count;

        const T* __restrict p_in = begin_in;
        T* __restrict p_out = begin_out;
        for (; p_in < limit; p_in += data_count)
        {
            Op::compute(p_in, mm_scale, p_out);
            p_out += data_count;
        }

        if (p_in < end_in)
        {
            Data tmp{{}};
            T* begin_tmp = &tmp[0];
            const T* end_tmp = begin_tmp + data_count;

            for (T* __restrict p_tmp = begin_tmp; (p_tmp != end_tmp) && (p_in != end_in); ++p_tmp)
            {
                *p_tmp = *p_in;
                ++p_in;
            }

            Data res;
            const T* begin_res = &res[0];
            const T* end_res = begin_res + data_count;

            Op::compute(reinterpret_cast<T *>(&tmp), mm_scale, reinterpret_cast<T *>(&res));

            for (const T* __restrict p_res = begin_res; (p_res != end_res) && (p_out != end_out); ++p_res)
            {
                *p_out = *p_res;
                ++p_out;
            }
        }
    }

    static inline T apply(T val, size_t scale)
    {
        if (val == 0)
            return val;
        else
        {
            Scale mm_scale;
            Op::prepare(scale, mm_scale);

            Data tmp{{}};
            tmp[0] = val;

            Data res;
            Op::compute(reinterpret_cast<T *>(&tmp), mm_scale, reinterpret_cast<T *>(&res));
            return res[0];
        }
    }
};

/** Implementation of high-level rounding functions in the case when a zero value is returned.
    */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
    typename std::enable_if<scale_mode == ScaleMode::Null>::type>
{
public:
    static inline void apply(const PaddedPODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
    {
        ::memset(reinterpret_cast<T *>(&out[0]), 0, in.size() * sizeof(T));
    }

    static inline T apply(T val, size_t scale)
    {
        return 0;
    }
};

/// The following code generates a table of powers of 10 during the build.

namespace
{
    /// Individual degrees of the number 10.

    template <size_t N>
    struct PowerOf10
    {
        static const size_t value = 10 * PowerOf10<N - 1>::value;
    };

    template <>
    struct PowerOf10<0>
    {
        static const size_t value = 1;
    };
}

/// Declaring and defining a container containing a table of powers of 10.

template <size_t... TArgs>
struct TableContainer
{
    static const std::array<size_t, sizeof...(TArgs)> values;
};

template <size_t... TArgs>
const std::array<size_t, sizeof...(TArgs)> TableContainer<TArgs...>::values {{ TArgs... }};

/// The generator of the first N degrees.

template <size_t N, size_t... TArgs>
struct FillArrayImpl
{
    using result = typename FillArrayImpl<N - 1, PowerOf10<N>::value, TArgs...>::result;
};

template <size_t... TArgs>
struct FillArrayImpl<0, TArgs...>
{
    using result = TableContainer<PowerOf10<0>::value, TArgs...>;
};

template <size_t N>
struct FillArray
{
    using result = typename FillArrayImpl<N - 1>::result;
};

/** This pattern defines the precision that the round/ceil/floor functions use,
  * then converts it to a value that can be used in operations of
  * multiplication and division. Therefore, it is called a scale.
  */
template <typename T, typename U, typename Enable = void>
struct ScaleForRightType;

template <typename T, typename U>
struct ScaleForRightType<T, U,
    typename std::enable_if<
        std::is_floating_point<T>::value
        && std::is_signed<U>::value>::type>
{
    static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;

        auto precision_col = checkAndGetColumnConst<ColumnVector<U>>(column.get());
        if (!precision_col)
            return false;

        U val = precision_col->template getValue<U>();
        if (val < 0)
        {
            if (val < -static_cast<U>(std::numeric_limits<T>::digits10))
            {
                scale_mode = ScaleMode::Null;
                scale = 1;
            }
            else
            {
                scale_mode = ScaleMode::Negative;
                scale = PowersOf10::values[-val];
            }
        }
        else if (val == 0)
        {
            scale_mode = ScaleMode::Zero;
            scale = 1;
        }
        else
        {
            scale_mode = ScaleMode::Positive;
            if (val > std::numeric_limits<T>::digits10)
                val = static_cast<U>(std::numeric_limits<T>::digits10);
            scale = PowersOf10::values[val];
        }

        return true;
    }
};

template <typename T, typename U>
struct ScaleForRightType<T, U,
    typename std::enable_if<
        std::is_floating_point<T>::value
        && std::is_unsigned<U>::value>::type>
{
    static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;
        auto precision_col = checkAndGetColumnConst<ColumnVector<U>>(column.get());
        if (!precision_col)
            return false;

        U val = precision_col->template getValue<U>();
        if (val == 0)
        {
            scale_mode = ScaleMode::Zero;
            scale = 1;
        }
        else
        {
            scale_mode = ScaleMode::Positive;
            if (val > static_cast<U>(std::numeric_limits<T>::digits10))
                val = static_cast<U>(std::numeric_limits<T>::digits10);
            scale = PowersOf10::values[val];
        }

        return true;
    }
};

template <typename T, typename U>
struct ScaleForRightType<T, U,
    typename std::enable_if<
        std::is_integral<T>::value
        && std::is_signed<U>::value>::type>
{
    static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;
        auto precision_col = checkAndGetColumnConst<ColumnVector<U>>(column.get());
        if (!precision_col)
            return false;

        U val = precision_col->template getValue<U>();
        if (val < 0)
        {
            if (val < -std::numeric_limits<T>::digits10)
            {
                scale_mode = ScaleMode::Null;
                scale = 1;
            }
            else
            {
                scale_mode = ScaleMode::Negative;
                scale = PowersOf10::values[-val];
            }
        }
        else
        {
            scale_mode = ScaleMode::Zero;
            scale = 1;
        }

        return true;
    }
};

template <typename T, typename U>
struct ScaleForRightType<T, U,
    typename std::enable_if<
        std::is_integral<T>::value
        && std::is_unsigned<U>::value>::type>
{
    static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        auto precision_col = checkAndGetColumnConst<ColumnVector<U>>(column.get());
        if (!precision_col)
            return false;

        scale_mode = ScaleMode::Zero;
        scale = 1;

        return true;
    }
};

/** Turn the precision parameter into a scale.
    */
template <typename T>
struct ScaleForLeftType
{
    static inline void apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        if (!( ScaleForRightType<T, UInt8>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, UInt16>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, UInt16>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, UInt32>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, UInt64>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, Int8>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, Int16>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, Int32>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, Int64>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, Float32>::apply(column, scale_mode, scale)
            || ScaleForRightType<T, Float64>::apply(column, scale_mode, scale)))
        {
            throw Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
        }
    }
};

/** The main template that applies the rounding function to a value or column.
    */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct Cruncher
{
    using Op = FunctionRoundingImpl<T, rounding_mode, scale_mode>;

    static inline void apply(Block & block, const ColumnVector<T> * col, const ColumnNumbers & arguments, size_t result, size_t scale)
    {
        auto col_res = std::make_shared<ColumnVector<T>>();
        block.getByPosition(result).column = col_res;

        typename ColumnVector<T>::Container_t & vec_res = col_res->getData();
        vec_res.resize(col->getData().size());

        if (vec_res.empty())
            return;

        Op::apply(col->getData(), scale, vec_res);
    }
};

/** Select the appropriate processing algorithm depending on the scale.
  */
template <typename T, typename ColumnType, RoundingMode rounding_mode>
struct Dispatcher
{
    static inline void apply(Block & block, const ColumnType * col, const ColumnNumbers & arguments, size_t result)
    {
        ScaleMode scale_mode;
        size_t scale;

        if (arguments.size() == 2)
            ScaleForLeftType<T>::apply(block.getByPosition(arguments[1]).column, scale_mode, scale);
        else
        {
            scale_mode = ScaleMode::Zero;
            scale = 1;
        }

        if (scale_mode == ScaleMode::Positive)
            Cruncher<T, rounding_mode, ScaleMode::Positive>::apply(block, col, arguments, result, scale);
        else if (scale_mode == ScaleMode::Zero)
            Cruncher<T, rounding_mode, ScaleMode::Zero>::apply(block, col, arguments, result, scale);
        else if (scale_mode == ScaleMode::Negative)
            Cruncher<T, rounding_mode, ScaleMode::Negative>::apply(block, col, arguments, result, scale);
        else if (scale_mode == ScaleMode::Null)
            Cruncher<T, rounding_mode, ScaleMode::Null>::apply(block, col, arguments, result, scale);
        else
            throw Exception("Illegal operation", ErrorCodes::LOGICAL_ERROR);
    }
};

/** A template for functions that round the value of an input parameter of type
    * (U)Int8/16/32/64 or Float32/64, and accept an additional optional
    * parameter (default is 0).
    */
template <typename Name, RoundingMode rounding_mode>
class FunctionRounding : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionRounding>(); }

private:
    template <typename T>
    bool checkType(const IDataType * type) const
    {
        return typeid_cast<const T *>(type);
    }

    template <typename T>
    bool executeForType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (auto col = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            Dispatcher<T, ColumnVector<T>, rounding_mode>::apply(block, col, arguments, result);
            return true;
        }
        return false;
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if ((arguments.size() < 1) || (arguments.size() > 2))
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 2)
        {
            const IDataType * type = &*arguments[1];
            if (!( checkType<DataTypeUInt8>(type)
                || checkType<DataTypeUInt16>(type)
                || checkType<DataTypeUInt32>(type)
                || checkType<DataTypeUInt64>(type)
                || checkType<DataTypeInt8>(type)
                || checkType<DataTypeInt16>(type)
                || checkType<DataTypeInt32>(type)
                || checkType<DataTypeInt64>(type)
                || checkType<DataTypeFloat32>(type)
                || checkType<DataTypeFloat64>(type)))
            {
                throw Exception("Illegal type in second argument of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        const IDataType * type = &*arguments[0];
        if (!type->behavesAsNumber())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        if (!(    executeForType<UInt8>(block, arguments, result)
            ||    executeForType<UInt16>(block, arguments, result)
            ||    executeForType<UInt32>(block, arguments, result)
            ||    executeForType<UInt64>(block, arguments, result)
            ||    executeForType<Int8>(block, arguments, result)
            ||    executeForType<Int16>(block, arguments, result)
            ||    executeForType<Int32>(block, arguments, result)
            ||    executeForType<Int64>(block, arguments, result)
            ||    executeForType<Float32>(block, arguments, result)
            ||    executeForType<Float64>(block, arguments, result)))
        {
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }
    }

    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return { true, true, true };
    }
};

struct NameRoundToExp2 { static constexpr auto name = "roundToExp2"; };
struct NameRoundDuration { static constexpr auto name = "roundDuration"; };
struct NameRoundAge { static constexpr auto name = "roundAge"; };

struct NameRound { static constexpr auto name = "round"; };
struct NameCeil { static constexpr auto name = "ceil"; };
struct NameFloor { static constexpr auto name = "floor"; };
struct NameTrunc { static constexpr auto name = "trunc"; };

using FunctionRoundToExp2 = FunctionUnaryArithmetic<RoundToExp2Impl, NameRoundToExp2, false>;
using FunctionRoundDuration = FunctionUnaryArithmetic<RoundDurationImpl, NameRoundDuration, false>;
using FunctionRoundAge = FunctionUnaryArithmetic<RoundAgeImpl, NameRoundAge, false>;

using FunctionRound = FunctionRounding<NameRound, RoundingMode::Round>;
using FunctionFloor = FunctionRounding<NameFloor, RoundingMode::Floor>;
using FunctionCeil = FunctionRounding<NameCeil, RoundingMode::Ceil>;
using FunctionTrunc = FunctionRounding<NameTrunc, RoundingMode::Trunc>;


struct PositiveMonotonicity
{
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field & left, const Field & right)
    {
        return { true };
    }
};

template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundToExp2> : PositiveMonotonicity {};
template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundDuration> : PositiveMonotonicity {};
template <> struct FunctionUnaryArithmeticMonotonicity<NameRoundAge> : PositiveMonotonicity {};

}
