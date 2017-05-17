#pragma once

#include <Functions/FunctionsArithmetic.h>
#include <IO/WriteHelpers.h>

#include <cmath>
#include <type_traits>
#include <array>

#if __SSE4_1__
    #include <smmintrin.h>
#endif


namespace DB
{

/** Функции округления:
    * roundToExp2 - вниз до ближайшей степени двойки;
    * roundDuration - вниз до ближайшего из: 0, 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000;
    * roundAge - вниз до ближайшего из: 0, 18, 25, 35, 45, 55.
    *
    * round(x, N) - арифметическое округление (N = 0 по умолчанию).
    * ceil(x, N) - наименьшее число, которое не меньше x (N = 0 по умолчанию).
    * floor(x, N) - наибольшее число, которое не больше x (N = 0 по умолчанию).
    *
    * Значение параметра N:
    * - N > 0: округлять до числа с N десятичными знаками после запятой
    * - N < 0: окурглять до целого числа с N нулевыми знаками
    * - N = 0: округлять до целого числа
    */

template<typename A>
struct RoundToExp2Impl
{
    using ResultType = A;

    static inline A apply(A x)
    {
        return x <= 0 ? static_cast<A>(0) : (static_cast<A>(1) << static_cast<UInt64>(log2(static_cast<double>(x))));
    }
};

template<>
struct RoundToExp2Impl<Float32>
{
    using ResultType = Float32;

    static inline Float32 apply(Float32 x)
    {
        return static_cast<Float32>(x < 1 ? 0. : pow(2., floor(log2(x))));
    }
};

template<>
struct RoundToExp2Impl<Float64>
{
    using ResultType = Float64;

    static inline Float64 apply(Float64 x)
    {
        return x < 1 ? 0. : pow(2., floor(log2(x)));
    }
};

template<typename A>
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

template<typename A>
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

/** Быстрое вычисление остатка от деления для применения к округлению целых чисел.
    * Без проверки, потому что делитель всегда положительный.
    */
template<typename T, typename Enable = void>
struct FastModulo;

template<typename T>
struct FastModulo<T, typename std::enable_if<std::is_integral<T>::value>::type>
{
private:
    template<typename InputType, typename Enable = void>
    struct Extend;

    template<typename InputType>
    struct Extend<InputType,
        typename std::enable_if<std::is_same<InputType, Int8>::value
            || std::is_same<InputType, Int16>::value>::type>
    {
        using Type = Int64;
    };

    template<typename InputType>
    struct Extend<InputType,
        typename std::enable_if<std::is_same<InputType, UInt8>::value
            || std::is_same<InputType, UInt16>::value>::type>
    {
        using Type = UInt64;
    };

    template<typename InputType>
    struct Extend<InputType,
        typename std::enable_if<std::is_integral<InputType>::value
            && (sizeof(InputType) >= 4)>::type>
    {
        using Type = InputType;
    };

    using U = typename Extend<T>::Type;

public:
    using Divisor = std::pair<size_t, typename libdivide::divider<U> >;

    static inline Divisor prepare(size_t b)
    {
        return std::make_pair(b, libdivide::divider<U>(b));
    }

    static inline T compute(T a, const Divisor & divisor)
    {
        U val = static_cast<U>(a);
        U rem = val - (val / divisor.second) * static_cast<U>(divisor.first);
        return static_cast<T>(rem);
    }
};

/** Этот параметр контролирует поведение функций округления.
    */
enum ScaleMode
{
    PositiveScale,    // округлять до числа с N десятичными знаками после запятой
    NegativeScale,  // окурглять до целого числа с N нулевыми знаками
    ZeroScale,        // округлять до целого числа
    NullScale         // возвращать нулевое значение
};

#if !defined(_MM_FROUND_NINT)
#define _MM_FROUND_NINT        0
#define _MM_FROUND_FLOOR     1
#define _MM_FROUND_CEIL        2
#endif

/** Реализация низкоуровневых функций округления для целочисленных значений.
    */
template<typename T, int rounding_mode, ScaleMode scale_mode, typename Enable = void>
struct IntegerRoundingComputation;

template<typename T, int rounding_mode, ScaleMode scale_mode>
struct IntegerRoundingComputation<T, rounding_mode, scale_mode,
    typename std::enable_if<std::is_integral<T>::value
        && ((scale_mode == PositiveScale) || (scale_mode == ZeroScale))>::type>
{
    using Divisor = int;

    static inline Divisor prepare(size_t scale)
    {
        return 0;
    }

    static inline T compute(T in, const Divisor & scale)
    {
        return in;
    }
};

template<typename T>
struct IntegerRoundingComputation<T, _MM_FROUND_NINT, NegativeScale,
    typename std::enable_if<std::is_integral<T>::value>::type>
{
    using Op = FastModulo<T>;
    using Divisor = typename Op::Divisor;

    static inline Divisor prepare(size_t scale)
    {
        return Op::prepare(scale);
    }

    static inline T compute(T in, const Divisor & scale)
    {
        T factor = (in < 0) ? -1 : 1;
        in *= factor;
        T rem = Op::compute(in, scale);
        in -= rem;
        T res;
        if ((2 * rem) < static_cast<T>(scale.first))
            res = in;
        else
            res = in + scale.first;
        return factor * res;
    }
};

template<typename T>
struct IntegerRoundingComputation<T, _MM_FROUND_CEIL, NegativeScale,
    typename std::enable_if<std::is_integral<T>::value>::type>
{
    using Op = FastModulo<T>;
    using Divisor = typename Op::Divisor;

    static inline Divisor prepare(size_t scale)
    {
        return Op::prepare(scale);
    }

    static inline T compute(T in, const Divisor & scale)
    {
        T factor = (in < 0) ? -1 : 1;
        in *= factor;
        T rem = Op::compute(in, scale);
        T res = in - rem + scale.first;
        return factor * res;
    }
};

template<typename T>
struct IntegerRoundingComputation<T, _MM_FROUND_FLOOR, NegativeScale,
    typename std::enable_if<std::is_integral<T>::value>::type>
{
    using Op = FastModulo<T>;
    using Divisor = typename Op::Divisor;

    static inline Divisor prepare(size_t scale)
    {
        return Op::prepare(scale);
    }

    static inline T compute(T in, const Divisor & scale)
    {
        T factor = (in < 0) ? -1 : 1;
        in *= factor;
        T rem = Op::compute(in, scale);
        T res = in - rem;
        return factor * res;
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
    /// Предотвратить появление отрицательных нолей определённых в стандарте IEEE-754.
    static inline void normalize(__m128 & val, const __m128 & mask)
    {
        __m128 mask1 = _mm_cmpeq_ps(val, getZero());
        __m128 mask2 = _mm_and_ps(mask, mask1);
        mask2 = _mm_cmpeq_ps(mask2, getZero());
        mask2 = _mm_min_ps(mask2, getTwo());
        mask2 = _mm_sub_ps(mask2, getOne());
        val = _mm_mul_ps(val, mask2);
    }

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
    /// Предотвратить появление отрицательных нолей определённых в стандарте IEEE-754.
    static inline void normalize(__m128d & val, const __m128d & mask)
    {
        __m128d mask1 = _mm_cmpeq_pd(val, getZero());
        __m128d mask2 = _mm_and_pd(mask, mask1);
        mask2 = _mm_cmpeq_pd(mask2, getZero());
        mask2 = _mm_min_pd(mask2, getTwo());
        mask2 = _mm_sub_pd(mask2, getOne());
        val = _mm_mul_pd(val, mask2);
    }

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

/** Реализация низкоуровневых функций округления для значений с плавающей точкой.
    */
template<typename T, int rounding_mode, ScaleMode scale_mode>
class FloatRoundingComputation;

template<int rounding_mode>
class FloatRoundingComputation<Float32, rounding_mode, PositiveScale>
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
        __m128 mask = _mm_cmplt_ps(val, getZero());

        /// Алгоритм округления.
        val = _mm_mul_ps(val, scale);
        val = _mm_round_ps(val, rounding_mode);
        val = _mm_div_ps(val, scale);

        normalize(val, mask);
        _mm_storeu_ps(out, val);
    }
};

template<int rounding_mode>
class FloatRoundingComputation<Float32, rounding_mode, NegativeScale>
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
        __m128 mask = _mm_cmplt_ps(val, getZero());

        /// Превратить отрицательные значения в положительные.
        __m128 factor = _mm_cmpge_ps(val, getZero());
        factor = _mm_min_ps(factor, getTwo());
        factor = _mm_sub_ps(factor, getOne());
        val = _mm_mul_ps(val, factor);

        /// Алгоритм округления.
        val = _mm_div_ps(val, scale);
        __m128 res = _mm_cmpge_ps(val, getOneTenth());
        val = _mm_round_ps(val, rounding_mode);
        val = _mm_mul_ps(val, scale);
        val = _mm_and_ps(val, res);

        /// Вернуть настоящие знаки всех значений.
        val = _mm_mul_ps(val, factor);

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

template<int rounding_mode>
class FloatRoundingComputation<Float32, rounding_mode, ZeroScale>
    : public BaseFloatRoundingComputation<Float32>
{
public:
    static inline void prepare(size_t scale, Scale & mm_scale)
    {
    }

    static inline void compute(const Float32 * __restrict in, const Scale & scale, Float32 * __restrict out)
    {
        __m128 val = _mm_loadu_ps(in);
        __m128 mask = _mm_cmplt_ps(val, getZero());

        val = _mm_round_ps(val, rounding_mode);

        normalize(val, mask);
        _mm_storeu_ps(out, val);
    }
};

template<int rounding_mode>
class FloatRoundingComputation<Float64, rounding_mode, PositiveScale>
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
        __m128d mask = _mm_cmplt_pd(val, getZero());

        /// Алгоритм округления.
        val = _mm_mul_pd(val, scale);
        val = _mm_round_pd(val, rounding_mode);
        val = _mm_div_pd(val, scale);

        normalize(val, mask);
        _mm_storeu_pd(out, val);
    }
};

template<int rounding_mode>
class FloatRoundingComputation<Float64, rounding_mode, NegativeScale>
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
        __m128d mask = _mm_cmplt_pd(val, getZero());

        /// Превратить отрицательные значения в положительные.
        __m128d factor = _mm_cmpge_pd(val, getZero());
        factor = _mm_min_pd(factor, getTwo());
        factor = _mm_sub_pd(factor, getOne());
        val = _mm_mul_pd(val, factor);

        /// Алгоритм округления.
        val = _mm_div_pd(val, scale);
        __m128d res = _mm_cmpge_pd(val, getOneTenth());
        val = _mm_round_pd(val, rounding_mode);
        val = _mm_mul_pd(val, scale);
        val = _mm_and_pd(val, res);

        /// Вернуть настоящие знаки всех значений.
        val = _mm_mul_pd(val, factor);

        normalize(val, mask);
        _mm_storeu_pd(out, val);
    }

private:
    static inline const __m128d & getOneTenth()
    {
        static const __m128d one_tenth = _mm_set1_pd(0.1);
        return one_tenth;
    }
};

template<int rounding_mode>
class FloatRoundingComputation<Float64, rounding_mode, ZeroScale>
    : public BaseFloatRoundingComputation<Float64>
{
public:
    static inline void prepare(size_t scale, Scale & mm_scale)
    {
    }

    static inline void compute(const Float64 * __restrict in, const Scale & scale, Float64 * __restrict out)
    {
        __m128d val = _mm_loadu_pd(in);
        __m128d mask = _mm_cmplt_pd(val, getZero());

        val = _mm_round_pd(val, rounding_mode);

        normalize(val, mask);
        _mm_storeu_pd(out, val);
    }
};
#else
/// Реализация для ARM. Не векторизована. Не исправляет отрицательные нули.

template <int mode>
float roundWithMode(float x)
{
    if (mode == _MM_FROUND_NINT)     return roundf(x);
    if (mode == _MM_FROUND_FLOOR)     return floorf(x);
    if (mode == _MM_FROUND_CEIL)     return ceilf(x);
    __builtin_unreachable();
}

template <int mode>
double roundWithMode(double x)
{
    if (mode == _MM_FROUND_NINT)     return round(x);
    if (mode == _MM_FROUND_FLOOR)     return floor(x);
    if (mode == _MM_FROUND_CEIL)     return ceil(x);
    __builtin_unreachable();
}

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

template <typename T, int rounding_mode, ScaleMode scale_mode>
class FloatRoundingComputation;

template <typename T, int rounding_mode>
class FloatRoundingComputation<T, rounding_mode, PositiveScale>
    : public BaseFloatRoundingComputation<T>
{
public:
    static inline void compute(const T * __restrict in, const T & scale, T * __restrict out)
    {
        out[0] = roundWithMode<rounding_mode>(in[0] * scale) / scale;
    }
};

template <typename T, int rounding_mode>
class FloatRoundingComputation<T, rounding_mode, NegativeScale>
    : public BaseFloatRoundingComputation<T>
{
public:
    static inline void compute(const T * __restrict in, const T & scale, T * __restrict out)
    {
        out[0] = roundWithMode<rounding_mode>(in[0] / scale) * scale;
    }
};

template <typename T, int rounding_mode>
class FloatRoundingComputation<T, rounding_mode, ZeroScale>
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


/** Реализация высокоуровневых функций округления.
    */
template<typename T, int rounding_mode, ScaleMode scale_mode, typename Enable = void>
struct FunctionRoundingImpl;

/** Реализация высокоуровневых функций округления для целочисленных значений.
    */
template<typename T, int rounding_mode, ScaleMode scale_mode>
struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
    typename std::enable_if<std::is_integral<T>::value && (scale_mode != NullScale)>::type>
{
private:
    using Op = IntegerRoundingComputation<T, rounding_mode, scale_mode>;

public:
    static inline void apply(const PaddedPODArray<T> & in, size_t scale, typename ColumnVector<T>::Container_t & out)
    {
        auto divisor = Op::prepare(scale);

        const T* begin_in = &in[0];
        const T* end_in = begin_in + in.size();

        T* __restrict p_out = &out[0];
        for (const T* __restrict p_in = begin_in; p_in != end_in; ++p_in)
        {
            *p_out = Op::compute(*p_in, divisor);
            ++p_out;
        }
    }

    static inline T apply(T val, size_t scale)
    {
        auto divisor = Op::prepare(scale);
        return Op::compute(val, divisor);
    }
};

/** Реализация высокоуровневых функций округления для значений с плавающей точкой.
    */
template<typename T, int rounding_mode, ScaleMode scale_mode>
struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
    typename std::enable_if<std::is_floating_point<T>::value && (scale_mode != NullScale)>::type>
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

/** Реализация высокоуровневых функций округления в том случае, когда возвращается нулевое значение.
    */
template<typename T, int rounding_mode, ScaleMode scale_mode>
struct FunctionRoundingImpl<T, rounding_mode, scale_mode,
    typename std::enable_if<scale_mode == NullScale>::type>
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

/// Следующий код генерирует во время сборки таблицу степеней числа 10.

namespace
{
    /// Отдельные степени числа 10.

    template<size_t N>
    struct PowerOf10
    {
        static const size_t value = 10 * PowerOf10<N - 1>::value;
    };

    template<>
    struct PowerOf10<0>
    {
        static const size_t value = 1;
    };
}

/// Объявление и определение контейнера содержащего таблицу степеней числа 10.

template<size_t... TArgs>
struct TableContainer
{
    static const std::array<size_t, sizeof...(TArgs)> values;
};

template<size_t... TArgs>
const std::array<size_t, sizeof...(TArgs)> TableContainer<TArgs...>::values {{ TArgs... }};

/// Генератор первых N степеней.

template<size_t N, size_t... TArgs>
struct FillArrayImpl
{
    using result = typename FillArrayImpl<N - 1, PowerOf10<N>::value, TArgs...>::result;
};

template<size_t... TArgs>
struct FillArrayImpl<0, TArgs...>
{
    using result = TableContainer<PowerOf10<0>::value, TArgs...>;
};

template<size_t N>
struct FillArray
{
    using result = typename FillArrayImpl<N - 1>::result;
};

/** Этот шаблон определяет точность, которую используют функции round/ceil/floor,
    * затем  преобразовывает её в значение, которое можно использовать в операциях
    * умножения и деления. Поэтому оно называется масштабом.
    */
template<typename T, typename U, typename Enable = void>
struct ScaleForRightType;

template<typename T, typename U>
struct ScaleForRightType<T, U,
    typename std::enable_if<
        std::is_floating_point<T>::value
        && std::is_signed<U>::value>::type>
{
    static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;
        using ColumnType = ColumnConst<U>;

        const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
        if (precision_col == nullptr)
            return false;

        U val = precision_col->getData();
        if (val < 0)
        {
            if (val < -static_cast<U>(std::numeric_limits<T>::digits10))
            {
                scale_mode = NullScale;
                scale = 1;
            }
            else
            {
                scale_mode = NegativeScale;
                scale = PowersOf10::values[-val];
            }
        }
        else if (val == 0)
        {
            scale_mode = ZeroScale;
            scale = 1;
        }
        else
        {
            scale_mode = PositiveScale;
            if (val > std::numeric_limits<T>::digits10)
                val = static_cast<U>(std::numeric_limits<T>::digits10);
            scale = PowersOf10::values[val];
        }

        return true;
    }
};

template<typename T, typename U>
struct ScaleForRightType<T, U,
    typename std::enable_if<
        std::is_floating_point<T>::value
        && std::is_unsigned<U>::value>::type>
{
    static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;
        using ColumnType = ColumnConst<U>;

        const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
        if (precision_col == nullptr)
            return false;

        U val = precision_col->getData();
        if (val == 0)
        {
            scale_mode = ZeroScale;
            scale = 1;
        }
        else
        {
            scale_mode = PositiveScale;
            if (val > static_cast<U>(std::numeric_limits<T>::digits10))
                val = static_cast<U>(std::numeric_limits<T>::digits10);
            scale = PowersOf10::values[val];
        }

        return true;
    }
};

template<typename T, typename U>
struct ScaleForRightType<T, U,
    typename std::enable_if<
        std::is_integral<T>::value
        && std::is_signed<U>::value>::type>
{
    static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        using PowersOf10 = typename FillArray<std::numeric_limits<T>::digits10 + 1>::result;
        using ColumnType = ColumnConst<U>;

        const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
        if (precision_col == nullptr)
                return false;

        U val = precision_col->getData();
        if (val < 0)
        {
            if (val < -std::numeric_limits<T>::digits10)
            {
                scale_mode = NullScale;
                scale = 1;
            }
            else
            {
                scale_mode = NegativeScale;
                scale = PowersOf10::values[-val];
            }
        }
        else
        {
            scale_mode = ZeroScale;
            scale = 1;
        }

        return true;
    }
};

template<typename T, typename U>
struct ScaleForRightType<T, U,
    typename std::enable_if<
        std::is_integral<T>::value
        && std::is_unsigned<U>::value>::type>
{
    static inline bool apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        using ColumnType = ColumnConst<U>;

        const ColumnType * precision_col = typeid_cast<const ColumnType *>(&*column);
        if (precision_col == nullptr)
            return false;

        scale_mode = ZeroScale;
        scale = 1;

        return true;
    }
};

/** Превратить параметр точности в масштаб.
    */
template<typename T>
struct ScaleForLeftType
{
    static inline void apply(const ColumnPtr & column, ScaleMode & scale_mode, size_t & scale)
    {
        if (!(    ScaleForRightType<T, UInt8>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, UInt16>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, UInt16>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, UInt32>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, UInt64>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, Int8>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, Int16>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, Int32>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, Int64>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, Float32>::apply(column, scale_mode, scale)
            ||    ScaleForRightType<T, Float64>::apply(column, scale_mode, scale)))
        {
            throw Exception("Internal error", ErrorCodes::LOGICAL_ERROR);
        }
    }
};

/** Главный шаблон применяющий функцию округления к значению или столбцу.
    */
template<typename T, int rounding_mode, ScaleMode scale_mode>
struct Cruncher
{
    using Op = FunctionRoundingImpl<T, rounding_mode, scale_mode>;

    static inline void apply(Block & block, ColumnVector<T> * col, const ColumnNumbers & arguments, size_t result, size_t scale)
    {
        auto col_res = std::make_shared<ColumnVector<T>>();
        block.safeGetByPosition(result).column = col_res;

        typename ColumnVector<T>::Container_t & vec_res = col_res->getData();
        vec_res.resize(col->getData().size());

        if (vec_res.empty())
            return;

        Op::apply(col->getData(), scale, vec_res);
    }

    static inline void apply(Block & block, ColumnConst<T> * col, const ColumnNumbers & arguments, size_t result, size_t scale)
    {
        T res = Op::apply(col->getData(), scale);
        auto col_res = std::make_shared<ColumnConst<T>>(col->size(), res);
        block.safeGetByPosition(result).column = col_res;
    }
};

/** Выбрать подходящий алгоритм обработки в зависимости от масштаба.
    */
template<typename T, template <typename> class U, int rounding_mode>
struct Dispatcher
{
    static inline void apply(Block & block, U<T> * col, const ColumnNumbers & arguments, size_t result)
    {
        ScaleMode scale_mode;
        size_t scale;

        if (arguments.size() == 2)
            ScaleForLeftType<T>::apply(block.safeGetByPosition(arguments[1]).column, scale_mode, scale);
        else
        {
            scale_mode = ZeroScale;
            scale = 1;
        }

        if (scale_mode == PositiveScale)
            Cruncher<T, rounding_mode, PositiveScale>::apply(block, col, arguments, result, scale);
        else if (scale_mode == ZeroScale)
            Cruncher<T, rounding_mode, ZeroScale>::apply(block, col, arguments, result, scale);
        else if (scale_mode == NegativeScale)
            Cruncher<T, rounding_mode, NegativeScale>::apply(block, col, arguments, result, scale);
        else if (scale_mode == NullScale)
            Cruncher<T, rounding_mode, NullScale>::apply(block, col, arguments, result, scale);
        else
            throw Exception("Illegal operation", ErrorCodes::LOGICAL_ERROR);
    }
};

/** Шаблон для функций, которые округляют значение входного параметра типа
    * (U)Int8/16/32/64 или Float32/64, и принимают дополнительный необязятельный
    * параметр (по умолчанию - 0).
    */
template<typename Name, int rounding_mode>
class FunctionRounding : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionRounding>(); }

private:
    template<typename T>
    bool checkType(const IDataType * type) const
    {
        return typeid_cast<const T *>(type) != nullptr;
    }

    template<typename T>
    bool executeForType(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (ColumnVector<T> * col = typeid_cast<ColumnVector<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            Dispatcher<T, ColumnVector, rounding_mode>::apply(block, col, arguments, result);
            return true;
        }
        else if (ColumnConst<T> * col = typeid_cast<ColumnConst<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
        {
            Dispatcher<T, ColumnConst, rounding_mode>::apply(block, col, arguments, result);
            return true;
        }
        else
            return false;
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Получить типы результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
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
            throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
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
        return { true };
    }
};

struct NameRoundToExp2        { static constexpr auto name = "roundToExp2"; };
struct NameRoundDuration    { static constexpr auto name = "roundDuration"; };
struct NameRoundAge         { static constexpr auto name = "roundAge"; };
struct NameRound            { static constexpr auto name = "round"; };
struct NameCeil                { static constexpr auto name = "ceil"; };
struct NameFloor            { static constexpr auto name = "floor"; };

using FunctionRoundToExp2 = FunctionUnaryArithmetic<RoundToExp2Impl, NameRoundToExp2, false>;
using FunctionRoundDuration = FunctionUnaryArithmetic<RoundDurationImpl, NameRoundDuration, false>;
using FunctionRoundAge = FunctionUnaryArithmetic<RoundAgeImpl, NameRoundAge, false>;

using FunctionRound = FunctionRounding<NameRound, _MM_FROUND_NINT>;
using FunctionFloor = FunctionRounding<NameFloor, _MM_FROUND_FLOOR>;
using FunctionCeil = FunctionRounding<NameCeil, _MM_FROUND_CEIL>;


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
