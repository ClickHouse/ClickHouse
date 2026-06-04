#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Common/TargetSpecific.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <cmath>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

#if USE_MULTITARGET_CODE
/// Widen 16 packed `BFloat16` to `Float32` without AVX512-BF16: a `BFloat16` is the upper 16 bits of the
/// corresponding `Float32`, so zero-extend each 16-bit value to 32 bits and shift it into the high half.
/// Uses only AVX-512F/BW, so it is available at `x86-64-v4` (not just AVX512-BF16 CPUs), and it is faster
/// than the native AVX512-BF16 convert / `dpbf16` instructions, which are throughput-limited.
X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static inline __m512 loadBFloat16AsFloat32(const BFloat16 * p)
{
    const __m256i raw = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(p));
    return _mm512_castsi512_ps(_mm512_slli_epi32(_mm512_cvtepu16_epi32(raw), 16));
}
#endif

struct L1Distance
{
    static constexpr auto name = "L1";

    struct ConstParams {};

    template <typename FloatType>
    struct State
    {
        FloatType sum{};
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y, const ConstParams &)
    {
        state.sum += std::fabs(x - y);
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state, const ConstParams &)
    {
        state.sum += other_state.sum;
    }

#if USE_MULTITARGET_CODE
    template <typename ResultType>
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineF32F64(
        const ResultType * __restrict data_x,
        const ResultType * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<ResultType> & state)
    {
        static constexpr bool is_float32 = std::is_same_v<ResultType, Float32>;
        constexpr size_t n = sizeof(__m512) / sizeof(ResultType);

        /// `abs(x - y)` = clear the sign bit (the mask -0.0 has only the sign bit set), then sum.
        /// Four independent accumulators break the `add` dependency chain so the kernel is throughput-
        /// rather than latency-bound: a single accumulator regresses on CPUs with deeper FP-add latency.
        if constexpr (is_float32)
        {
            const __m512 sign = _mm512_set1_ps(-0.0f);
            __m512 a0 = _mm512_setzero_ps(), a1 = _mm512_setzero_ps(), a2 = _mm512_setzero_ps(), a3 = _mm512_setzero_ps();
            for (; i_x + 4 * n < i_max; i_x += 4 * n, i_y += 4 * n)
            {
                a0 = _mm512_add_ps(a0, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x),         _mm512_loadu_ps(data_y + i_y))));
                a1 = _mm512_add_ps(a1, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x + n),     _mm512_loadu_ps(data_y + i_y + n))));
                a2 = _mm512_add_ps(a2, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x + 2 * n), _mm512_loadu_ps(data_y + i_y + 2 * n))));
                a3 = _mm512_add_ps(a3, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x + 3 * n), _mm512_loadu_ps(data_y + i_y + 3 * n))));
            }
            __m512 sums = _mm512_add_ps(_mm512_add_ps(a0, a1), _mm512_add_ps(a2, a3));
            for (; i_x + n < i_max; i_x += n, i_y += n)
                sums = _mm512_add_ps(sums, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x), _mm512_loadu_ps(data_y + i_y))));
            state.sum = _mm512_reduce_add_ps(sums);
        }
        else
        {
            const __m512d sign = _mm512_set1_pd(-0.0);
            __m512d a0 = _mm512_setzero_pd(), a1 = _mm512_setzero_pd(), a2 = _mm512_setzero_pd(), a3 = _mm512_setzero_pd();
            for (; i_x + 4 * n < i_max; i_x += 4 * n, i_y += 4 * n)
            {
                a0 = _mm512_add_pd(a0, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x),         _mm512_loadu_pd(data_y + i_y))));
                a1 = _mm512_add_pd(a1, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x + n),     _mm512_loadu_pd(data_y + i_y + n))));
                a2 = _mm512_add_pd(a2, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x + 2 * n), _mm512_loadu_pd(data_y + i_y + 2 * n))));
                a3 = _mm512_add_pd(a3, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x + 3 * n), _mm512_loadu_pd(data_y + i_y + 3 * n))));
            }
            __m512d sums = _mm512_add_pd(_mm512_add_pd(a0, a1), _mm512_add_pd(a2, a3));
            for (; i_x + n < i_max; i_x += n, i_y += n)
                sums = _mm512_add_pd(sums, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x), _mm512_loadu_pd(data_y + i_y))));
            state.sum = _mm512_reduce_add_pd(sums);
        }
    }

    /// `BFloat16` inputs widened to `Float32` on the fly (see `loadBFloat16AsFloat32`). No AVX512-BF16
    /// instruction is needed for `L1`, so this runs at `x86-64-v4` rather than `sapphirerapids`.
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineBF16V4(
        const BFloat16 * __restrict data_x,
        const BFloat16 * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<Float32> & state)
    {
        constexpr size_t n = sizeof(__m512) / sizeof(Float32);
        const __m512 sign = _mm512_set1_ps(-0.0f);
        __m512 a0 = _mm512_setzero_ps(), a1 = _mm512_setzero_ps(), a2 = _mm512_setzero_ps(), a3 = _mm512_setzero_ps();
        for (; i_x + 4 * n < i_max; i_x += 4 * n, i_y += 4 * n)
        {
            a0 = _mm512_add_ps(a0, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x),         loadBFloat16AsFloat32(data_y + i_y))));
            a1 = _mm512_add_ps(a1, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x + n),     loadBFloat16AsFloat32(data_y + i_y + n))));
            a2 = _mm512_add_ps(a2, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x + 2 * n), loadBFloat16AsFloat32(data_y + i_y + 2 * n))));
            a3 = _mm512_add_ps(a3, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x + 3 * n), loadBFloat16AsFloat32(data_y + i_y + 3 * n))));
        }
        __m512 sums = _mm512_add_ps(_mm512_add_ps(a0, a1), _mm512_add_ps(a2, a3));
        for (; i_x + n < i_max; i_x += n, i_y += n)
            sums = _mm512_add_ps(sums, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x), loadBFloat16AsFloat32(data_y + i_y))));
        state.sum = _mm512_reduce_add_ps(sums);
    }
#endif

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams &)
    {
        return state.sum;
    }
};

struct L2Distance
{
    static constexpr auto name = "L2";

    struct ConstParams {};

    template <typename FloatType>
    struct State
    {
        FloatType sum{};
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y, const ConstParams &)
    {
        state.sum += (x - y) * (x - y);
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state, const ConstParams &)
    {
        state.sum += other_state.sum;
    }

#if USE_MULTITARGET_CODE
    template <typename ResultType>
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineF32F64(
        const ResultType * __restrict data_x,
        const ResultType * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<ResultType> & state)
    {
        static constexpr bool is_float32 = std::is_same_v<ResultType, Float32>;

        __m512 sums;
        if constexpr (is_float32)
            sums = _mm512_setzero_ps();
        else
            sums = _mm512_setzero_pd();

        constexpr size_t n = sizeof(__m512) / sizeof(ResultType);

        for (; i_x + n < i_max; i_x += n, i_y += n)
        {
            if constexpr (is_float32)
            {
                __m512 x = _mm512_loadu_ps(data_x + i_x);
                __m512 y = _mm512_loadu_ps(data_y + i_y);
                __m512 differences = _mm512_sub_ps(x, y);
                sums = _mm512_fmadd_ps(differences, differences, sums);
            }
            else
            {
                __m512 x = _mm512_loadu_pd(data_x + i_x);
                __m512 y = _mm512_loadu_pd(data_y + i_y);
                __m512 differences = _mm512_sub_pd(x, y);
                sums = _mm512_fmadd_pd(differences, differences, sums);
            }
        }

        if constexpr (is_float32)
            state.sum = _mm512_reduce_add_ps(sums);
        else
            state.sum = _mm512_reduce_add_pd(sums);
    }

    /// `BFloat16` inputs widened to `Float32` on the fly (see `loadBFloat16AsFloat32`), then the usual
    /// `Float32` FMA path. Used on all `x86-64-v4` CPUs: it is faster than the native AVX512-BF16
    /// instructions (which are throughput-limited) even on `sapphirerapids` where those are available.
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineBF16V4(
        const BFloat16 * __restrict data_x,
        const BFloat16 * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<Float32> & state)
    {
        __m512 sums = _mm512_setzero_ps();
        constexpr size_t n = sizeof(__m512) / sizeof(Float32);
        for (; i_x + n < i_max; i_x += n, i_y += n)
        {
            __m512 differences = _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x), loadBFloat16AsFloat32(data_y + i_y));
            sums = _mm512_fmadd_ps(differences, differences, sums);
        }
        state.sum = _mm512_reduce_add_ps(sums);
    }
#endif

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams &)
    {
        return std::sqrt(state.sum);
    }
};

struct L2SquaredDistance : L2Distance
{
    static constexpr auto name = "L2Squared";

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams &)
    {
        return state.sum;
    }
};

struct LpDistance
{
    static constexpr auto name = "Lp";

    struct ConstParams
    {
        Float64 power;
        Float64 inverted_power;
    };

    template <typename FloatType>
    struct State
    {
        FloatType sum{};
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y, const ConstParams & params)
    {
        state.sum += static_cast<ResultType>(std::pow(static_cast<double>(std::fabs(x - y)), params.power));
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state, const ConstParams &)
    {
        state.sum += other_state.sum;
    }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams & params)
    {
        return static_cast<ResultType>(std::pow(static_cast<double>(state.sum), params.inverted_power));
    }
};

struct LinfDistance
{
    static constexpr auto name = "Linf";

    struct ConstParams {};

    template <typename FloatType>
    struct State
    {
        FloatType dist{};
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y, const ConstParams &)
    {
        state.dist = std::fmax(state.dist, std::fabs(x - y));
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state, const ConstParams &)
    {
        state.dist = std::fmax(state.dist, other_state.dist);
    }

#if USE_MULTITARGET_CODE
    template <typename ResultType>
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineF32F64(
        const ResultType * __restrict data_x,
        const ResultType * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<ResultType> & state)
    {
        static constexpr bool is_float32 = std::is_same_v<ResultType, Float32>;
        constexpr size_t n = sizeof(__m512) / sizeof(ResultType);

        /// `max(|x - y|)`. Four independent max-accumulators break the dependency chain (latency-bound
        /// with one). `|x - y|` clears the sign bit; running maxima start at 0 (distances are non-negative).
        if constexpr (is_float32)
        {
            const __m512 sign = _mm512_set1_ps(-0.0f);
            __m512 a0 = _mm512_setzero_ps(), a1 = _mm512_setzero_ps(), a2 = _mm512_setzero_ps(), a3 = _mm512_setzero_ps();
            for (; i_x + 4 * n < i_max; i_x += 4 * n, i_y += 4 * n)
            {
                a0 = _mm512_max_ps(a0, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x),         _mm512_loadu_ps(data_y + i_y))));
                a1 = _mm512_max_ps(a1, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x + n),     _mm512_loadu_ps(data_y + i_y + n))));
                a2 = _mm512_max_ps(a2, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x + 2 * n), _mm512_loadu_ps(data_y + i_y + 2 * n))));
                a3 = _mm512_max_ps(a3, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x + 3 * n), _mm512_loadu_ps(data_y + i_y + 3 * n))));
            }
            __m512 m = _mm512_max_ps(_mm512_max_ps(a0, a1), _mm512_max_ps(a2, a3));
            for (; i_x + n < i_max; i_x += n, i_y += n)
                m = _mm512_max_ps(m, _mm512_andnot_ps(sign, _mm512_sub_ps(_mm512_loadu_ps(data_x + i_x), _mm512_loadu_ps(data_y + i_y))));
            state.dist = std::fmax(state.dist, _mm512_reduce_max_ps(m));
        }
        else
        {
            const __m512d sign = _mm512_set1_pd(-0.0);
            __m512d a0 = _mm512_setzero_pd(), a1 = _mm512_setzero_pd(), a2 = _mm512_setzero_pd(), a3 = _mm512_setzero_pd();
            for (; i_x + 4 * n < i_max; i_x += 4 * n, i_y += 4 * n)
            {
                a0 = _mm512_max_pd(a0, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x),         _mm512_loadu_pd(data_y + i_y))));
                a1 = _mm512_max_pd(a1, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x + n),     _mm512_loadu_pd(data_y + i_y + n))));
                a2 = _mm512_max_pd(a2, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x + 2 * n), _mm512_loadu_pd(data_y + i_y + 2 * n))));
                a3 = _mm512_max_pd(a3, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x + 3 * n), _mm512_loadu_pd(data_y + i_y + 3 * n))));
            }
            __m512d m = _mm512_max_pd(_mm512_max_pd(a0, a1), _mm512_max_pd(a2, a3));
            for (; i_x + n < i_max; i_x += n, i_y += n)
                m = _mm512_max_pd(m, _mm512_andnot_pd(sign, _mm512_sub_pd(_mm512_loadu_pd(data_x + i_x), _mm512_loadu_pd(data_y + i_y))));
            state.dist = std::fmax(state.dist, _mm512_reduce_max_pd(m));
        }
    }

    /// `BFloat16` inputs widened to `Float32` on the fly (see `loadBFloat16AsFloat32`). No AVX512-BF16
    /// instruction is needed for `Linf`, so this runs at `x86-64-v4` rather than `sapphirerapids`.
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineBF16V4(
        const BFloat16 * __restrict data_x,
        const BFloat16 * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<Float32> & state)
    {
        constexpr size_t n = sizeof(__m512) / sizeof(Float32);
        const __m512 sign = _mm512_set1_ps(-0.0f);
        __m512 a0 = _mm512_setzero_ps(), a1 = _mm512_setzero_ps(), a2 = _mm512_setzero_ps(), a3 = _mm512_setzero_ps();
        for (; i_x + 4 * n < i_max; i_x += 4 * n, i_y += 4 * n)
        {
            a0 = _mm512_max_ps(a0, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x),         loadBFloat16AsFloat32(data_y + i_y))));
            a1 = _mm512_max_ps(a1, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x + n),     loadBFloat16AsFloat32(data_y + i_y + n))));
            a2 = _mm512_max_ps(a2, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x + 2 * n), loadBFloat16AsFloat32(data_y + i_y + 2 * n))));
            a3 = _mm512_max_ps(a3, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x + 3 * n), loadBFloat16AsFloat32(data_y + i_y + 3 * n))));
        }
        __m512 m = _mm512_max_ps(_mm512_max_ps(a0, a1), _mm512_max_ps(a2, a3));
        for (; i_x + n < i_max; i_x += n, i_y += n)
            m = _mm512_max_ps(m, _mm512_andnot_ps(sign, _mm512_sub_ps(loadBFloat16AsFloat32(data_x + i_x), loadBFloat16AsFloat32(data_y + i_y))));
        state.dist = std::fmax(state.dist, _mm512_reduce_max_ps(m));
    }
#endif

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams &)
    {
        return state.dist;
    }
};

struct CosineDistance
{
    static constexpr auto name = "Cosine";

    struct ConstParams {};

    template <typename FloatType>
    struct State
    {
        FloatType dot_prod{};
        FloatType x_squared{};
        FloatType y_squared{};
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y, const ConstParams &)
    {
        state.dot_prod += x * y;
        state.x_squared += x * x;
        state.y_squared += y * y;
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state, const ConstParams &)
    {
        state.dot_prod += other_state.dot_prod;
        state.x_squared += other_state.x_squared;
        state.y_squared += other_state.y_squared;
    }

#if USE_MULTITARGET_CODE
    template <typename ResultType>
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineF32F64(
        const ResultType * __restrict data_x,
        const ResultType * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<ResultType> & state)
    {
        static constexpr bool is_float32 = std::is_same_v<ResultType, Float32>;

        __m512 dot_products;
        __m512 x_squareds;
        __m512 y_squareds;

        if constexpr (is_float32)
        {
            dot_products = _mm512_setzero_ps();
            x_squareds = _mm512_setzero_ps();
            y_squareds = _mm512_setzero_ps();
        }
        else
        {
            dot_products = _mm512_setzero_pd();
            x_squareds = _mm512_setzero_pd();
            y_squareds = _mm512_setzero_pd();
        }

        constexpr size_t n = sizeof(__m512) / sizeof(ResultType);

        for (; i_x + n < i_max; i_x += n, i_y += n)
        {
            if constexpr (is_float32)
            {
                __m512 x = _mm512_loadu_ps(data_x + i_x);
                __m512 y = _mm512_loadu_ps(data_y + i_y);
                dot_products = _mm512_fmadd_ps(x, y, dot_products);
                x_squareds = _mm512_fmadd_ps(x, x, x_squareds);
                y_squareds = _mm512_fmadd_ps(y, y, y_squareds);
            }
            else
            {
                __m512 x = _mm512_loadu_pd(data_x + i_x);
                __m512 y = _mm512_loadu_pd(data_y + i_y);
                dot_products = _mm512_fmadd_pd(x, y, dot_products);
                x_squareds = _mm512_fmadd_pd(x, x, x_squareds);
                y_squareds = _mm512_fmadd_pd(y, y, y_squareds);
            }
        }

        if constexpr (is_float32)
        {
            state.dot_prod = _mm512_reduce_add_ps(dot_products);
            state.x_squared = _mm512_reduce_add_ps(x_squareds);
            state.y_squared = _mm512_reduce_add_ps(y_squareds);
        }
        else
        {
            state.dot_prod = _mm512_reduce_add_pd(dot_products);
            state.x_squared = _mm512_reduce_add_pd(x_squareds);
            state.y_squared = _mm512_reduce_add_pd(y_squareds);
        }
    }

    /// `BFloat16` inputs widened to `Float32` on the fly (see `loadBFloat16AsFloat32`), then the usual
    /// `Float32` FMA path. Used on all `x86-64-v4` CPUs: it is faster than the native AVX512-BF16
    /// `dpbf16` instruction (which is throughput-limited) even on `sapphirerapids` where it is available.
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineBF16V4(
        const BFloat16 * __restrict data_x,
        const BFloat16 * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<Float32> & state)
    {
        __m512 dot_products = _mm512_setzero_ps();
        __m512 x_squareds = _mm512_setzero_ps();
        __m512 y_squareds = _mm512_setzero_ps();
        constexpr size_t n = sizeof(__m512) / sizeof(Float32);
        for (; i_x + n < i_max; i_x += n, i_y += n)
        {
            __m512 x = loadBFloat16AsFloat32(data_x + i_x);
            __m512 y = loadBFloat16AsFloat32(data_y + i_y);
            dot_products = _mm512_fmadd_ps(x, y, dot_products);
            x_squareds = _mm512_fmadd_ps(x, x, x_squareds);
            y_squareds = _mm512_fmadd_ps(y, y, y_squareds);
        }
        state.dot_prod = _mm512_reduce_add_ps(dot_products);
        state.x_squared = _mm512_reduce_add_ps(x_squareds);
        state.y_squared = _mm512_reduce_add_ps(y_squareds);
    }
#endif

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams &)
    {
        return ResultType{1} - state.dot_prod / std::sqrt(state.x_squared * state.y_squared);
    }
};

#if USE_MULTITARGET_CODE
/// Generic v4 distance kernel for integer inputs (widened to `Float64`). Unlike the hand-written float and
/// BFloat16 kernels, this leans on the auto-vectorizer: the per-lane independent-accumulator loop and the
/// `static_cast<ResultType>` integer->double widening both vectorize under the `x86-64-v4` target attribute,
/// while the final horizontal `combine` reduction stays scalar (strict FP forbids reassociating it). This
/// brings integer distances from the file's `x86-64-v2` baseline up to AVX-512 without a per-type kernel.
template <typename Kernel, typename ResultType, typename ArgumentType>
X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineIntegerV4(
    const ArgumentType * __restrict data_x,
    const ArgumentType * __restrict data_y,
    size_t i_max,
    size_t & i_x,
    size_t & i_y,
    typename Kernel::template State<ResultType> & state,
    const typename Kernel::ConstParams & params)
{
    constexpr size_t unroll_count = 16;
    typename Kernel::template State<ResultType> partial_results[unroll_count]{};
    for (; i_x + unroll_count < i_max; i_x += unroll_count, i_y += unroll_count)
        for (size_t s = 0; s < unroll_count; ++s)
            Kernel::template accumulate<ResultType>(
                partial_results[s], static_cast<ResultType>(data_x[i_x + s]), static_cast<ResultType>(data_y[i_y + s]), params);
    for (auto & partial_result : partial_results)
        Kernel::template combine<ResultType>(state, partial_result, params);
}
#endif

template <typename Kernel>
class FunctionArrayDistance : public IFunction
{
public:
    String getName() const override
    {
        static auto name = String("array") + Kernel::name + "Distance";
        return name;
    }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayDistance<Kernel>>(); }
    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes types;
        for (size_t i = 0; i < 2; ++i)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of function {} must be array.", i, getName());

            types.push_back(array_type->getNestedType());
        }
        const DataTypePtr & common_type = getLeastSupertype(types);
        switch (common_type->getTypeId())
        {
            case TypeIndex::BFloat16: /// (*)
            case TypeIndex::Float32:
                return std::make_shared<DataTypeFloat32>();
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::UInt64:
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Int64:
            case TypeIndex::Float64:
                return std::make_shared<DataTypeFloat64>();
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Supported types: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, BFloat16, Float32, Float64.",
                    getName(),
                    common_type->getName());

            /// (*) You may ask why we return Float32 instead of BFloat16 for Array(BFloat16) arguments.
            ///     The reason is that Intels' SIMD support for BFloat16 that is extremely limited at the moment, see
            ///     https://en.wikichip.org/wiki/x86/avx512_bf16 for AVX-512 BF16. To calculate the common L2 and cosine distances with
            ///     SIMD, we need to cast up or relinquish SIMD support. (Interestingly, FP16 (IEEE 754 binary16) is well supported by
            ///     AVX-512 but nobody seems to likes FP16 these days ...)
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        switch (result_type->getTypeId())
        {
            case TypeIndex::Float32:
                return executeWithResultType<Float32>(arguments, input_rows_count);
            case TypeIndex::Float64:
                return executeWithResultType<Float64>(arguments, input_rows_count);
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type {}", result_type->getName());
        }
    }


#define SUPPORTED_TYPES(ACTION) \
    ACTION(UInt8)   \
    ACTION(UInt16)  \
    ACTION(UInt32)  \
    ACTION(UInt64)  \
    ACTION(Int8)    \
    ACTION(Int16)   \
    ACTION(Int32)   \
    ACTION(Int64)   \
    ACTION(BFloat16) \
    ACTION(Float32) \
    ACTION(Float64)


private:
    template <typename ResultType>
    ColumnPtr executeWithResultType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        DataTypePtr type_x = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();

        switch (type_x->getTypeId())
        {
        #define ON_TYPE(type) \
            case TypeIndex::type: \
                return executeWithResultTypeAndLeftType<ResultType, type>(arguments, input_rows_count); \
                break;

            SUPPORTED_TYPES(ON_TYPE)
        #undef ON_TYPE

            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} have nested type {}. "
                    "Supported types: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, BFloat16, Float32, Float64.",
                    getName(),
                    type_x->getName());
        }
    }

    template <typename ResultType, typename LeftType>
    ColumnPtr executeWithResultTypeAndLeftType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        DataTypePtr type_y = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

        switch (type_y->getTypeId())
        {
        #define ON_TYPE(type) \
            case TypeIndex::type: \
                return executeWithResultTypeAndLeftTypeAndRightType<ResultType, LeftType, type>(arguments[0].column, arguments[1].column, input_rows_count, arguments); \
                break;

            SUPPORTED_TYPES(ON_TYPE)
        #undef ON_TYPE

            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} have nested type {}. "
                    "Supported types: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, BFloat16, Float32, Float64.",
                    getName(),
                    type_y->getName());
        }
    }

    template <typename ResultType, typename LeftType, typename RightType>
    ColumnPtr executeWithResultTypeAndLeftTypeAndRightType(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count, const ColumnsWithTypeAndName & arguments) const
    {
        if (col_x->isConst())
            return executeWithLeftArgConst<ResultType, LeftType, RightType>(col_x, col_y, input_rows_count, arguments);
        if (col_y->isConst())
            return executeWithLeftArgConst<ResultType, RightType, LeftType>(col_y, col_x, input_rows_count, arguments);

        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & data_x = typeid_cast<const ColumnVector<LeftType> &>(array_x.getData()).getData();
        const auto & data_y = typeid_cast<const ColumnVector<RightType> &>(array_y.getData()).getData();

        const auto & offsets_x = array_x.getOffsets();

        if (!array_x.hasEqualOffsets(array_y))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Array arguments for function {} must have equal sizes", getName());

        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        auto col_res = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = col_res->getData();

        ColumnArray::Offset prev = 0;
        size_t row = 0;

        for (auto off : offsets_x)
        {
            /// Process chunks in vectorized manner
            static constexpr size_t VEC_SIZE = 16; /// the choice of the constant has no huge performance impact. 16 seems the best.
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; prev + VEC_SIZE < off; prev += VEC_SIZE)
            {
                for (size_t s = 0; s < VEC_SIZE; ++s)
                    Kernel::template accumulate<ResultType>(
                        states[s], static_cast<ResultType>(data_x[prev + s]), static_cast<ResultType>(data_y[prev + s]), kernel_params);
            }

            typename Kernel::template State<ResultType> state;
            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state, kernel_params);

            /// Process the tail
            for (; prev < off; ++prev)
            {
                Kernel::template accumulate<ResultType>(
                    state, static_cast<ResultType>(data_x[prev]), static_cast<ResultType>(data_y[prev]), kernel_params);
            }
            result_data[row] = Kernel::finalize(state, kernel_params);
            ++row;
        }
        return col_res;
    }

    /// Special case when the 1st parameter is Const
    template <typename ResultType, typename LeftType, typename RightType>
    ColumnPtr executeWithLeftArgConst(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count, const ColumnsWithTypeAndName & arguments) const
    {
        col_x = assert_cast<const ColumnConst *>(col_x.get())->getDataColumnPtr();
        col_y = col_y->convertToFullColumnIfConst();

        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & data_x = typeid_cast<const ColumnVector<LeftType> &>(array_x.getData()).getData();
        const auto & data_y = typeid_cast<const ColumnVector<RightType> &>(array_y.getData()).getData();

        const auto & offsets_x = array_x.getOffsets();
        const auto & offsets_y = array_y.getOffsets();

        ColumnArray::Offset prev_offset = 0;
        for (auto offset_y : offsets_y)
        {
            if (offsets_x[0] != offset_y - prev_offset)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Array arguments for function {} must have equal sizes", getName());
            prev_offset = offset_y;
        }

        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        auto result = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result->getData();

        size_t prev = 0;
        size_t row = 0;

        for (auto off : offsets_y)
        {
            size_t i = 0;
            typename Kernel::template State<ResultType> state;

            /// SIMD optimization: process multiple elements in both input arrays at once.
            /// To avoid combinatorial explosion of SIMD kernels, focus on
            /// - the three most common input/output types (BFloat16 x BFloat16) --> Float32,
            ///   (Float32 x Float32) --> Float32 and (Float64 x Float64) --> Float64
            ///   instead of 11 x 11 input types x 2 output types,
            /// - const/non-const inputs instead of non-const/non-const inputs
            /// - the two most common metrics L2 and cosine distance,
            /// - the most powerful SIMD instruction set (AVX-512).
            bool processed_with_simd = false;
#if USE_MULTITARGET_CODE
            if constexpr (std::is_same_v<Kernel, L1Distance> || std::is_same_v<Kernel, LinfDistance>
                       || std::is_same_v<Kernel, L2Distance> || std::is_same_v<Kernel, CosineDistance>)
            {
                if constexpr ((std::is_same_v<ResultType, Float32> && std::is_same_v<LeftType, Float32> && std::is_same_v<RightType, Float32>)
                           || (std::is_same_v<ResultType, Float64> && std::is_same_v<LeftType, Float64> && std::is_same_v<RightType, Float64>))
                {
                    if (isArchSupported(TargetArch::x86_64_v4))
                    {
                        Kernel::template accumulateCombineF32F64<ResultType>(data_x.data(), data_y.data(), i + offsets_x[0], i, prev, state);
                        processed_with_simd = true;
                    }
                }
                else if constexpr (std::is_same_v<ResultType, Float32> && std::is_same_v<LeftType, BFloat16> && std::is_same_v<RightType, BFloat16>)
                {
                    /// All `BFloat16` metrics widen to `Float32` in a `v4` kernel and use the normal FMA path.
                    /// This beats the native AVX512-BF16 instructions (`vdpbf16ps` for `Cosine`, the BF16->
                    /// `Float32` up-convert for `L2`), which are throughput-limited, even on `sapphirerapids`
                    /// where they are available; plain AVX-512 CPUs (AMD Zen4/5, Skylake-X, Ice Lake) too.
                    if (isArchSupported(TargetArch::x86_64_v4))
                    {
                        Kernel::accumulateCombineBF16V4(data_x.data(), data_y.data(), i + offsets_x[0], i, prev, state);
                        processed_with_simd = true;
                    }
                }
                else if constexpr (is_integer<LeftType> && std::is_same_v<LeftType, RightType>)
                {
                    /// Same-type integer inputs (widened to `Float64`): a generic v4 kernel auto-vectorizes
                    /// the accumulation, lifting integer distances from `x86-64-v2` to AVX-512.
                    if (isArchSupported(TargetArch::x86_64_v4))
                    {
                        accumulateCombineIntegerV4<Kernel, ResultType, LeftType>(data_x.data(), data_y.data(), i + offsets_x[0], i, prev, state, kernel_params);
                        processed_with_simd = true;
                    }
                }
            }
#endif
            if (!processed_with_simd)
            {
                /// Process chunks in a vectorized manner.
                static constexpr size_t VEC_SIZE = 8; /// the choice of the constant has no huge performance impact. 16 breaks interleaving with clang-21. 8 is ok.
                typename Kernel::template State<ResultType> states[VEC_SIZE];
                for (; prev + VEC_SIZE < off; i += VEC_SIZE, prev += VEC_SIZE)
                {
                    for (size_t s = 0; s < VEC_SIZE; ++s)
                        Kernel::template accumulate<ResultType>(
                            states[s], static_cast<ResultType>(data_x[i + s]), static_cast<ResultType>(data_y[prev + s]), kernel_params);
                }

                for (const auto & other_state : states)
                    Kernel::template combine<ResultType>(state, other_state, kernel_params);
            }

            /// Process the tail.
            for (; prev < off; ++i, ++prev)
            {
                Kernel::template accumulate<ResultType>(
                    state, static_cast<ResultType>(data_x[i]), static_cast<ResultType>(data_y[prev]), kernel_params);
            }
            result_data[row] = Kernel::finalize(state, kernel_params);
            row++;
        }

        return result;
    }

    typename Kernel::ConstParams initConstParams(const ColumnsWithTypeAndName &) const { return {}; }
};


template <>
size_t FunctionArrayDistance<LpDistance>::getNumberOfArguments() const { return 3; }

template <>
ColumnNumbers FunctionArrayDistance<LpDistance>::getArgumentsThatAreAlwaysConstant() const { return {2}; }

template <>
LpDistance::ConstParams FunctionArrayDistance<LpDistance>::initConstParams(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() < 3)
        throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Argument p of function {} was not provided",
                    getName());

    if (!arguments[2].column->isNumeric())
        throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument p of function {} must be numeric constant",
                    getName());

    if (!isColumnConst(*arguments[2].column) && arguments[2].column->size() != 1)
        throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Second argument for function {} must be either constant Float64 or constant UInt",
                    getName());

    Float64 p = arguments[2].column->getFloat64(0);
    if (p < 1 || p >= HUGE_VAL)
        throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Second argument for function {} must be not less than one and not be an infinity",
                    getName());

    return LpDistance::ConstParams{p, 1 / p};
}

/// These functions are used by TupleOrArrayFunction
FunctionPtr createFunctionArrayL1Distance(ContextPtr context_);
FunctionPtr createFunctionArrayL2Distance(ContextPtr context_);
FunctionPtr createFunctionArrayL2SquaredDistance(ContextPtr context_);
FunctionPtr createFunctionArrayLpDistance(ContextPtr context_);
FunctionPtr createFunctionArrayLinfDistance(ContextPtr context_);
FunctionPtr createFunctionArrayCosineDistance(ContextPtr context_);
FunctionPtr createFunctionArrayL1Distance(ContextPtr context_) { return FunctionArrayDistance<L1Distance>::create(context_); }
FunctionPtr createFunctionArrayL2Distance(ContextPtr context_) { return FunctionArrayDistance<L2Distance>::create(context_); }
FunctionPtr createFunctionArrayL2SquaredDistance(ContextPtr context_) { return FunctionArrayDistance<L2SquaredDistance>::create(context_); }
FunctionPtr createFunctionArrayLpDistance(ContextPtr context_) { return FunctionArrayDistance<LpDistance>::create(context_); }
FunctionPtr createFunctionArrayLinfDistance(ContextPtr context_) { return FunctionArrayDistance<LinfDistance>::create(context_); }
FunctionPtr createFunctionArrayCosineDistance(ContextPtr context_) { return FunctionArrayDistance<CosineDistance>::create(context_); }

}
