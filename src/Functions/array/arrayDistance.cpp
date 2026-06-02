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

    X86_64_SAPPHIRE_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineBF16(
        const BFloat16 * __restrict data_x,
        const BFloat16 * __restrict data_y,
        size_t i_max,
        size_t & i_x,
        size_t & i_y,
        State<Float32> & state)
    {
        __m512 sums = _mm512_setzero_ps();

        constexpr size_t n = sizeof(__m512) / sizeof(BFloat16);

        for (; i_x + n < i_max; i_x += n, i_y += n)
        {
            __m512 x1 = _mm512_cvtpbh_ps(_mm256_loadu_ps(reinterpret_cast<const Float32 *>(data_x + i_x)));
            __m512 x2 = _mm512_cvtpbh_ps(_mm256_loadu_ps(reinterpret_cast<const Float32 *>(data_x + i_x + n / 2)));
            __m512 y1 = _mm512_cvtpbh_ps(_mm256_loadu_ps(reinterpret_cast<const Float32 *>(data_y + i_y)));
            __m512 y2 = _mm512_cvtpbh_ps(_mm256_loadu_ps(reinterpret_cast<const Float32 *>(data_y + i_y + n / 2)));

            __m512 differences1 = _mm512_sub_ps(x1, y1);
            __m512 differences2 = _mm512_sub_ps(x2, y2);
            sums = _mm512_fmadd_ps(differences1, differences1, sums);
            sums = _mm512_fmadd_ps(differences2, differences2, sums);
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

    X86_64_SAPPHIRE_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombineBF16(
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

        constexpr size_t n = sizeof(__m512) / sizeof(BFloat16);

        for (; i_x + n < i_max; i_x += n, i_y += n)
        {
            __m512 x = _mm512_loadu_ps(data_x + i_x);
            __m512 y = _mm512_loadu_ps(data_y + i_y);
            dot_products = _mm512_dpbf16_ps(dot_products, x, y);
            x_squareds = _mm512_dpbf16_ps(x_squareds, x, x);
            y_squareds = _mm512_dpbf16_ps(y_squareds, y, y);
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

template <typename ResultType, typename LeftType, typename RightType>
constexpr bool is_native_distance_type = std::is_same_v<ResultType, LeftType> && std::is_same_v<ResultType, RightType>;

template <typename LeftType, typename RightType, typename A, typename B>
constexpr bool is_unordered_pair = (std::is_same_v<LeftType, A> && std::is_same_v<RightType, B>)
    || (std::is_same_v<LeftType, B> && std::is_same_v<RightType, A>);

/// Common mixed-type pairs that warrant SIMD specialisation. Selection mirrors
/// real-world vector-search workloads:
///   - UInt8 <-> Float32/Float64: quantised embeddings stored as UInt8 to save
///     space, queried against Float32/Float64 query vectors (typical for ANN).
///   - Float32 <-> Float64: cross-precision lookup (e.g. Float64 client query
///     against Float32 server-side storage).
/// Other mixed pairs fall through to the baseline (non-MULTITARGET) instantiation
/// of executeDistanceMixedImpl. The set is deliberately tiny: extending to the
/// full 11x11 numeric Cartesian product would explode MULTITARGET instantiations
/// to 11*11*6*2*3 = 4356 .text copies and push arrayDistance.o past the 50 MB
/// CI object-size limit.
template <typename LeftType, typename RightType>
constexpr bool is_common_mixed_pair = is_unordered_pair<LeftType, RightType, UInt8, Float32>
    || is_unordered_pair<LeftType, RightType, UInt8, Float64>
    || is_unordered_pair<LeftType, RightType, Float32, Float64>;

/// Multi-target hot loop for the native same-type path (LeftType == RightType == ResultType).
/// The MULTITARGET macro generates _x86_64_v4, _x86_64_v3, and default versions
/// so the compiler can auto-vectorize with the best available ISA.
///
/// Mixed-type pairs do NOT use this kernel; they go through `executeDistanceMixed` which casts
/// per element inside the hot loop instead of pre-converting full columns. Templating only on
/// <Kernel, ResultType> here keeps the LeftType x RightType x arch instantiation explosion bounded.
MULTITARGET_FUNCTION_X86_V4_V3(
MULTITARGET_FUNCTION_HEADER(
    template <typename Kernel, typename ResultType>
    void NO_INLINE
), executeDistanceImpl, MULTITARGET_FUNCTION_BODY((
    const ResultType * __restrict data_x,
    const ResultType * __restrict data_y,
    const ColumnArray::Offset * __restrict offsets,
    ResultType * __restrict result,
    size_t row_count,
    const typename Kernel::ConstParams & params)
{
    constexpr size_t unroll_count = 128 / sizeof(ResultType);

    ColumnArray::Offset prev = 0;
    for (size_t row = 0; row < row_count; ++row)
    {
        const auto off = offsets[row];
        const size_t count = off - prev;

        typename Kernel::template State<ResultType> partial[unroll_count];
        size_t i = 0;
        const size_t unrolled_end = count / unroll_count * unroll_count;

        for (; i < unrolled_end; i += unroll_count)
            for (size_t s = 0; s < unroll_count; ++s)
                Kernel::template accumulate<ResultType>(
                    partial[s],
                    data_x[prev + i + s],
                    data_y[prev + i + s],
                    params);

        typename Kernel::template State<ResultType> state;
        for (size_t s = 0; s < unroll_count; ++s)
            Kernel::template combine<ResultType>(state, partial[s], params);

        for (; i < count; ++i)
            Kernel::template accumulate<ResultType>(
                state,
                data_x[prev + i],
                data_y[prev + i],
                params);

        result[row] = Kernel::finalize(state, params);
        prev = off;
    }
}))

template <typename Kernel, typename ResultType>
void executeDistance(
    const ResultType * __restrict data_x,
    const ResultType * __restrict data_y,
    const ColumnArray::Offset * __restrict offsets,
    ResultType * __restrict result,
    size_t row_count,
    const typename Kernel::ConstParams & params)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
        return executeDistanceImpl_x86_64_v4<Kernel, ResultType>(data_x, data_y, offsets, result, row_count, params);
    if (isArchSupported(TargetArch::x86_64_v3))
        return executeDistanceImpl_x86_64_v3<Kernel, ResultType>(data_x, data_y, offsets, result, row_count, params);
#endif
    executeDistanceImpl<Kernel, ResultType>(data_x, data_y, offsets, result, row_count, params);
}

/// Streaming mixed-type path. It casts individual elements inside the hot loop
/// and never materialises converted full-column buffers.
///
/// One MULTITARGET definition serves both common and uncommon pairs:
///   - Common pairs (see is_common_mixed_pair) call executeDistanceMixedImpl_x86_64_v3/v4
///     for SIMD acceleration.
///   - Other mixed pairs fall back to the baseline (non-MULTITARGET) instantiation
///     of executeDistanceMixedImpl, keeping LeftType x RightType x ISA explosion bounded.
MULTITARGET_FUNCTION_X86_V4_V3(
MULTITARGET_FUNCTION_HEADER(
    template <typename Kernel, typename ResultType, typename LeftType, typename RightType>
    void NO_INLINE
), executeDistanceMixedImpl, MULTITARGET_FUNCTION_BODY((
    const LeftType * __restrict data_x,
    const RightType * __restrict data_y,
    const ColumnArray::Offset * __restrict offsets,
    ResultType * __restrict result,
    size_t row_count,
    const typename Kernel::ConstParams & params)
{
    constexpr size_t unroll_count = 128 / sizeof(ResultType);

    ColumnArray::Offset prev = 0;
    for (size_t row = 0; row < row_count; ++row)
    {
        const auto off = offsets[row];
        const size_t count = off - prev;

        typename Kernel::template State<ResultType> partial[unroll_count];
        size_t i = 0;
        const size_t unrolled_end = count / unroll_count * unroll_count;

        for (; i < unrolled_end; i += unroll_count)
            for (size_t s = 0; s < unroll_count; ++s)
                Kernel::template accumulate<ResultType>(
                    partial[s],
                    static_cast<ResultType>(data_x[prev + i + s]),
                    static_cast<ResultType>(data_y[prev + i + s]),
                    params);

        typename Kernel::template State<ResultType> state;
        for (size_t s = 0; s < unroll_count; ++s)
            Kernel::template combine<ResultType>(state, partial[s], params);

        for (; i < count; ++i)
            Kernel::template accumulate<ResultType>(
                state,
                static_cast<ResultType>(data_x[prev + i]),
                static_cast<ResultType>(data_y[prev + i]),
                params);

        result[row] = Kernel::finalize(state, params);
        prev = off;
    }
}))

template <typename Kernel, typename ResultType, typename LeftType, typename RightType>
void executeDistanceMixed(
    const LeftType * __restrict data_x,
    const RightType * __restrict data_y,
    const ColumnArray::Offset * __restrict offsets,
    ResultType * __restrict result,
    size_t row_count,
    const typename Kernel::ConstParams & params)
{
#if USE_MULTITARGET_CODE
    if constexpr (is_common_mixed_pair<LeftType, RightType>)
    {
        if (isArchSupported(TargetArch::x86_64_v4))
            return executeDistanceMixedImpl_x86_64_v4<Kernel, ResultType, LeftType, RightType>(
                data_x, data_y, offsets, result, row_count, params);
        if (isArchSupported(TargetArch::x86_64_v3))
            return executeDistanceMixedImpl_x86_64_v3<Kernel, ResultType, LeftType, RightType>(
                data_x, data_y, offsets, result, row_count, params);
    }
#endif
    executeDistanceMixedImpl<Kernel, ResultType, LeftType, RightType>(data_x, data_y, offsets, result, row_count, params);
}


/// Multi-target hot loop for const x non-const path.
/// data_x is the constant array (repeated for every row), data_y varies per row.
MULTITARGET_FUNCTION_X86_V4_V3(
MULTITARGET_FUNCTION_HEADER(
    template <typename Kernel, typename ResultType>
    void NO_INLINE
), executeDistanceConstImpl, MULTITARGET_FUNCTION_BODY((
    const ResultType * __restrict data_x,
    size_t array_size,
    const ResultType * __restrict data_y,
    const ColumnArray::Offset * __restrict offsets,
    ResultType * __restrict result,
    size_t row_count,
    const typename Kernel::ConstParams & params)
{
    constexpr size_t unroll_count = 128 / sizeof(ResultType);

    ColumnArray::Offset prev = 0;
    for (size_t row = 0; row < row_count; ++row)
    {
        const auto off = offsets[row];
        const size_t count = off - prev;
        chassert(count == array_size);

        typename Kernel::template State<ResultType> partial[unroll_count];
        size_t i = 0;
        const size_t unrolled_end = array_size / unroll_count * unroll_count;

        for (; i < unrolled_end; i += unroll_count)
            for (size_t s = 0; s < unroll_count; ++s)
                Kernel::template accumulate<ResultType>(
                    partial[s],
                    data_x[i + s],
                    data_y[prev + i + s],
                    params);

        typename Kernel::template State<ResultType> state;
        for (size_t s = 0; s < unroll_count; ++s)
            Kernel::template combine<ResultType>(state, partial[s], params);

        for (; i < array_size; ++i)
            Kernel::template accumulate<ResultType>(
                state,
                data_x[i],
                data_y[prev + i],
                params);

        result[row] = Kernel::finalize(state, params);
        prev = off;
    }
}))

template <typename Kernel, typename ResultType>
void executeDistanceConst(
    const ResultType * __restrict data_x,
    size_t array_size,
    const ResultType * __restrict data_y,
    const ColumnArray::Offset * __restrict offsets,
    ResultType * __restrict result,
    size_t row_count,
    const typename Kernel::ConstParams & params)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
        return executeDistanceConstImpl_x86_64_v4<Kernel, ResultType>(data_x, array_size, data_y, offsets, result, row_count, params);
    if (isArchSupported(TargetArch::x86_64_v3))
        return executeDistanceConstImpl_x86_64_v3<Kernel, ResultType>(data_x, array_size, data_y, offsets, result, row_count, params);
#endif
    executeDistanceConstImpl<Kernel, ResultType>(data_x, array_size, data_y, offsets, result, row_count, params);
}

/// const-left mirror of executeDistanceMixedImpl. Same MULTITARGET strategy:
/// common pairs get x86_64_v3/v4 instances, other pairs reuse the baseline.
MULTITARGET_FUNCTION_X86_V4_V3(
MULTITARGET_FUNCTION_HEADER(
    template <typename Kernel, typename ResultType, typename LeftType, typename RightType>
    void NO_INLINE
), executeDistanceConstMixedImpl, MULTITARGET_FUNCTION_BODY((
    const LeftType * __restrict data_x,
    size_t array_size,
    const RightType * __restrict data_y,
    const ColumnArray::Offset * __restrict offsets,
    ResultType * __restrict result,
    size_t row_count,
    const typename Kernel::ConstParams & params)
{
    constexpr size_t unroll_count = 128 / sizeof(ResultType);

    ColumnArray::Offset prev = 0;
    for (size_t row = 0; row < row_count; ++row)
    {
        const auto off = offsets[row];
        const size_t count = off - prev;
        chassert(count == array_size);

        typename Kernel::template State<ResultType> partial[unroll_count];
        size_t i = 0;
        const size_t unrolled_end = array_size / unroll_count * unroll_count;

        for (; i < unrolled_end; i += unroll_count)
            for (size_t s = 0; s < unroll_count; ++s)
                Kernel::template accumulate<ResultType>(
                    partial[s],
                    static_cast<ResultType>(data_x[i + s]),
                    static_cast<ResultType>(data_y[prev + i + s]),
                    params);

        typename Kernel::template State<ResultType> state;
        for (size_t s = 0; s < unroll_count; ++s)
            Kernel::template combine<ResultType>(state, partial[s], params);

        for (; i < array_size; ++i)
            Kernel::template accumulate<ResultType>(
                state,
                static_cast<ResultType>(data_x[i]),
                static_cast<ResultType>(data_y[prev + i]),
                params);

        result[row] = Kernel::finalize(state, params);
        prev = off;
    }
}))

template <typename Kernel, typename ResultType, typename LeftType, typename RightType>
void executeDistanceConstMixed(
    const LeftType * __restrict data_x,
    size_t array_size,
    const RightType * __restrict data_y,
    const ColumnArray::Offset * __restrict offsets,
    ResultType * __restrict result,
    size_t row_count,
    const typename Kernel::ConstParams & params)
{
#if USE_MULTITARGET_CODE
    if constexpr (is_common_mixed_pair<LeftType, RightType>)
    {
        if (isArchSupported(TargetArch::x86_64_v4))
            return executeDistanceConstMixedImpl_x86_64_v4<Kernel, ResultType, LeftType, RightType>(
                data_x, array_size, data_y, offsets, result, row_count, params);
        if (isArchSupported(TargetArch::x86_64_v3))
            return executeDistanceConstMixedImpl_x86_64_v3<Kernel, ResultType, LeftType, RightType>(
                data_x, array_size, data_y, offsets, result, row_count, params);
    }
#endif
    executeDistanceConstMixedImpl<Kernel, ResultType, LeftType, RightType>(
        data_x, array_size, data_y, offsets, result, row_count, params);
}


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

        if constexpr (is_native_distance_type<ResultType, LeftType, RightType>)
            executeDistance<Kernel, ResultType>(
                data_x.data(), data_y.data(), offsets_x.data(), result_data.data(), input_rows_count, kernel_params);
        else
            executeDistanceMixed<Kernel, ResultType, LeftType, RightType>(
                data_x.data(), data_y.data(), offsets_x.data(), result_data.data(), input_rows_count, kernel_params);

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

        /// Hand-written AVX-512 intrinsics for L2/Cosine with Float32/Float64/BFloat16.
        /// These outperform compiler auto-vectorization for these specific kernels.
#if USE_MULTITARGET_CODE
        if constexpr (std::is_same_v<Kernel, L2Distance> || std::is_same_v<Kernel, CosineDistance>)
        {
            if constexpr ((std::is_same_v<ResultType, Float32> && std::is_same_v<LeftType, Float32> && std::is_same_v<RightType, Float32>)
                       || (std::is_same_v<ResultType, Float64> && std::is_same_v<LeftType, Float64> && std::is_same_v<RightType, Float64>))
            {
                if (isArchSupported(TargetArch::x86_64_v4))
                {
                    size_t prev = 0;
                    for (size_t row = 0; row < input_rows_count; ++row)
                    {
                        const auto off = offsets_y[row];
                        size_t i = 0;
                        size_t j = prev;
                        typename Kernel::template State<ResultType> state;
                        Kernel::template accumulateCombineF32F64<ResultType>(data_x.data(), data_y.data(), i + offsets_x[0], i, j, state);
                        for (; j < off; ++i, ++j)
                            Kernel::template accumulate<ResultType>(
                                state, data_x[i], data_y[j], kernel_params);
                        result_data[row] = Kernel::finalize(state, kernel_params);
                        prev = off;
                    }
                    return result;
                }
            }
            else if constexpr (std::is_same_v<ResultType, Float32> && std::is_same_v<LeftType, BFloat16> && std::is_same_v<RightType, BFloat16>)
            {
                if (isArchSupported(TargetArch::x86_64_sapphirerapids))
                {
                    size_t prev = 0;
                    for (size_t row = 0; row < input_rows_count; ++row)
                    {
                        const auto off = offsets_y[row];
                        size_t i = 0;
                        size_t j = prev;
                        typename Kernel::template State<Float32> state;
                        Kernel::accumulateCombineBF16(data_x.data(), data_y.data(), i + offsets_x[0], i, j, state);
                        for (; j < off; ++i, ++j)
                            Kernel::template accumulate<Float32>(
                                state, static_cast<Float32>(data_x[i]), static_cast<Float32>(data_y[j]), kernel_params);
                        result_data[row] = Kernel::finalize(state, kernel_params);
                        prev = off;
                    }
                    return result;
                }
            }
        }
#endif

        if constexpr (is_native_distance_type<ResultType, LeftType, RightType>)
            executeDistanceConst<Kernel, ResultType>(
                data_x.data(), offsets_x[0], data_y.data(), offsets_y.data(), result_data.data(), input_rows_count, kernel_params);
        else
            executeDistanceConstMixed<Kernel, ResultType, LeftType, RightType>(
                data_x.data(), offsets_x[0], data_y.data(), offsets_y.data(), result_data.data(), input_rows_count, kernel_params);

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
