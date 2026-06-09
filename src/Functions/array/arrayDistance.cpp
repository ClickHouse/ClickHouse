#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDenseVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/TargetSpecific.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeVector.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/castColumn.h>

#include <cmath>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

/// Include immintrin. Otherwise `simsimd` fails to build: `unknown type name '__bfloat16'`
#if USE_SIMSIMD
#    if defined(__x86_64__) || defined(__i386__)
#        include <immintrin.h>
#    endif
#    include <simsimd/simsimd.h>
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
        const DataTypeVector * vector_types[2] = {nullptr, nullptr};
        for (size_t i = 0; i < 2; ++i)
        {
            if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get()))
            {
                types.push_back(array_type->getNestedType());
            }
            else if (const auto * vector_type = checkAndGetDataType<DataTypeVector>(arguments[i].type.get()))
            {
                vector_types[i] = vector_type;
                types.push_back(vector_type->getElementType());
            }
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of function {} must be array or vector.", i, getName());
        }

        /// When both arguments are vectors their dimension is known at analysis time, so enforce it early.
        if (vector_types[0] && vector_types[1] && vector_types[0]->getDimension() != vector_types[1]->getDimension())
            throw Exception(
                ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Vector arguments for function {} must have equal dimension, got {} and {}",
                getName(),
                vector_types[0]->getDimension(),
                vector_types[1]->getDimension());
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
        /// The Vector(T, N) FLAT path: contiguous, offset-free storage computed through SimSIMD (see executeVector).
        if (isVector(arguments[0].type) || isVector(arguments[1].type))
            return executeVector(arguments, result_type, input_rows_count);

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
            if constexpr (std::is_same_v<Kernel, L2Distance> || std::is_same_v<Kernel, CosineDistance>)
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
                    if (isArchSupported(TargetArch::x86_64_sapphirerapids))
                    {
                        Kernel::accumulateCombineBF16(data_x.data(), data_y.data(), i + offsets_x[0], i, prev, state);
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

    /// Vector(T, N) path. One argument is a Vector column; the other is the reference (an Array, usually a
    /// constant query vector). Data is contiguous with a fixed stride N, so the distance is computed directly
    /// over raw pointers via SimSIMD (with a generic scalar fallback), without building offsets.
    ColumnPtr executeVector(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const auto * vector_type = checkAndGetDataType<DataTypeVector>(arguments[0].type.get());
        if (!vector_type)
            vector_type = checkAndGetDataType<DataTypeVector>(arguments[1].type.get());

        const size_t dimension = vector_type->getDimension();
        const DataTypePtr & element_type = vector_type->getElementType();

        switch (result_type->getTypeId())
        {
            case TypeIndex::Float32:
                return executeVectorWithElementType<Float32>(arguments, element_type, dimension, input_rows_count);
            case TypeIndex::Float64:
                return executeVectorWithElementType<Float64>(arguments, element_type, dimension, input_rows_count);
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type {}", result_type->getName());
        }
    }

    template <typename ResultType>
    ColumnPtr executeVectorWithElementType(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & element_type, size_t dimension, size_t input_rows_count) const
    {
        switch (element_type->getTypeId())
        {
            case TypeIndex::BFloat16:
                return executeVectorImpl<ResultType, BFloat16>(arguments, element_type, dimension, input_rows_count);
            case TypeIndex::Float32:
                return executeVectorImpl<ResultType, Float32>(arguments, element_type, dimension, input_rows_count);
            case TypeIndex::Float64:
                return executeVectorImpl<ResultType, Float64>(arguments, element_type, dimension, input_rows_count);
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Vector element type {} is not supported", element_type->getName());
        }
    }

    template <typename ResultType, typename T>
    ColumnPtr executeVectorImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & element_type, size_t dimension, size_t input_rows_count) const
    {
        /// Holders keep the (possibly casted) source columns alive while we read their raw data.
        std::vector<ColumnPtr> holders;
        const auto [data_x, stride_x] = getVectorData<T>(arguments[0], element_type, dimension, holders);
        const auto [data_y, stride_y] = getVectorData<T>(arguments[1], element_type, dimension, holders);

        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        auto col_res = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = col_res->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
            result_data[row] = computeVectorDistance<ResultType, T>(
                data_x + row * stride_x, data_y + row * stride_y, dimension, kernel_params);

        return col_res;
    }

    /// Returns a base pointer to the element data of type T and a per-row stride (0 for a constant argument that
    /// is reused for every row, `dimension` for a regular per-row column).
    template <typename T>
    std::pair<const T *, size_t> getVectorData(
        const ColumnWithTypeAndName & argument, const DataTypePtr & element_type, size_t dimension, std::vector<ColumnPtr> & holders) const
    {
        const ColumnPtr & col = argument.column;
        const bool is_const = isColumnConst(*col);
        const IColumn * data_col = is_const ? &assert_cast<const ColumnConst &>(*col).getDataColumn() : col.get();

        if (const auto * vec = typeid_cast<const ColumnDenseVector *>(data_col))
        {
            if (vec->getDimension() != dimension)
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Vector arguments for function {} must have equal dimension", getName());
            holders.push_back(col);
            const T * base = reinterpret_cast<const T *>(vec->getFixedStringData().getChars().data());
            return {base, is_const ? 0 : dimension};
        }

        /// The reference vector is an Array. Cast it to Array(element_type) so its data is a ColumnVector<T>.
        ColumnPtr casted = castColumn(argument, std::make_shared<DataTypeArray>(element_type));
        holders.push_back(casted);

        const bool casted_const = isColumnConst(*casted);
        const IColumn * casted_data = casted_const ? &assert_cast<const ColumnConst &>(*casted).getDataColumn() : casted.get();
        const auto & array = assert_cast<const ColumnArray &>(*casted_data);
        const auto & offsets = array.getOffsets();

        for (size_t i = 0; i < offsets.size(); ++i)
        {
            const size_t length = offsets[i] - (i == 0 ? 0 : offsets[i - 1]);
            if (length != dimension)
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "Array argument for function {} must have size {}, got {}",
                    getName(),
                    dimension,
                    length);
        }

        const T * base = typeid_cast<const ColumnVector<T> &>(array.getData()).getData().data();
        return {base, casted_const ? 0 : dimension};
    }

    template <typename ResultType, typename T>
    static ResultType computeVectorDistance(const T * x, const T * y, size_t n, const typename Kernel::ConstParams & kernel_params)
    {
#if USE_SIMSIMD
        /// SimSIMD provides Faiss-class kernels for the two most common metrics (the same library the vector
        /// similarity index and QBit distances use). Other metrics fall through to the generic scalar path below.
        if constexpr (std::is_same_v<Kernel, L2Distance>)
        {
            simsimd_distance_t result = 0;
            if constexpr (std::is_same_v<T, Float32>)
                simsimd_l2_f32(x, y, n, &result);
            else if constexpr (std::is_same_v<T, Float64>)
                simsimd_l2_f64(x, y, n, &result);
            else
                simsimd_l2_bf16(reinterpret_cast<const simsimd_bf16_t *>(x), reinterpret_cast<const simsimd_bf16_t *>(y), n, &result);
            return static_cast<ResultType>(result);
        }
        else if constexpr (std::is_same_v<Kernel, CosineDistance>)
        {
            simsimd_distance_t result = 0;
            if constexpr (std::is_same_v<T, Float32>)
                simsimd_cos_f32(x, y, n, &result);
            else if constexpr (std::is_same_v<T, Float64>)
                simsimd_cos_f64(x, y, n, &result);
            else
                simsimd_cos_bf16(reinterpret_cast<const simsimd_bf16_t *>(x), reinterpret_cast<const simsimd_bf16_t *>(y), n, &result);
            return static_cast<ResultType>(result);
        }
#endif
        /// Generic scalar path: reuses the kernel's accumulate/finalize over the contiguous data. Also the
        /// fallback when SimSIMD is unavailable, and the implementation for L1/L2Squared/Lp/Linf on vectors.
        typename Kernel::template State<ResultType> state;
        for (size_t i = 0; i < n; ++i)
            Kernel::template accumulate<ResultType>(state, static_cast<ResultType>(x[i]), static_cast<ResultType>(y[i]), kernel_params);
        return Kernel::finalize(state, kernel_params);
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
