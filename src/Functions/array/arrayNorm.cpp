#include <cmath>
#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Common/TargetSpecific.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

struct L1Norm
{
    static constexpr auto name = "L1";

    struct ConstParams {};

    template <typename ResultType>
    static ResultType accumulate(ResultType result, ResultType value, const ConstParams &)
    {
        return result + fabs(value);
    }

    template <typename ResultType>
    static ResultType combine(ResultType result, ResultType other_result, const ConstParams &)
    {
        return result + other_result;
    }

    template <typename ResultType>
    static ResultType finalize(ResultType result, const ConstParams &)
    {
        return result;
    }

#if USE_MULTITARGET_CODE
    template <typename ResultType>
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombine(
        const ResultType * data,
        size_t i_max,
        size_t & i,
        ResultType & state)
    {
        static constexpr bool is_float32 = std::is_same_v<ResultType, Float32>;

        __m512 sums;
        if constexpr (is_float32)
            sums = _mm512_setzero_ps();
        else
            sums = _mm512_setzero_pd();

        constexpr size_t n = sizeof(__m512) / sizeof(ResultType);

        if constexpr (is_float32)
        {
            const __m512 sign_mask = _mm512_set1_ps(-0.0f);
            for (; i + n < i_max; i += n)
            {
                __m512 x = _mm512_loadu_ps(data + i);
                __m512 abs_x = _mm512_andnot_ps(sign_mask, x);
                sums = _mm512_add_ps(sums, abs_x);
            }
            state += _mm512_reduce_add_ps(sums);
        }
        else
        {
            const __m512d sign_mask = _mm512_set1_pd(-0.0);
            for (; i + n < i_max; i += n)
            {
                __m512d x = _mm512_loadu_pd(data + i);
                __m512d abs_x = _mm512_andnot_pd(sign_mask, x);
                sums = _mm512_add_pd(abs_x, sums);
            }
            state += _mm512_reduce_add_pd(sums);
        }
    }
#endif
};

struct L2Norm
{
    static constexpr auto name = "L2";

    struct ConstParams {};

    template <typename ResultType>
    static ResultType accumulate(ResultType result, ResultType value, const ConstParams &)
    {
        return result + value * value;
    }

    template <typename ResultType>
    static ResultType combine(ResultType result, ResultType other_result, const ConstParams &)
    {
        return result + other_result;
    }

    template <typename ResultType>
    static ResultType finalize(ResultType result, const ConstParams &)
    {
        return sqrt(result);
    }

#if USE_MULTITARGET_CODE
    template <typename ResultType>
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombine(
        const ResultType * data,
        size_t i_max,
        size_t & i,
        ResultType & state)
    {
        static constexpr bool is_float32 = std::is_same_v<ResultType, Float32>;

        __m512 sums;
        if constexpr (is_float32)
            sums = _mm512_setzero_ps();
        else
            sums = _mm512_setzero_pd();

        constexpr size_t n = sizeof(__m512) / sizeof(ResultType);

        for (; i + n < i_max; i += n)
        {
            if constexpr (is_float32)
            {
                __m512 x = _mm512_loadu_ps(data + i);
                sums = _mm512_fmadd_ps(x, x, sums);
            }
            else
            {
                __m512d x = _mm512_loadu_pd(data + i);
                sums = _mm512_fmadd_pd(x, x, sums);
            }
        }

        if constexpr (is_float32)
            state += _mm512_reduce_add_ps(sums);
        else
            state += _mm512_reduce_add_pd(sums);
    }
#endif
};

struct L2SquaredNorm : L2Norm
{
    static constexpr auto name = "L2Squared";

    template <typename ResultType>
    static ResultType finalize(ResultType result, const ConstParams &)
    {
        return result;
    }
};


struct LpNorm
{
    static constexpr auto name = "Lp";

    struct ConstParams
    {
        Float64 power;
        Float64 inverted_power = 1 / power;
    };

    template <typename ResultType>
    static ResultType accumulate(ResultType result, ResultType value, const ConstParams & params)
    {
        return result + static_cast<ResultType>(std::pow(fabs(value), params.power));
    }

    template <typename ResultType>
    static ResultType combine(ResultType result, ResultType other_result, const ConstParams &)
    {
        return result + other_result;
    }

    template <typename ResultType>
    static ResultType finalize(ResultType result, const ConstParams & params)
    {
        return static_cast<ResultType>(std::pow(result, params.inverted_power));
    }
};

struct LinfNorm
{
    static constexpr auto name = "Linf";

    struct ConstParams {};

    template <typename ResultType>
    static ResultType accumulate(ResultType result, ResultType value, const ConstParams &)
    {
        return fmax(result, fabs(value));
    }

    template <typename ResultType>
    static ResultType combine(ResultType result, ResultType other_result, const ConstParams &)
    {
        return fmax(result, other_result);
    }

    template <typename ResultType>
    static ResultType finalize(ResultType result, const ConstParams &)
    {
        return result;
    }

#if USE_MULTITARGET_CODE
    template <typename ResultType>
    X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombine(
        const ResultType * data,
        size_t i_max,
        size_t & i,
        ResultType & state)
    {
        static constexpr bool is_float32 = std::is_same_v<ResultType, Float32>;

        constexpr size_t n = sizeof(__m512) / sizeof(ResultType);

        if constexpr (is_float32)
        {
            const __m512 sign_mask = _mm512_set1_ps(-0.0f);
            __m512 maxes = _mm512_setzero_ps();
            for (; i + n < i_max; i += n)
            {
                __m512 x = _mm512_loadu_ps(data + i);
                __m512 abs_x = _mm512_andnot_ps(sign_mask, x);
                maxes = _mm512_max_ps(maxes, abs_x);
            }
            state = fmax(state, _mm512_reduce_max_ps(maxes));
        }
        else
        {
            const __m512d sign_mask = _mm512_set1_pd(-0.0);
            __m512d maxes = _mm512_setzero_pd();
            for (; i + n < i_max; i += n)
            {
                __m512d x = _mm512_loadu_pd(data + i);
                __m512d abs_x = _mm512_andnot_pd(sign_mask, x);
                maxes = _mm512_max_pd(maxes, abs_x);
            }
            state = fmax(state, _mm512_reduce_max_pd(maxes));
        }
    }
#endif
};


template <class Kernel>
class FunctionArrayNorm : public IFunction
{
public:
    String getName() const override { static auto name = String("array") + Kernel::name + "Norm"; return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayNorm<Kernel>>(); }
    size_t getNumberOfArguments() const override { return 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must be array.", getName());

        switch (array_type->getNestedType()->getTypeId())
        {
            case TypeIndex::BFloat16:
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
                    array_type->getNestedType()->getName());
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        DataTypePtr type = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();
        ColumnPtr column = arguments[0].column->convertToFullColumnIfConst();
        const auto * arr = assert_cast<const ColumnArray *>(column.get());

        switch (result_type->getTypeId())
        {
            case TypeIndex::Float32:
                return executeWithResultType<Float32>(*arr, type, input_rows_count, arguments);
                break;
            case TypeIndex::Float64:
                return executeWithResultType<Float64>(*arr, type, input_rows_count, arguments);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type {}", result_type->getName());
        }
    }

private:

#define SUPPORTED_TYPES(action) \
    action(UInt8)   \
    action(UInt16)  \
    action(UInt32)  \
    action(UInt64)  \
    action(Int8)    \
    action(Int16)   \
    action(Int32)   \
    action(Int64)   \
    action(BFloat16) \
    action(Float32) \
    action(Float64)


    template <typename ResultType>
    ColumnPtr executeWithResultType(const ColumnArray & array, const DataTypePtr & nested_type, size_t input_rows_count, const ColumnsWithTypeAndName & arguments) const
    {
        switch (nested_type->getTypeId())
        {
        #define ON_TYPE(type) \
            case TypeIndex::type: \
                return executeWithTypes<ResultType, type>(array, input_rows_count, arguments); \
                break;

            SUPPORTED_TYPES(ON_TYPE)
        #undef ON_TYPE

            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} have nested type {}. "
                    "Supported types: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, BFloat16, Float32, Float64.",
                    getName(), nested_type->getName());
        }
    }

    template <typename ResultType, typename ArgumentType>
    ColumnPtr executeWithTypes(const ColumnArray & array, size_t input_rows_count, const ColumnsWithTypeAndName & arguments) const
    {
        const auto & data = typeid_cast<const ColumnVector<ArgumentType> &>(array.getData()).getData();
        const auto & offsets = array.getOffsets();

        auto result_col = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result_col->getData();

        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        ColumnArray::Offset prev = 0;
        size_t row = 0;
        for (auto off : offsets)
        {
            ResultType state = 0;
            size_t i = 0;
            const size_t count = off - prev;

            bool processed_with_simd = false;
#if USE_MULTITARGET_CODE
            if constexpr (!std::is_same_v<Kernel, LpNorm>)
            {
                if constexpr ((std::is_same_v<ResultType, Float32> && std::is_same_v<ArgumentType, Float32>)
                           || (std::is_same_v<ResultType, Float64> && std::is_same_v<ArgumentType, Float64>))
                {
                    if (isArchSupported(TargetArch::x86_64_v4))
                    {
                        Kernel::template accumulateCombine<ResultType>(data.data() + prev, count, i, state);
                        processed_with_simd = true;
                    }
                }
            }
#endif
            if (!processed_with_simd)
            {
                /// Process chunks in vectorized manner
                static constexpr size_t VEC_SIZE = 4;
                ResultType results[VEC_SIZE] = {0};
                for (; i + VEC_SIZE < count; i += VEC_SIZE)
                {
                    for (size_t s = 0; s < VEC_SIZE; ++s)
                        results[s] = Kernel::template accumulate<ResultType>(results[s], static_cast<ResultType>(data[prev + i + s]), kernel_params);
                }

                for (const auto & other_state : results)
                    state = Kernel::template combine<ResultType>(state, other_state, kernel_params);
            }

            /// Process the tail
            for (; i < count; ++i)
            {
                state = Kernel::template accumulate<ResultType>(state, static_cast<ResultType>(data[prev + i]), kernel_params);
            }
            result_data[row] = Kernel::finalize(state, kernel_params);
            prev = off;
            row++;
        }
        return result_col;
    }

    typename Kernel::ConstParams initConstParams(const ColumnsWithTypeAndName &) const { return {}; }
};

template <>
size_t FunctionArrayNorm<LpNorm>::getNumberOfArguments() const { return 2; }

template <>
ColumnNumbers FunctionArrayNorm<LpNorm>::getArgumentsThatAreAlwaysConstant() const { return {1}; }

template <>
LpNorm::ConstParams FunctionArrayNorm<LpNorm>::initConstParams(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() < 2)
        throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Argument p of function {} was not provided",
                    getName());

    if (!arguments[1].column->isNumeric())
        throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument p of function {} must be numeric constant",
                    getName());

    if (!isColumnConst(*arguments[1].column))
        throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Second argument for function {} must be either constant Float64 or constant UInt",
                    getName());

    Float64 p = arguments[1].column->getFloat64(0);
    if (p < 1 || p >= HUGE_VAL)
        throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Second argument for function {} must be not less than one and not be an infinity",
                    getName());

    return LpNorm::ConstParams{p, 1 / p};
}


/// These functions are used by TupleOrArrayFunction
FunctionPtr createFunctionArrayL1Norm(ContextPtr context_) { return FunctionArrayNorm<L1Norm>::create(context_); }
FunctionPtr createFunctionArrayL2Norm(ContextPtr context_) { return FunctionArrayNorm<L2Norm>::create(context_); }
FunctionPtr createFunctionArrayL2SquaredNorm(ContextPtr context_) { return FunctionArrayNorm<L2SquaredNorm>::create(context_); }
FunctionPtr createFunctionArrayLpNorm(ContextPtr context_) { return FunctionArrayNorm<LpNorm>::create(context_); }
FunctionPtr createFunctionArrayLinfNorm(ContextPtr context_) { return FunctionArrayNorm<LinfNorm>::create(context_); }

}
