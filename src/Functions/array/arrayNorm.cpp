#include <cmath>
#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Common/TargetSpecific.h>

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
};


/// Auto-vectorized norm reduction kernel, modeled after the `arrayDotProduct` kernel. Manual unrolling
/// with independent accumulators breaks the FP dependency chain so the compiler can keep several SIMD
/// registers in flight and (for `L2`/`L2Squared`) fuse `a*b + c` into FMA.
///
/// The whole row loop lives inside this single multitarget call: an `x86_64_v4` (AVX-512) specialisation
/// cannot be inlined into the `v2`-baseline caller, so a per-row call would impose a hard boundary every
/// ~150 elements that interrupts the hardware prefetcher's stream. Batching all rows into one call keeps
/// the load stream continuous across row boundaries, which matters for the bandwidth-bound `Float64` paths.
MULTITARGET_FUNCTION_X86_V4(
    MULTITARGET_FUNCTION_HEADER(template <typename Kernel, typename ResultType> static void NO_SANITIZE_UNDEFINED NO_INLINE),
    normBatchImpl,
    MULTITARGET_FUNCTION_BODY((
        const ResultType * __restrict data,
        const ColumnArray::Offset * __restrict offsets,
        ResultType * __restrict result_data,
        size_t input_rows_count,
        const typename Kernel::ConstParams & params)
    {
        constexpr size_t unroll_count = 16;
        ColumnArray::Offset prev = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const size_t count = offsets[row] - prev;
            const ResultType * __restrict row_data = data + prev;

            ResultType partial_results[unroll_count]{};
            size_t i = 0;
            const size_t unrolled_end = count / unroll_count * unroll_count;
            for (; i < unrolled_end; i += unroll_count)
                for (size_t s = 0; s < unroll_count; ++s)
                    partial_results[s] = Kernel::template accumulate<ResultType>(partial_results[s], row_data[i + s], params);

            ResultType result = 0;
            for (auto & partial_result : partial_results)
                result = Kernel::template combine<ResultType>(result, partial_result, params);

            for (; i < count; ++i)
                result = Kernel::template accumulate<ResultType>(result, row_data[i], params);

            result_data[row] = Kernel::finalize(result, params);
            prev = offsets[row];
        }
    }))


template <class Kernel>
class FunctionArrayNorm final : public IFunction
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

        if constexpr (std::is_same_v<ResultType, ArgumentType>
            && (std::is_same_v<ResultType, Float32> || std::is_same_v<ResultType, Float64>))
        {
            /// SIMD-optimized path for same-type floating point: the entire row loop is handled in a single
            /// multitarget call, runtime-dispatched to AVX-512 when available, else the baseline variant.
            /// This keeps the load stream continuous across rows (see `normBatchImpl`).
#if USE_MULTITARGET_CODE
            if (isArchSupported(TargetArch::x86_64_v4))
                normBatchImpl_x86_64_v4<Kernel, ResultType>(data.data(), offsets.data(), result_data.data(), input_rows_count, kernel_params);
            else
#endif
                normBatchImpl<Kernel, ResultType>(data.data(), offsets.data(), result_data.data(), input_rows_count, kernel_params);
        }
        else
        {
            /// Scalar path for widened types (integers cast up to Float64, BFloat16 cast to Float32).
            ColumnArray::Offset prev = 0;
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const auto off = offsets[row];
                const size_t array_size = off - prev;

                static constexpr size_t VEC_SIZE = 4;
                ResultType results[VEC_SIZE] = {0};
                size_t i = 0;
                for (; i + VEC_SIZE < array_size; i += VEC_SIZE)
                    for (size_t s = 0; s < VEC_SIZE; ++s)
                        results[s] = Kernel::template accumulate<ResultType>(results[s], static_cast<ResultType>(data[prev + i + s]), kernel_params);

                ResultType result = 0;
                for (const auto & other_state : results)
                    result = Kernel::template combine<ResultType>(result, other_state, kernel_params);

                for (; i < array_size; ++i)
                    result = Kernel::template accumulate<ResultType>(result, static_cast<ResultType>(data[prev + i]), kernel_params);

                result_data[row] = Kernel::finalize(result, kernel_params);
                prev = off;
            }
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
FunctionPtr createFunctionArrayL1Norm(ContextPtr context_);
FunctionPtr createFunctionArrayL2Norm(ContextPtr context_);
FunctionPtr createFunctionArrayL2SquaredNorm(ContextPtr context_);
FunctionPtr createFunctionArrayLpNorm(ContextPtr context_);
FunctionPtr createFunctionArrayLinfNorm(ContextPtr context_);
FunctionPtr createFunctionArrayL1Norm(ContextPtr context_) { return FunctionArrayNorm<L1Norm>::create(context_); }
FunctionPtr createFunctionArrayL2Norm(ContextPtr context_) { return FunctionArrayNorm<L2Norm>::create(context_); }
FunctionPtr createFunctionArrayL2SquaredNorm(ContextPtr context_) { return FunctionArrayNorm<L2SquaredNorm>::create(context_); }
FunctionPtr createFunctionArrayLpNorm(ContextPtr context_) { return FunctionArrayNorm<LpNorm>::create(context_); }
FunctionPtr createFunctionArrayLinfNorm(ContextPtr context_) { return FunctionArrayNorm<LinfNorm>::create(context_); }

}
