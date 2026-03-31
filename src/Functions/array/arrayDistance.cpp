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
        state.sum += fabs(x - y);
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

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams &)
    {
        return sqrt(state.sum);
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
        state.sum += static_cast<ResultType>(std::pow(fabs(x - y), params.power));
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state, const ConstParams &)
    {
        state.sum += other_state.sum;
    }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams & params)
    {
        return static_cast<ResultType>(std::pow(state.sum, params.inverted_power));
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
        state.dist = fmax(state.dist, fabs(x - y));
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state, const ConstParams &)
    {
        state.dist = fmax(state.dist, other_state.dist);
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

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams &)
    {
        return 1.0f - state.dot_prod / sqrt(state.x_squared * state.y_squared);
    }
};

/// Pre-convert input data to ResultType before passing to the SIMD kernel.
/// This lets the kernel template use only <Kernel, ResultType> instead of
/// <Kernel, ResultType, LeftType, RightType>, reducing MULTITARGET instantiations
/// from 6 kernels * 2 result types * 11 left * 11 right * 3 arch = 4356
/// to just 6 * 2 * 3 = 36. Without this, the .o file exceeds the 50 MB CI limit.
/// When InputType already matches ResultType, no copy is made.
template <typename ResultType, typename InputType>
std::pair<const ResultType *, PaddedPODArray<ResultType>>
castData(const PaddedPODArray<InputType> & data)
{
    if constexpr (std::is_same_v<ResultType, InputType>)
        return {data.data(), {}};
    else
    {
        PaddedPODArray<ResultType> converted(data.size());
        for (size_t i = 0; i < data.size(); ++i)
            converted[i] = static_cast<ResultType>(data[i]);
        return {converted.data(), std::move(converted)};
    }
}

/// Multi-target hot loop for non-const x non-const path.
/// The MULTITARGET macro generates _x86_64_v4, _x86_64_v3, and default versions
/// so the compiler can auto-vectorize with the best available ISA.
///
/// Templated only on <Kernel, ResultType> to avoid combinatorial explosion of
/// LeftType x RightType x arch instantiations. Callers pre-convert data to ResultType.
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
    /// Multiple independent accumulators to break the data dependency chain and let
    /// the CPU execute SIMD additions in parallel across registers.
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
        /// Skip the combine loop for short arrays where no unrolled block was processed.
        if (unrolled_end > 0)
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
        /// count == array_size is guaranteed by the caller.
        (void)count;

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
        if (unrolled_end > 0)
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

        const auto [ptr_x, buf_x] = castData<ResultType>(data_x);
        const auto [ptr_y, buf_y] = castData<ResultType>(data_y);

        executeDistance<Kernel, ResultType>(
            ptr_x, ptr_y, offsets_x.data(), result_data.data(), input_rows_count, kernel_params);

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

        const auto [ptr_x, buf_x] = castData<ResultType>(data_x);
        const auto [ptr_y, buf_y] = castData<ResultType>(data_y);

        executeDistanceConst<Kernel, ResultType>(
            ptr_x, offsets_x[0], ptr_y, offsets_y.data(), result_data.data(), input_rows_count, kernel_params);

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
FunctionPtr createFunctionArrayL1Distance(ContextPtr context_) { return FunctionArrayDistance<L1Distance>::create(context_); }
FunctionPtr createFunctionArrayL2Distance(ContextPtr context_) { return FunctionArrayDistance<L2Distance>::create(context_); }
FunctionPtr createFunctionArrayL2SquaredDistance(ContextPtr context_) { return FunctionArrayDistance<L2SquaredDistance>::create(context_); }
FunctionPtr createFunctionArrayLpDistance(ContextPtr context_) { return FunctionArrayDistance<LpDistance>::create(context_); }
FunctionPtr createFunctionArrayLinfDistance(ContextPtr context_) { return FunctionArrayDistance<LinfDistance>::create(context_); }
FunctionPtr createFunctionArrayCosineDistance(ContextPtr context_) { return FunctionArrayDistance<CosineDistance>::create(context_); }

}
