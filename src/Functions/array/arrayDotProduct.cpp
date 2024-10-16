#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionBinaryArithmetic.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}


struct DotProduct
{
    static constexpr auto name = "arrayDotProduct";

    static DataTypePtr getReturnType(const DataTypePtr & left, const DataTypePtr & right)
    {
        using Types = TypeList<DataTypeFloat32, DataTypeFloat64,
                               DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64,
                               DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64>;
        Types types;

        DataTypePtr result_type;
        bool valid = castTypeToEither(types, left.get(), [&](const auto & left_)
        {
            return castTypeToEither(types, right.get(), [&](const auto & right_)
            {
                using LeftType = typename std::decay_t<decltype(left_)>::FieldType;
                using RightType = typename std::decay_t<decltype(right_)>::FieldType;
                using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<LeftType, RightType>::Type;

                if constexpr (std::is_same_v<LeftType, Float32> && std::is_same_v<RightType, Float32>)
                    result_type = std::make_shared<DataTypeFloat32>();
                else
                    result_type = std::make_shared<DataTypeFromFieldType<ResultType>>();
                return true;
            });
        });

        if (!valid)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Arguments of function {} only support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.", name);
        return result_type;
    }

    template <typename Type>
    struct State
    {
        Type sum = 0;
    };

    template <typename Type>
    static NO_SANITIZE_UNDEFINED void accumulate(State<Type> & state, Type x, Type y)
    {
        state.sum += x * y;
    }

    template <typename Type>
    static NO_SANITIZE_UNDEFINED void combine(State<Type> & state, const State<Type> & other_state)
    {
        state.sum += other_state.sum;
    }

#if USE_MULTITARGET_CODE
    template <typename Type>
    AVX512_FUNCTION_SPECIFIC_ATTRIBUTE static void accumulateCombine(
        const Type * __restrict data_x,
        const Type * __restrict data_y,
        size_t i_max,
        size_t & i,
        State<Type> & state)
    {
        static constexpr bool is_float32 = std::is_same_v<Type, Float32>;

        __m512 sums;
        if constexpr (is_float32)
            sums = _mm512_setzero_ps();
        else
            sums = _mm512_setzero_pd();

        constexpr size_t n = is_float32 ? 16 : 8;

        for (; i + n < i_max; i += n)
        {
            if constexpr (is_float32)
            {
                __m512 x = _mm512_loadu_ps(data_x + i);
                __m512 y = _mm512_loadu_ps(data_y + i);
                sums = _mm512_fmadd_ps(x, y, sums);
            }
            else
            {
                __m512 x = _mm512_loadu_pd(data_x + i);
                __m512 y = _mm512_loadu_pd(data_y + i);
                sums = _mm512_fmadd_pd(x, y, sums);
            }
        }

        if constexpr (is_float32)
            state.sum = _mm512_reduce_add_ps(sums);
        else
            state.sum = _mm512_reduce_add_pd(sums);
    }
#endif

    template <typename Type>
    static Type finalize(const State<Type> & state)
    {
        return state.sum;
    }

};


/// The implementation is modeled after the implementation of distance functions arrayL1Distance, arrayL2Distance, etc.
/// The main difference is that arrayDotProduct() interferes the result type differently.
template <typename Kernel>
class FunctionArrayScalarProduct : public IFunction
{
public:
    static constexpr auto name = Kernel::name;

    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayScalarProduct>(); }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        std::array<DataTypePtr, 2> nested_types;
        for (size_t i = 0; i < 2; ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Arguments for function {} must be of type Array", getName());

            const auto & nested_type = array_type->getNestedType();
            if (!isNativeNumber(nested_type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} cannot process values of type {}", getName(), nested_type->getName());

            nested_types[i] = nested_type;
        }

        return Kernel::getReturnType(nested_types[0], nested_types[1]);
    }

#define SUPPORTED_TYPES(ACTION) \
    ACTION(UInt8) \
    ACTION(UInt16) \
    ACTION(UInt32) \
    ACTION(UInt64) \
    ACTION(Int8) \
    ACTION(Int16) \
    ACTION(Int32) \
    ACTION(Int64) \
    ACTION(Float32) \
    ACTION(Float64)

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        switch (result_type->getTypeId())
        {
        #define ON_TYPE(type) \
            case TypeIndex::type: \
                return executeWithResultType<type>(arguments, input_rows_count); \
                break;

            SUPPORTED_TYPES(ON_TYPE)
        #undef ON_TYPE

            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type {}", result_type->getName());
        }
    }

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
                    "Arguments of function {} has nested type {}. "
                    "Supported types: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
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
                return executeWithResultTypeAndLeftTypeAndRightType<ResultType, LeftType, type>(arguments[0].column, arguments[1].column, input_rows_count); \
                break;

            SUPPORTED_TYPES(ON_TYPE)
        #undef ON_TYPE

            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Supported types: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    getName(),
                    type_y->getName());
        }
    }

    template <typename ResultType, typename LeftType, typename RightType>
    ColumnPtr executeWithResultTypeAndLeftTypeAndRightType(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count) const
    {
        if (typeid_cast<const ColumnConst *>(col_x.get()))
        {
            return executeWithLeftArgConst<ResultType, LeftType, RightType>(col_x, col_y, input_rows_count);
        }
        if (typeid_cast<const ColumnConst *>(col_y.get()))
        {
            return executeWithLeftArgConst<ResultType, RightType, LeftType>(col_y, col_x, input_rows_count);
        }

        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & data_x = typeid_cast<const ColumnVector<LeftType> &>(array_x.getData()).getData();
        const auto & data_y = typeid_cast<const ColumnVector<RightType> &>(array_y.getData()).getData();

        const auto & offsets_x = array_x.getOffsets();

        if (!array_x.hasEqualOffsets(array_y))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Array arguments for function {} must have equal sizes", getName());

        auto col_res = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = col_res->getData();

        ColumnArray::Offset current_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const size_t array_size = offsets_x[row] - current_offset;

            size_t i = 0;

            /// Process chunks in vectorized manner
            static constexpr size_t VEC_SIZE = 4;
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; i + VEC_SIZE < array_size; i += VEC_SIZE)
            {
                for (size_t j = 0; j < VEC_SIZE; ++j)
                    Kernel::template accumulate<ResultType>(states[j], static_cast<ResultType>(data_x[current_offset + i + j]), static_cast<ResultType>(data_y[current_offset + i + j]));
            }

            typename Kernel::template State<ResultType> state;
            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state);

            /// Process the tail
            for (; i < array_size; ++i)
                Kernel::template accumulate<ResultType>(state, static_cast<ResultType>(data_x[current_offset + i]), static_cast<ResultType>(data_y[current_offset + i]));

            result_data[row] = Kernel::template finalize<ResultType>(state);

            current_offset = offsets_x[row];
        }

        return col_res;
    }

    template <typename ResultType, typename LeftType, typename RightType>
    ColumnPtr executeWithLeftArgConst(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count) const
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
            if (offsets_x[0] != offset_y - prev_offset) [[unlikely]]
            {
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "Arguments of function {} have different array sizes: {} and {}",
                    getName(),
                    offsets_x[0],
                    offset_y - prev_offset);
            }
            prev_offset = offset_y;
        }

        auto col_res = ColumnVector<ResultType>::create(input_rows_count);
        auto & result = col_res->getData();

        ColumnArray::Offset current_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const size_t array_size = offsets_x[0];

            typename Kernel::template State<ResultType> state;
            size_t i = 0;

            /// SIMD optimization: process multiple elements in both input arrays at once.
            /// To avoid combinatorial explosion of SIMD kernels, focus on
            /// - the two most common input/output types (Float32 x Float32) --> Float32 and (Float64 x Float64) --> Float64 instead of 10 x
            ///   10 input types x 8 output types,
            /// - const/non-const inputs instead of non-const/non-const inputs
            /// - the most powerful SIMD instruction set (AVX-512F).
#if USE_MULTITARGET_CODE
            if constexpr ((std::is_same_v<ResultType, Float32> || std::is_same_v<ResultType, Float64>)
                            && std::is_same_v<ResultType, LeftType> && std::is_same_v<LeftType, RightType>)
            {
                if (isArchSupported(TargetArch::AVX512F))
                    Kernel::template accumulateCombine<ResultType>(&data_x[0], &data_y[current_offset], array_size, i, state);
            }
#else
            /// Process chunks in vectorized manner
            static constexpr size_t VEC_SIZE = 4;
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; i + VEC_SIZE < array_size; i += VEC_SIZE)
            {
                for (size_t j = 0; j < VEC_SIZE; ++j)
                    Kernel::template accumulate<ResultType>(states[j], static_cast<ResultType>(data_x[i + j]), static_cast<ResultType>(data_y[current_offset + i + j]));
            }

            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state);
#endif

            /// Process the tail
            for (; i < array_size; ++i)
                Kernel::template accumulate<ResultType>(state, static_cast<ResultType>(data_x[i]), static_cast<ResultType>(data_y[current_offset + i]));

            result[row] = Kernel::template finalize<ResultType>(state);

            current_offset = offsets_y[row];
        }

        return col_res;
    }
};

using FunctionArrayDotProduct = FunctionArrayScalarProduct<DotProduct>;

REGISTER_FUNCTION(ArrayDotProduct)
{
    factory.registerFunction<FunctionArrayDotProduct>();
}

// These functions are used by TupleOrArrayFunction in Function/vectorFunctions.cpp
FunctionPtr createFunctionArrayDotProduct(ContextPtr context_) { return FunctionArrayDotProduct::create(context_); }

}
