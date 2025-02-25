#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/TargetSpecific.h>
#include "base/types.h"
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <base/range.h>
#include <cstring>

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

struct L2DistanceTransposed
{
    static constexpr auto name = "L2T";

    struct ConstParams {
        int p;
    };

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

template <typename Kernel>
class FunctionArrayDistanceTransposed : public IFunction
{
public:
    String getName() const override
    {
        static auto name = String("array") + Kernel::name + "Distance";
        return name;
    }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayDistanceTransposed<Kernel>>(); }
    size_t getNumberOfArguments() const override { return 3; }
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
        const auto & common_type = getLeastSupertype(types);
        switch (common_type->getTypeId())
        {
            case TypeIndex::Float64:
                return std::make_shared<DataTypeFloat64>();
            case TypeIndex::Float32:
                return std::make_shared<DataTypeFloat32>();
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Supported types: Float32, Float64.",
                    getName(),
                    common_type->getName());
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
                    "Arguments of function {} has nested type {}. "
                    "Supported types: Float32, Float64.",
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
                    "Arguments of function {} has nested type {}. "
                    "Supported types: Float32, Float64.",
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
        ColumnArray::Offset curr = 0;
        size_t row = 0;
        int numbits = static_cast<int>(sizeof(ResultType)) * 8;
        using BitType = std::conditional_t<std::is_same_v<ResultType, Float32>, uint32_t, uint64_t>;
        BitType x = 0;
        BitType y = 0;
        BitType bits = 0;

        for (auto off : offsets_x)
        {
            /// Process chunks in vectorized manner
            static constexpr size_t VEC_SIZE = 4;
            size_t array_size = off - prev;
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; curr + VEC_SIZE < off; curr += VEC_SIZE)
            {
                for (size_t s = 0; s < VEC_SIZE; ++s)
                {
                    x=0; y=0; bits = 0;
                    size_t ind = curr + s - prev;
                    for(int j=0; j < numbits; j++){
                        size_t idx = (array_size * j + ind) / numbits;
                        int bitpos = (array_size * j + ind) % numbits;
                        memcpy(&bits, &data_x[idx], sizeof(bits));
                        bool bx = (bits & (static_cast<BitType>(1) << (numbits - 1 - bitpos)));
                        memcpy(&bits, &data_y[idx], sizeof(bits));
                        bool by = (bits & (static_cast<BitType>(1) << (numbits - 1 - bitpos)));
                        x |= (static_cast<BitType>(bx) << j);
                        y |= (static_cast<BitType>(by) << j);
                    }
                    Kernel::template accumulate<ResultType>(
                        states[s], std::bit_cast<ResultType>(x), std::bit_cast<ResultType>(y), kernel_params);
                }
            }

            typename Kernel::template State<ResultType> state;
            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state, kernel_params);

            /// Process the tail
            for (; curr < off; ++curr)
            {
                x=0; y=0; bits = 0;
                size_t ind = curr - prev;
                for(int j=0; j < numbits; j++){
                    size_t idx = (array_size * j + ind) / numbits;
                    int bitpos = (array_size * j + ind) % numbits;
                    memcpy(&bits, &data_x[idx], sizeof(bits));
                    bool bx = (bits & (static_cast<BitType>(1) << (numbits - 1 - bitpos)));
                    memcpy(&bits, &data_y[idx], sizeof(bits));
                    bool by = (bits & (static_cast<BitType>(1) << (numbits - 1 - bitpos)));
                    x |= (static_cast<BitType>(bx) << j);
                    y |= (static_cast<BitType>(by) << j);
                }
                Kernel::template accumulate<ResultType>(
                    state, std::bit_cast<ResultType>(x), std::bit_cast<ResultType>(y), kernel_params);
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

        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        auto result = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result->getData();

        size_t prev = 0;
        size_t row = 0;

        for (auto off : offsets_y)
        {
            size_t i = 0;
            typename Kernel::template State<ResultType> state;
            bool processed = false;
            if (!processed)
            {
                /// Process chunks in a vectorized manner.
                static constexpr size_t VEC_SIZE = 32;
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

/// These functions are used by TupleOrArrayFunction
FunctionPtr createFunctionArrayL2DistanceTransposed(ContextPtr context_) { return FunctionArrayDistanceTransposed<L2DistanceTransposed>::create(context_); }
}
