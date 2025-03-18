#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
// TODO: add support for BFloat16 and Float64

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
    static constexpr auto name = "L2DistanceTransposed";
    struct ConstParams { UInt8 groups; };

    template <typename FloatType>
    struct State { FloatType sum{}; };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y, const ConstParams &) { state.sum += (x - y) * (x - y); }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other, const ConstParams &) { state.sum += other.sum; }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams &) { return sqrt(state.sum); }
};

template <typename Kernel>
class FunctionArrayDistance : public IFunction
{
public:
    String getName() const override
    {
        static auto name = String("array") + Kernel::name;
        return name;
    }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayDistance<Kernel>>(); }
    size_t getNumberOfArguments() const override { return 3; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes types;
        for (size_t i = 0; i < 2; ++i)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of function {} must be array.", i,
                    getName());

            types.push_back(array_type->getNestedType());
        }
        const auto & common_type = getLeastSupertype(types);
        switch (common_type->getTypeId())
        {
            case TypeIndex::Float32:
                return std::make_shared<DataTypeFloat32>();
            default:
                throw Exception( ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested or unsupported type {}. Supported types: Float32.",
                    getName(), common_type->getName());
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        switch (result_type->getTypeId())
        {
            case TypeIndex::Float32:
                return executeWithResultType<Float32>(arguments, input_rows_count);
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type {}", result_type->getName());
        }
    }


#define SUPPORTED_TYPES(ACTION) \
    ACTION(Float32)


private:
    static void untransposeBits(const Float32 * in_floats, Float32 * out_floats, size_t length, size_t p)
    {
        p = std::min<size_t>(p, 32);

        const auto * in_u32 = reinterpret_cast<const UInt32 *>(in_floats);
        auto * out_u32 = reinterpret_cast<UInt32 *>(out_floats);

        for (size_t i = 0; i < length; i++)
            out_u32[i] = 0;

        for (size_t bit = 0; bit < p; bit++)
        {
            for (size_t j = 0; j < length; j++)
            {
                const size_t in_bit_index = bit * length + j;
                const size_t in_word_index = in_bit_index >> 5; // / 32
                const size_t in_bit_offset = in_bit_index & 0x1F; // % 32
                const UInt32 b = (in_u32[in_word_index] >> (31 - in_bit_offset)) & 1;
                out_u32[j] |= (b << (31 - bit));
            }
        }
    }

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
                throw Exception( ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested or unsupported type {}. Supported types: Float32.", getName(), type_x->getName());
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
                throw Exception( ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested or unsupported type {}. Supported types: Float32.", getName(),
                    type_y->getName());
        }
    }

    template <typename ResultType, typename LeftType, typename RightType>
    ColumnPtr executeWithResultTypeAndLeftTypeAndRightType(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count, const ColumnsWithTypeAndName & arguments) const
    {
        if (col_x->isConst() || col_y->isConst())
        {
            const bool is_left_const = col_x->isConst();
            return executeWithArgConst<ResultType, LeftType, RightType>(col_x, col_y, input_rows_count, arguments, is_left_const);
        }

        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & data_x = typeid_cast<const ColumnVector<LeftType> &>(array_x.getData()).getData();
        const auto & data_y = typeid_cast<const ColumnVector<RightType> &>(array_y.getData()).getData();

        const auto & offsets_x = array_x.getOffsets();

        if (!array_x.hasEqualOffsets(array_y))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Array arguments for function {} must have equal sizes",
                getName());

        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        auto col_res = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = col_res->getData();

        ColumnArray::Offset prev = 0;
        size_t row = 0;
        ColumnArray::Offset prev_offset = 0;

        for (auto off : offsets_x)
        {
            size_t array_size = off - prev_offset;

            std::vector<LeftType> untransposed_x(array_size);
            untransposeBits(&data_x[prev_offset], untransposed_x.data(), array_size, kernel_params.groups);

            /// Process chunks in vectorized manner
            static constexpr size_t VEC_SIZE = 16; /// the choice of the constant has no huge performance impact. 16 seems the best.
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; prev + VEC_SIZE < off; prev += VEC_SIZE)
            {
                for (size_t s = 0; s < VEC_SIZE; ++s)
                    Kernel::template accumulate<ResultType>(states[s], static_cast<ResultType>(untransposed_x.data()[prev + s]),
                        static_cast<ResultType>(data_y.data()[prev + s]), kernel_params);
            }

            typename Kernel::template State<ResultType> state;
            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state, kernel_params);

            /// Process the tail
            for (; prev < off; ++prev)
            {
                Kernel::template accumulate<ResultType>(
                    state, static_cast<ResultType>(untransposed_x.data()[prev]), static_cast<ResultType>(data_y.data()[prev]), kernel_params);
            }
            result_data[row] = Kernel::finalize(state, kernel_params);
            ++row;
            prev_offset = off;
        }
        return col_res;
    }

    /// Special case when the 1st parameter is Const
    template <typename ResultType, typename LeftType, typename RightType>
    ColumnPtr executeWithArgConst(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count, const ColumnsWithTypeAndName & arguments, const bool is_left_const) const
    {
        if (is_left_const)
        {
            col_x = assert_cast<const ColumnConst *>(col_x.get())->getDataColumnPtr();
            col_y = col_y->convertToFullColumnIfConst();
        }
        else
        {
            col_x = col_x->convertToFullColumnIfConst();
            col_y = assert_cast<const ColumnConst *>(col_y.get())->getDataColumnPtr();
        }

        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & data_x = typeid_cast<const ColumnVector<LeftType> &>(array_x.getData()).getData();
        const auto & data_y = typeid_cast<const ColumnVector<RightType> &>(array_y.getData()).getData();

        const auto & offsets_x = array_x.getOffsets();
        const auto & offsets_y = array_y.getOffsets();

        const auto & const_offsets = is_left_const ? offsets_x : offsets_y;
        const auto & var_offsets = is_left_const ? offsets_y : offsets_x;

        ColumnArray::Offset prev_offset = 0;

        for (auto var_offset : var_offsets)
        {
            if (const_offsets[0] != var_offset - prev_offset)
                throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "Array arguments for function {} must have equal sizes", getName());
            prev_offset = var_offset;
        }


        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        auto result = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result->getData();

        size_t prev = 0;
        size_t row = 0;
        prev_offset = 0;

        for (auto off : offsets_y)
        {
            size_t i = 0;
            typename Kernel::template State<ResultType> state;

            size_t array_size = off - prev_offset;

            std::vector<LeftType> untransposed_x(array_size);
            untransposeBits(&data_x[prev_offset], untransposed_x.data(), array_size, kernel_params.groups);

            /// Process chunks in a vectorized manner.
            static constexpr size_t VEC_SIZE = 16; /// the choice of the constant has no huge performance impact. 16 seems the best.
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; prev + VEC_SIZE < off; i += VEC_SIZE, prev += VEC_SIZE)
            {
                for (size_t s = 0; s < VEC_SIZE; ++s)
                    Kernel::template accumulate<ResultType>(states[s], static_cast<ResultType>(untransposed_x.data()[i + s]),
                        static_cast<ResultType>(data_y[prev + s]), kernel_params);
            }

            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state, kernel_params);


            /// Process the tail.
            for (; prev < off; ++i, ++prev)
            {
                Kernel::template accumulate<ResultType>(
                    state, static_cast<ResultType>(untransposed_x.data()[i]), static_cast<ResultType>(data_y[prev]), kernel_params);
            }
            result_data[row] = Kernel::finalize(state, kernel_params);
            row++;

            prev_offset = off;
        }

        return result;
    }

    typename Kernel::ConstParams initConstParams(const ColumnsWithTypeAndName &) const { return {}; }
};

template <>
size_t FunctionArrayDistance<L2DistanceTransposed>::getNumberOfArguments() const { return 3; }

template <>
ColumnNumbers FunctionArrayDistance<L2DistanceTransposed>::getArgumentsThatAreAlwaysConstant() const { return {2}; }

template <>
L2DistanceTransposed::ConstParams FunctionArrayDistance<L2DistanceTransposed>::initConstParams(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() < 3)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Argument p of function {} was not provided", getName());

    if (!arguments[2].column->isNumeric())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument p of function {} must be numeric constant", getName());

    if (!isColumnConst(*arguments[2].column) && arguments[2].column->size() != 1)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be constant UInt8", getName());

    UInt8 p = arguments[2].column->getUInt(0);
    if (p < 1 || p >= HUGE_VAL)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Second argument for function {} must be not less than one and not be an infinity", getName());

    return L2DistanceTransposed::ConstParams{p};
}


/// Used by TupleOrArrayFunction
FunctionPtr createFunctionArrayL2DistanceTransposed(ContextPtr context_) { return FunctionArrayDistance<L2DistanceTransposed>::create(context_); }

}
