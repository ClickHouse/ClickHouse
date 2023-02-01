#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <base/range.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

struct L1Distance
{
    static constexpr auto name = "L1";

    struct ConstParams {};

    template <typename FloatType>
    struct State
    {
        FloatType sum = 0;
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
        FloatType sum = 0;
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
        FloatType sum = 0;
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y, const ConstParams & params)
    {
        state.sum += std::pow(fabs(x - y), params.power);
    }

    template <typename ResultType>
    static void combine(State<ResultType> & state, const State<ResultType> & other_state, const ConstParams &)
    {
        state.sum += other_state.sum;
    }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state, const ConstParams & params)
    {
        return std::pow(state.sum, params.inverted_power);
    }
};

struct LinfDistance
{
    static constexpr auto name = "Linf";

    struct ConstParams {};

    template <typename FloatType>
    struct State
    {
        FloatType dist = 0;
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
        FloatType dot_prod = 0;
        FloatType x_squared = 0;
        FloatType y_squared = 0;
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
        return 1 - state.dot_prod / sqrt(state.x_squared * state.y_squared);
    }
};

template <class Kernel>
class FunctionArrayDistance : public IFunction
{
public:
    String getName() const override { static auto name = String("array") + Kernel::name + "Distance"; return name; }
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
        const auto & common_type = getLeastSupertype(types);
        switch (common_type->getTypeId())
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::UInt64:
            case TypeIndex::Int64:
            case TypeIndex::Float64:
                return std::make_shared<DataTypeFloat64>();
            case TypeIndex::Float32:
                return std::make_shared<DataTypeFloat32>();
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
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
                break;
            case TypeIndex::Float64:
                return executeWithResultType<Float64>(arguments, input_rows_count);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type {}", result_type->getName());
        }
    }


#define SUPPORTED_TYPES(action) \
    action(UInt8)   \
    action(UInt16)  \
    action(UInt32)  \
    action(UInt64)  \
    action(Int8)    \
    action(Int16)   \
    action(Int32)   \
    action(Int64)   \
    action(Float32) \
    action(Float64)


private:
    template <typename ResultType>
    ColumnPtr executeWithResultType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        DataTypePtr type_x = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();

        /// Dynamic disaptch based on the 1st argument type
        switch (type_x->getTypeId())
        {
        #define ON_TYPE(type) \
            case TypeIndex::type: \
                return executeWithFirstType<ResultType, type>(arguments, input_rows_count); \
                break;

            SUPPORTED_TYPES(ON_TYPE)
        #undef ON_TYPE

            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    getName(),
                    type_x->getName());
        }
    }

    template <typename ResultType, typename FirstArgType>
    ColumnPtr executeWithFirstType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        DataTypePtr type_y = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

        /// Dynamic disaptch based on the 2nd argument type
        switch (type_y->getTypeId())
        {
        #define ON_TYPE(type) \
            case TypeIndex::type: \
                return executeWithTypes<ResultType, FirstArgType, type>(arguments[0].column, arguments[1].column, input_rows_count, arguments); \
                break;

            SUPPORTED_TYPES(ON_TYPE)
        #undef ON_TYPE

            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    getName(),
                    type_y->getName());
        }
    }

    template <typename ResultType, typename FirstArgType, typename SecondArgType>
    ColumnPtr executeWithTypes(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count, const ColumnsWithTypeAndName & arguments) const
    {
        if (typeid_cast<const ColumnConst *>(col_x.get()))
        {
            return executeWithTypesFirstArgConst<ResultType, FirstArgType, SecondArgType>(col_x, col_y, input_rows_count, arguments);
        }
        else if (typeid_cast<const ColumnConst *>(col_y.get()))
        {
            return executeWithTypesFirstArgConst<ResultType, SecondArgType, FirstArgType>(col_y, col_x, input_rows_count, arguments);
        }

        col_x = col_x->convertToFullColumnIfConst();
        col_y = col_y->convertToFullColumnIfConst();

        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & data_x = typeid_cast<const ColumnVector<FirstArgType> &>(array_x.getData()).getData();
        const auto & data_y = typeid_cast<const ColumnVector<SecondArgType> &>(array_y.getData()).getData();

        const auto & offsets_x = array_x.getOffsets();
        const auto & offsets_y = array_y.getOffsets();

        /// Check that arrays in both columns are the sames size
        for (size_t row = 0; row < offsets_x.size(); ++row)
        {
            if (unlikely(offsets_x[row] != offsets_y[row]))
            {
                ColumnArray::Offset prev_offset = row > 0 ? offsets_x[row] : 0;
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH,
                    "Arguments of function {} have different array sizes: {} and {}",
                    getName(),
                    offsets_x[row] - prev_offset,
                    offsets_y[row] - prev_offset);
            }
        }

        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        auto result = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result->getData();

        /// Do the actual computation
        ColumnArray::Offset prev = 0;
        size_t row = 0;
        for (auto off : offsets_x)
        {
            /// Process chunks in vectorized manner
            static constexpr size_t VEC_SIZE = 4;
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            for (; prev + VEC_SIZE < off; prev += VEC_SIZE)
            {
                for (size_t s = 0; s < VEC_SIZE; ++s)
                    Kernel::template accumulate<ResultType>(states[s], data_x[prev+s], data_y[prev+s], kernel_params);
            }

            typename Kernel::template State<ResultType> state;
            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state, kernel_params);

            /// Process the tail
            for (; prev < off; ++prev)
            {
                Kernel::template accumulate<ResultType>(state, data_x[prev], data_y[prev], kernel_params);
            }
            result_data[row] = Kernel::finalize(state, kernel_params);
            row++;
        }
        return result;
    }

    /// Special case when the 1st parameter is Const
    template <typename ResultType, typename FirstArgType, typename SecondArgType>
    ColumnPtr executeWithTypesFirstArgConst(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count, const ColumnsWithTypeAndName & arguments) const
    {
        col_x = assert_cast<const ColumnConst *>(col_x.get())->getDataColumnPtr();
        col_y = col_y->convertToFullColumnIfConst();

        const auto & array_x = *assert_cast<const ColumnArray *>(col_x.get());
        const auto & array_y = *assert_cast<const ColumnArray *>(col_y.get());

        const auto & data_x = typeid_cast<const ColumnVector<FirstArgType> &>(array_x.getData()).getData();
        const auto & data_y = typeid_cast<const ColumnVector<SecondArgType> &>(array_y.getData()).getData();

        const auto & offsets_x = array_x.getOffsets();
        const auto & offsets_y = array_y.getOffsets();

        /// Check that arrays in both columns are the sames size
        ColumnArray::Offset prev_offset = 0;
        for (size_t row : collections::range(0, offsets_y.size()))
        {
            if (unlikely(offsets_x[0] != offsets_y[row] - prev_offset))
            {
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH,
                    "Arguments of function {} have different array sizes: {} and {}",
                    getName(),
                    offsets_x[0],
                    offsets_y[row] - prev_offset);
            }
            prev_offset = offsets_y[row];
        }

        const typename Kernel::ConstParams kernel_params = initConstParams(arguments);

        auto result = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result->getData();

        /// Do the actual computation
        ColumnArray::Offset prev = 0;
        size_t row = 0;
        for (auto off : offsets_y)
        {
            /// Process chunks in vectorized manner
            static constexpr size_t VEC_SIZE = 4;
            typename Kernel::template State<ResultType> states[VEC_SIZE];
            size_t i = 0;
            for (; prev + VEC_SIZE < off; i += VEC_SIZE, prev += VEC_SIZE)
            {
                for (size_t s = 0; s < VEC_SIZE; ++s)
                    Kernel::template accumulate<ResultType>(states[s], data_x[i+s], data_y[prev+s], kernel_params);
            }

            typename Kernel::template State<ResultType> state;
            for (const auto & other_state : states)
                Kernel::template combine<ResultType>(state, other_state, kernel_params);

            /// Process the tail
            for (; prev < off; ++i, ++prev)
            {
                Kernel::template accumulate<ResultType>(state, data_x[i], data_y[prev], kernel_params);
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
FunctionPtr createFunctionArrayL1Distance(ContextPtr context_) { return FunctionArrayDistance<L1Distance>::create(context_); }
FunctionPtr createFunctionArrayL2Distance(ContextPtr context_) { return FunctionArrayDistance<L2Distance>::create(context_); }
FunctionPtr createFunctionArrayL2SquaredDistance(ContextPtr context_) { return FunctionArrayDistance<L2SquaredDistance>::create(context_); }
FunctionPtr createFunctionArrayLpDistance(ContextPtr context_) { return FunctionArrayDistance<LpDistance>::create(context_); }
FunctionPtr createFunctionArrayLinfDistance(ContextPtr context_) { return FunctionArrayDistance<LinfDistance>::create(context_); }
FunctionPtr createFunctionArrayCosineDistance(ContextPtr context_) { return FunctionArrayDistance<CosineDistance>::create(context_); }
}
