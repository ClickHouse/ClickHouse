#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include "base/range.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}

struct L1Distance
{
    static inline String name = "L1";

    template <typename FloatType>
    struct State
    {
        FloatType sum = 0;
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y)
    {
        state.sum += fabs(x - y);
    }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state)
    {
        return state.sum;
    }
};

struct L2Distance
{
    static inline String name = "L2";

    template <typename FloatType>
    struct State
    {
        FloatType sum = 0;
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y)
    {
        state.sum += (x - y) * (x - y);
    }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state)
    {
        return sqrt(state.sum);
    }
};

struct LinfDistance
{
    static inline String name = "Linf";

    template <typename FloatType>
    struct State
    {
        FloatType dist = 0;
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y)
    {
        state.dist = fmax(state.dist, fabs(x - y));
    }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state)
    {
        return state.dist;
    }
};
struct CosineDistance
{
    static inline String name = "Cosine";

    template <typename FloatType>
    struct State
    {
        FloatType dot_prod = 0;
        FloatType x_squared = 0;
        FloatType y_squared = 0;
    };

    template <typename ResultType>
    static void accumulate(State<ResultType> & state, ResultType x, ResultType y)
    {
        state.dot_prod += x * y;
        state.x_squared += x * x;
        state.y_squared += y * y;
    }

    template <typename ResultType>
    static ResultType finalize(const State<ResultType> & state)
    {
        return 1 - state.dot_prod / sqrt(state.x_squared * state.y_squared);
    }
};

template <class Kernel>
class FunctionArrayDistance : public IFunction
{
public:
    static inline auto name = "array" + Kernel::name + "Distance";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayDistance<Kernel>>(); }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes types;
        for (const auto & argument : arguments)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(argument.type.get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must be array.", getName());

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
            case TypeIndex::Float32:
                return std::make_shared<DataTypeFloat32>();
            case TypeIndex::UInt64:
            case TypeIndex::Int64:
            case TypeIndex::Float64:
                return std::make_shared<DataTypeFloat64>();
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    getName(), common_type->getName());
        }
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type.");
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
                    getName(), type_x->getName());
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
                return executeWithTypes<ResultType, FirstArgType, type>(arguments[0].column, arguments[1].column, input_rows_count); \
                break;

            SUPPORTED_TYPES(ON_TYPE)
        #undef ON_TYPE

            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    getName(), type_y->getName());
        }
    }

    template <typename ResultType, typename FirstArgType, typename SecondArgType>
    ColumnPtr executeWithTypes(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count) const
    {
        if (typeid_cast<const ColumnConst *>(col_x.get()))
        {
            return executeWithTypesFirstArgConst<ResultType, FirstArgType, SecondArgType>(col_x, col_y, input_rows_count);
        }
        else if (typeid_cast<const ColumnConst *>(col_y.get()))
        {
            return executeWithTypesFirstArgConst<ResultType, SecondArgType, FirstArgType>(col_y, col_x, input_rows_count);
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
                    getName(), offsets_x[row] - prev_offset, offsets_y[row] - prev_offset);
            }
        }

        auto result = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result->getData();

        /// Do the actual computation
        ColumnArray::Offset prev = 0;
        size_t row = 0;
        for (auto off : offsets_x)
        {
            typename Kernel::template State<Float64> state;
            for (; prev < off; ++prev)
            {
                Kernel::template accumulate<Float64>(state, data_x[prev], data_y[prev]);
            }
            result_data[row] = Kernel::finalize(state);
            row++;
        }
        return result;
    }

    /// Special case when the 1st parameter is Const
    template <typename ResultType, typename FirstArgType, typename SecondArgType>
    ColumnPtr executeWithTypesFirstArgConst(ColumnPtr col_x, ColumnPtr col_y, size_t input_rows_count) const
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
                    getName(), offsets_x[0], offsets_y[row] - prev_offset);
            }
            prev_offset = offsets_y[row];
        }

        auto result = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result->getData();

        /// Do the actual computation
        ColumnArray::Offset prev = 0;
        size_t row = 0;
        for (auto off : offsets_y)
        {
            typename Kernel::template State<Float64> state;
            for (size_t i = 0; prev < off; ++i, ++prev)
            {
                Kernel::template accumulate<Float64>(state, data_x[i], data_y[prev]);
            }
            result_data[row] = Kernel::finalize(state);
            row++;
        }
        return result;
    }

};

void registerFunctionArrayDistance(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayDistance<L1Distance>>();
    factory.registerFunction<FunctionArrayDistance<L2Distance>>();
    factory.registerFunction<FunctionArrayDistance<LinfDistance>>();
    factory.registerFunction<FunctionArrayDistance<CosineDistance>>();
}

}
