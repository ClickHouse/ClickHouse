#include <cmath>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

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
    static inline String name = "L1";

    struct ConstParams {};

    template <typename ResultType>
    inline static ResultType accumulate(ResultType result, ResultType value, const ConstParams &)
    {
        return result + fabs(value);
    }

    template <typename ResultType>
    inline static ResultType finalize(ResultType result, const ConstParams &)
    {
        return result;
    }
};

struct L2Norm
{
    static inline String name = "L2";

    struct ConstParams {};

    template <typename ResultType>
    inline static ResultType accumulate(ResultType result, ResultType value, const ConstParams &)
    {
        return result + value * value;
    }

    template <typename ResultType>
    inline static ResultType finalize(ResultType result, const ConstParams &)
    {
        return sqrt(result);
    }
};


struct LpNorm
{
    static inline String name = "Lp";

    struct ConstParams
    {
        Float64 power;
        Float64 inverted_power = 1 / power;
    };

    template <typename ResultType>
    inline static ResultType accumulate(ResultType result, ResultType value, const ConstParams & params)
    {
        return result + std::pow(fabs(value), params.power);
    }

    template <typename ResultType>
    inline static ResultType finalize(ResultType result, const ConstParams & params)
    {
        return std::pow(result, params.inverted_power);
    }
};

struct LinfNorm
{
    static inline String name = "Linf";

    struct ConstParams {};

    template <typename ResultType>
    inline static ResultType accumulate(ResultType result, ResultType value, const ConstParams &)
    {
        return fmax(result, fabs(value));
    }

    template <typename ResultType>
    inline static ResultType finalize(ResultType result, const ConstParams &)
    {
        return result;
    }
};


template <class Kernel>
class FunctionArrayNorm : public IFunction
{
public:
    static inline auto name = "array" + Kernel::name + "Norm";
    String getName() const override { return name; }
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
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Float32:
            case TypeIndex::UInt64:
            case TypeIndex::Int64:
            case TypeIndex::Float64:
                return std::make_shared<DataTypeFloat64>();
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    getName(), array_type->getNestedType()->getName());
        }
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        DataTypePtr type = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();
        ColumnPtr column = arguments[0].column->convertToFullColumnIfConst();
        const auto * arr = assert_cast<const ColumnArray *>(column.get());

        switch (result_type->getTypeId())
        {
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
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
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
            Float64 result = 0;
            for (; prev < off; ++prev)
            {
                result = Kernel::template accumulate<Float64>(result, data[prev], kernel_params);
            }
            result_data[row] = Kernel::finalize(result, kernel_params);
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

    if (!isColumnConst(*arguments[1].column) && arguments[1].column->size() != 1)
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
FunctionPtr createFunctionArrayLpNorm(ContextPtr context_) { return FunctionArrayNorm<LpNorm>::create(context_); }
FunctionPtr createFunctionArrayLinfNorm(ContextPtr context_) { return FunctionArrayNorm<LinfNorm>::create(context_); }

}
