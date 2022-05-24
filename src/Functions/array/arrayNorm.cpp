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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

struct L1Norm
{
    static inline String name = "L1";

    template <typename ResultType>
    inline static ResultType accumulate(ResultType result, ResultType value)
    {
        return result + fabs(value);
    }

    template <typename ResultType>
    inline static ResultType finalize(ResultType result)
    {
        return result;
    }
};

struct L2Norm
{
    static inline String name = "L2";

    template <typename ResultType>
    inline static ResultType accumulate(ResultType result, ResultType value)
    {
        return result + value * value;
    }

    template <typename ResultType>
    inline static ResultType finalize(ResultType result)
    {
        return sqrt(result);
    }
};


struct LinfNorm
{
    static inline String name = "Linf";

    template <typename ResultType>
    inline static ResultType accumulate(ResultType result, ResultType value)
    {
        return fmax(result, fabs(value));
    }

    template <typename ResultType>
    inline static ResultType finalize(ResultType result)
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
        DataTypePtr type = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();
        ColumnPtr column = arguments[0].column->convertToFullColumnIfConst();
        const auto * arr = assert_cast<const ColumnArray *>(column.get());

        switch (result_type->getTypeId())
        {
            case TypeIndex::Float32:
                return executeWithResultType<Float32>(*arr, type, input_rows_count);
                break;
            case TypeIndex::Float64:
                return executeWithResultType<Float64>(*arr, type, input_rows_count);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type.");
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
    ColumnPtr executeWithResultType(const ColumnArray & array, const DataTypePtr & nested_type, size_t input_rows_count) const
    {
        switch (nested_type->getTypeId())
        {
        #define ON_TYPE(type) \
            case TypeIndex::type: \
                return executeWithTypes<ResultType, type>(array, input_rows_count); \
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
    static ColumnPtr executeWithTypes(const ColumnArray & array, size_t input_rows_count)
    {
        const auto & data = typeid_cast<const ColumnVector<ArgumentType> &>(array.getData()).getData();
        const auto & offsets = array.getOffsets();

        auto result_col = ColumnVector<ResultType>::create(input_rows_count);
        auto & result_data = result_col->getData();

        ColumnArray::Offset prev = 0;
        size_t row = 0;
        for (auto off : offsets)
        {
            Float64 result = 0;
            for (; prev < off; ++prev)
            {
                result = Kernel::template accumulate<Float64>(result, data[prev]);
            }
            result_data[row] = Kernel::finalize(result);
            row++;
        }
        return result_col;
    }
};

void registerFunctionArrayNorm(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayNorm<L1Norm>>();
    factory.registerFunction<FunctionArrayNorm<L2Norm>>();
    factory.registerFunction<FunctionArrayNorm<LinfNorm>>();
}

}
