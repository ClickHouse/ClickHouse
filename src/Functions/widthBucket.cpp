#include <Columns/ColumnVector.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/register_objects.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <string>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionWidthBucket : public IFunction
{
    template <is_any_of<UInt8, UInt16, UInt32, UInt64> TResultType>
    static ColumnPtr executeForResultType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count)
    {
        auto result_column = ColumnVector<TResultType>::create();
        auto & results = result_column->getData();
        results.reserve(input_rows_count);
        auto common_type = std::make_shared<DataTypeNumber<Float64>>();

        std::vector<ColumnPtr> casted_columns;
        casted_columns.reserve(3);
        for (const auto & arg : arguments)
        {
            casted_columns.push_back(castColumn(arg, common_type));
        }

        const auto & operands = checkAndGetColumn<ColumnVector<Float64>>(casted_columns[0].get())->getData();
        const auto & mins = checkAndGetColumn<ColumnVector<Float64>>(casted_columns[1].get())->getData();
        const auto & maxs = checkAndGetColumn<ColumnVector<Float64>>(casted_columns[2].get())->getData();
        const auto & counts = checkAndGetColumn<ColumnVector<TResultType>>(arguments[3].column.get())->getData();
        assert(operands.size() == input_rows_count);
        assert(mins.size() == input_rows_count);
        assert(maxs.size() == input_rows_count);
        assert(counts.size() == input_rows_count);

        for (auto i{0u}; i < input_rows_count; ++i)
        {
            const auto operand = operands[i];
            const auto min = mins[i];
            const auto max = maxs[i];
            const auto count = counts[i];
            if (operand < min || min >= max)
            {
                results.push_back(0);
            }
            else if (operand >= max)
            {
                results.push_back(count + 1);
            }
            else
            {
                results.push_back(static_cast<TResultType>(count * ((operand - min) / (max - min)) + 1));
            }
        }

        return result_column;
    }

public:
    static inline const char * name = "width_bucket";

    explicit FunctionWidthBucket() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionWidthBucket>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (auto i{0u}; i < 3; ++i)
        {
            const auto & data_type = arguments[i];
            const auto is_integer_or_float = isInteger(data_type) || isFloat(data_type);
            const auto extended = is_integer_or_float ? data_type->getSizeOfValueInMemory() > 8 : false;
            if (!is_integer_or_float || extended)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first three arguments of function {} must be Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32 or "
                    "Float64.",
                    getName());
            }
        }
        if (!isUnsignedInteger(arguments[3]) || arguments[3]->getSizeOfValueInMemory() > 8)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The last argument of function {} must be UInt8, UInt16, UInt32 or UInt64, found {}.",
                getName(),
                arguments[3]->getName());
        }
        switch (arguments[3]->getTypeId())
        {
            case TypeIndex::UInt8:
                return std::make_shared<DataTypeUInt16>();
            case TypeIndex::UInt16:
                return std::make_shared<DataTypeUInt32>();
            case TypeIndex::UInt32:
                [[fallthrough]];
            case TypeIndex::UInt64:
                return std::make_shared<DataTypeUInt64>();
            default:
                break;
        }

        UNREACHABLE();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        switch (result_type->getTypeId())
        {
            case TypeIndex::UInt8:
                return executeForResultType<UInt8>(arguments, input_rows_count);
            case TypeIndex::UInt16:
                return executeForResultType<UInt16>(arguments, input_rows_count);
            case TypeIndex::UInt32:
                return executeForResultType<UInt32>(arguments, input_rows_count);
            case TypeIndex::UInt64:
                return executeForResultType<UInt64>(arguments, input_rows_count);
            default:
                break;
        }

        UNREACHABLE();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};

REGISTER_FUNCTION(WidthBucket)
{
    factory.registerFunction<FunctionWidthBucket>({}, FunctionFactory::CaseInsensitive);
}

}
