#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Types.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>
#include <Common/Concepts.h>
#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <Common/register_objects.h>
#include <base/range.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <string>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

class FunctionWidthBucket : public IFunction
{
    template <typename TDataType>
    void throwIfInvalid(
        const size_t argument_index,
        const ColumnConst * col_const,
        const typename ColumnVector<TDataType>::Container * col_vec,
        const size_t expected_size) const
    {
        if ((nullptr == col_const) ^ (nullptr != col_vec && col_vec->size() == expected_size))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Logical error in function {}: argument {} has unexpected type or size.",
                getName(),
                argument_index);
        }
    }

    template <typename TDataType>
    const typename ColumnVector<TDataType>::Container * getDataIfNotNull(const ColumnVector<TDataType> * col_vec) const
    {
        if (nullptr == col_vec)
        {
            return nullptr;
        }
        return &col_vec->getData();
    }

    template <typename TDataType>
    static TDataType
    getValue(const ColumnConst * col_const, const typename ColumnVector<TDataType>::Container * col_vec, const size_t index)
    {
        if (nullptr != col_const)
        {
            return col_const->getValue<TDataType>();
        }
        return col_vec->data()[index];
    }

    static Float64 calculateRelativeBucket(const Float64 operand, const Float64 low, const Float64 high)
    {
        return (operand - low) / (high - low);
    }

    template <typename TResultType, typename TCountType>
    std::optional<TResultType> checkArguments(const Float64 operand, const Float64 low, const Float64 high, const TCountType count) const
    {
        if (count == 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Last argument (count) for function {} cannot be 0.", getName());
        }
        if (isNaN(operand) || isNaN(low) || isNaN(high))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The first three arguments (operand, low, high) cannot be NaN in function {}", getName());
        }
        // operand can be infinity, the following conditions will take care of it
        if (!isFinite(low) || !isFinite(high))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second and third arguments (low, high) cannot be Inf function {}", getName());
        }
        if (operand < low || low >= high)
        {
            return 0;
        }
        if (operand >= high)
        {
            return count + 1;
        }
        return std::nullopt;
    }

    template <typename TResultType, typename TCountType>
    TResultType NO_SANITIZE_UNDEFINED calculate(const Float64 operand, const Float64 low, const Float64 high, const TCountType count) const
    {
        if (const auto maybe_early_return = checkArguments<TResultType>(operand, low, high, count); maybe_early_return.has_value())
        {
            return *maybe_early_return;
        }

        const auto relative_bucket = calculateRelativeBucket(operand, low, high);

        if (isNaN(relative_bucket) || !isFinite(relative_bucket))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "The calculation resulted in NaN or Inf which is unexpected in function {}.", getName());
        }
        return static_cast<TResultType>(count * relative_bucket + 1);
    }

    template <is_any_of<UInt8, UInt16, UInt32, UInt64> TCountType>
    ColumnPtr executeForResultType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        using ResultType = typename NumberTraits::Construct<false, false, NumberTraits::nextSize(sizeof(TCountType))>::Type;
        auto common_type = std::make_shared<DataTypeNumber<Float64>>();

        std::vector<ColumnPtr> casted_columns;
        casted_columns.reserve(3);
        for (const auto argument_index : collections::range(0, 3))
        {
            casted_columns.push_back(castColumn(arguments[argument_index], common_type));
        }

        const auto * operands_vec = getDataIfNotNull(checkAndGetColumn<ColumnVector<Float64>>(casted_columns[0].get()));
        const auto * lows_vec = getDataIfNotNull(checkAndGetColumn<ColumnVector<Float64>>(casted_columns[1].get()));
        const auto * highs_vec = getDataIfNotNull(checkAndGetColumn<ColumnVector<Float64>>(casted_columns[2].get()));
        const auto * counts_vec = getDataIfNotNull(checkAndGetColumn<ColumnVector<TCountType>>(arguments[3].column.get()));

        const auto * operands_col_const = checkAndGetColumnConst<ColumnVector<Float64>>(casted_columns[0].get());
        const auto * lows_col_const = checkAndGetColumnConst<ColumnVector<Float64>>(casted_columns[1].get());
        const auto * highs_col_const = checkAndGetColumnConst<ColumnVector<Float64>>(casted_columns[2].get());
        const auto * counts_col_const = checkAndGetColumnConst<ColumnVector<TCountType>>(arguments[3].column.get());

        throwIfInvalid<Float64>(0, operands_col_const, operands_vec, input_rows_count);
        throwIfInvalid<Float64>(1, lows_col_const, lows_vec, input_rows_count);
        throwIfInvalid<Float64>(2, highs_col_const, highs_vec, input_rows_count);
        throwIfInvalid<TCountType>(4, counts_col_const, counts_vec, input_rows_count);

        const auto are_all_const_cols
            = nullptr != operands_col_const && nullptr != lows_col_const && nullptr != highs_col_const && nullptr != counts_col_const;


        if (are_all_const_cols)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Logical error in function {}: unexpected combination of argument types.", getName());
        }

        auto result_column = ColumnVector<ResultType>::create();
        result_column->reserve(1);
        auto & result_data = result_column->getData();

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const auto operand = getValue<Float64>(operands_col_const, operands_vec, row);
            const auto low = getValue<Float64>(lows_col_const, lows_vec, row);
            const auto high = getValue<Float64>(highs_col_const, highs_vec, row);
            const auto count = getValue<TCountType>(counts_col_const, counts_vec, row);
            result_data.push_back(calculate<ResultType>(operand, low, high, count));
        }

        return result_column;
    }

public:
    static inline const char * name = "widthBucket";

    explicit FunctionWidthBucket() = default;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionWidthBucket>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto argument_index : collections::range(0, 3))
        {
            if (!isNativeNumber(arguments[argument_index]))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first three arguments of function {} must be a Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32 "
                    "or Float64.",
                    getName());
            }
        }
        if (!WhichDataType(arguments[3]).isNativeUInt())
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

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        switch (arguments[3].type->getTypeId())
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
    factory.registerFunction<FunctionWidthBucket>(FunctionDocumentation{
        .description=R"(
Returns the number of the bucket in which `operand` falls in a histogram having `count` equal-width buckets spanning the range `low` to `high`. Returns `0` if `operand < low`, and returns `count+1` if `operand >= high`.

`operand`, `low`, `high` can be any native number type. `count` can only be unsigned native integer and its value cannot be zero.

**Syntax**

```sql
widthBucket(operand, low, high, count)
```

There is also a case insensitive alias called `WIDTH_BUCKET` to provide compatibility with other databases.

**Example**

Query:
[example:simple]

Result:

``` text
┌─widthBucket(10.15, -8.6, 23, 18)─┐
│                               11 │
└──────────────────────────────────┘
```
)",
        .examples{
            {"simple", "SELECT widthBucket(10.15, -8.6, 23, 18)", ""},
        },
        .categories{"Mathematical"},
    });

    factory.registerAlias("width_bucket", "widthBucket", FunctionFactory::Case::Insensitive);
}

}
