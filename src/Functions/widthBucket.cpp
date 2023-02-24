#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

class FunctionWidthBucket : public IFunction
{
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

    template <typename TResultType, typename TCountType>
    TResultType calculate(const Float64 operand, const Float64 low, const Float64 high, const TCountType count) const
    {
        if (count == 0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Last argument (count) for function {} cannot be 0.", getName());
        }

        if (operand < low || low >= high)
        {
            return 0;
        }
        else if (operand >= high)
        {
            return count + 1;
        }

        return static_cast<TResultType>(count * ((operand - low) / (high - low)) + 1);
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

        assert((nullptr != operands_col_const) ^ (nullptr != operands_vec && operands_vec->size() == input_rows_count));
        assert((nullptr != lows_col_const) ^ (nullptr != lows_vec && lows_vec->size() == input_rows_count));
        assert((nullptr != highs_col_const) ^ (nullptr != highs_vec && highs_vec->size() == input_rows_count));
        assert((nullptr != counts_col_const) ^ (nullptr != counts_vec && counts_vec->size() == input_rows_count));

        const auto are_all_const_cols
            = nullptr != operands_col_const && nullptr != lows_col_const && nullptr != highs_col_const && nullptr != counts_col_const;


        if (are_all_const_cols)
        {
            auto result_column = ColumnVector<ResultType>::create();
            result_column->reserve(1);
            auto & result_data = result_column->getData();
            result_data.push_back(calculate<ResultType>(
                operands_col_const->getValue<Float64>(),
                lows_col_const->getValue<Float64>(),
                highs_col_const->getValue<Float64>(),
                counts_col_const->template getValue<TCountType>()));

            return ColumnConst::create(std::move(result_column), input_rows_count);
        }

        auto result_column = ColumnVector<ResultType>::create();
        result_column->reserve(1);
        auto & result_data = result_column->getData();

        for (const auto row_index : collections::range(0, input_rows_count))
        {
            const auto operand = getValue<Float64>(operands_col_const, operands_vec, row_index);
            const auto low = getValue<Float64>(lows_col_const, lows_vec, row_index);
            const auto high = getValue<Float64>(highs_col_const, highs_vec, row_index);
            const auto count = getValue<TCountType>(counts_col_const, counts_vec, row_index);
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
    factory.registerFunction<FunctionWidthBucket>({
        R"(
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
        Documentation::Examples{
            {"simple", "SELECT widthBucket(10.15, -8.6, 23, 18)"},
        },
        Documentation::Categories{"Mathematical"},
    });

    factory.registerAlias("width_bucket", "widthBucket", FunctionFactory::CaseInsensitive);
}

}
