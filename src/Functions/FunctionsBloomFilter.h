#pragma once

#include <AggregateFunctions/AggregateFunctionGroupBloomFilterData.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/castColumn.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <base/wide_integer.h>
#include <base/UUID.h>
#include <base/types.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

/** Bloom filter functions.
  *
  * Check if a Bloom filter contains a value:
  * bloomFilterContains(bloom_filter_state, value) -> UInt8
  *
  * The first argument must be of type AggregateFunction(groupBloomFilter, T).
  * The second argument must be of the same type T as was used to build the filter.
  *
  * Returns 1 if the value is probably in the filter, 0 if it is definitely not.
  * Note: false positives are possible (controlled by false_positive_rate parameter),
  * but false negatives are not.
  */
class FunctionBloomFilterContains : public IFunction
{
public:
    static constexpr auto name = "bloomFilterContains";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBloomFilterContains>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * bloom_type = typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get());
        if (!(bloom_type && bloom_type->getFunctionName() == AggregateFunctionGroupBloomFilterData::name))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a Bloom filter state "
                "(AggregateFunction(groupBloomFilter, T)) but it has type {}",
                getName(), arguments[0]->getName());

        return std::make_shared<DataTypeNumber<UInt8>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnVector<UInt8>::create(input_rows_count);
        typename ColumnVector<UInt8>::Container & vec_to = col_to->getData();

        const IDataType * from_type = arguments[0].type.get();
        const DataTypeAggregateFunction * aggr_type = typeid_cast<const DataTypeAggregateFunction *>(from_type);
        DataTypes arg_data_types = aggr_type->getArgumentsDataTypes();
        const DataTypePtr & value_type = arg_data_types[0];
        DataTypePtr dispatch_value_type = removeLowCardinalityAndNullable(value_type);

        if (!canCastProbeType(arguments[1].type, dispatch_value_type))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Conversion from {} to Bloom filter value type {} is not supported for function {}",
                arguments[1].type->getName(), value_type->getName(), getName());

        /// Use an accurate cast so a narrowing probe (e.g. toUInt16(300) against a UInt8 filter) does
        /// not wrap/truncate before hashing and report a false positive; non-representable values
        /// become NULL and their result is forced to 0 below.
        ColumnPtr casted_value = castColumnAccurateOrNull(arguments[1], dispatch_value_type)->convertToFullColumnIfConst();
        const auto & nullable_value = assert_cast<const ColumnNullable &>(*casted_value);

        ColumnsWithTypeAndName casted_arguments = arguments;
        casted_arguments[1].column = nullable_value.getNestedColumnPtr();
        casted_arguments[1].type = dispatch_value_type;

        WhichDataType which(dispatch_value_type);

        // Integer types
        if (which.isUInt8())
            executeNumericType<UInt8>(casted_arguments, input_rows_count, vec_to);
        else if (which.isUInt16())
            executeNumericType<UInt16>(casted_arguments, input_rows_count, vec_to);
        else if (which.isUInt32())
            executeNumericType<UInt32>(casted_arguments, input_rows_count, vec_to);
        else if (which.isUInt64())
            executeNumericType<UInt64>(casted_arguments, input_rows_count, vec_to);
        else if (which.isUInt128())
            executeNumericType<UInt128>(casted_arguments, input_rows_count, vec_to);
        else if (which.isUInt256())
            executeNumericType<UInt256>(casted_arguments, input_rows_count, vec_to);
        else if (which.isInt8())
            executeNumericType<Int8>(casted_arguments, input_rows_count, vec_to);
        else if (which.isInt16())
            executeNumericType<Int16>(casted_arguments, input_rows_count, vec_to);
        else if (which.isInt32())
            executeNumericType<Int32>(casted_arguments, input_rows_count, vec_to);
        else if (which.isInt64())
            executeNumericType<Int64>(casted_arguments, input_rows_count, vec_to);
        else if (which.isInt128())
            executeNumericType<Int128>(casted_arguments, input_rows_count, vec_to);
        else if (which.isInt256())
            executeNumericType<Int256>(casted_arguments, input_rows_count, vec_to);
        // Floating point types
        else if (which.isFloat32())
            executeNumericType<Float32>(casted_arguments, input_rows_count, vec_to);
        else if (which.isFloat64())
            executeNumericType<Float64>(casted_arguments, input_rows_count, vec_to);
        // Date and time types
        else if (which.isDate())
            executeNumericType<UInt16>(casted_arguments, input_rows_count, vec_to);
        else if (which.isDate32())
            executeNumericType<Int32>(casted_arguments, input_rows_count, vec_to);
        else if (which.isDateTime())
            executeNumericType<UInt32>(casted_arguments, input_rows_count, vec_to);
        else if (which.isDateTime64())
            executeDecimalType<DateTime64>(casted_arguments, input_rows_count, vec_to);
        // String types
        else if (which.isString() || which.isFixedString())
            executeStringType(casted_arguments, input_rows_count, vec_to);
        // UUID type
        else if (which.isUUID())
            executeNumericType<UUID>(casted_arguments, input_rows_count, vec_to);
        // IP address types
        else if (which.isIPv4())
            executeNumericType<IPv4>(casted_arguments, input_rows_count, vec_to);
        else if (which.isIPv6())
            executeNumericType<IPv6>(casted_arguments, input_rows_count, vec_to);
        // Enum types
        else if (which.isEnum8())
            executeNumericType<Int8>(casted_arguments, input_rows_count, vec_to);
        else if (which.isEnum16())
            executeNumericType<Int16>(casted_arguments, input_rows_count, vec_to);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Unexpected value type {} for function {}",
                value_type->getName(), getName());

        /// Non-representable values became NULL during the accurate cast and cannot be in the filter.
        const NullMap & null_map = nullable_value.getNullMapData();
        for (size_t i = 0; i < input_rows_count; ++i)
            if (null_map[i])
                vec_to[i] = 0;

        return col_to;
    }

private:
    static bool canCastProbeType(const DataTypePtr & probe_type, const DataTypePtr & bloom_value_type)
    {
        DataTypePtr dispatch_probe_type = removeLowCardinalityAndNullable(probe_type);

        if (dispatch_probe_type->equals(*bloom_value_type))
            return true;

        WhichDataType probe_which(dispatch_probe_type);
        WhichDataType bloom_value_which(bloom_value_type);

        /// Numeric probes are intentionally allowed to use a different numeric type. The accurate
        /// cast below preserves correct values and turns non-representable values into NULL, which
        /// are then reported as definitely absent from the Bloom filter.
        return (probe_which.isInteger() || probe_which.isFloat())
            && (bloom_value_which.isInteger() || bloom_value_which.isFloat());
    }

    static size_t getBloomFilterDataOffset(const ColumnAggregateFunction & bloom_col)
    {
        AggregateFunctionPtr function = bloom_col.getAggregateFunction();
        AggregateFunctionPtr nested_function = function->getNestedFunction();
        if (!nested_function)
            return 0;

        /// Nullable aggregate function wrappers keep a small prefix before the nested state.
        /// The nested groupBloomFilter state starts after that prefix.
        size_t function_size = function->sizeOfData();
        size_t nested_function_size = nested_function->sizeOfData();
        if (function_size < nested_function_size)
            return 0;

        return function_size - nested_function_size;
    }

    static const AggregateFunctionGroupBloomFilterData & getBloomFilterData(
        const ColumnAggregateFunction & bloom_col,
        size_t row,
        size_t data_offset)
    {
        return *reinterpret_cast<const AggregateFunctionGroupBloomFilterData *>(bloom_col.getData()[row] + data_offset);
    }

    template <typename T>
    void executeNumericType(
        const ColumnsWithTypeAndName & arguments,
        size_t input_rows_count,
        typename ColumnVector<UInt8>::Container & vec_to) const
    {
        /// First argument: Bloom filter state (may be const or column)
        const ColumnAggregateFunction * bloom_col = nullptr;
        const ColumnAggregateFunction * bloom_col_const = nullptr;
        bool bloom_is_const = false;

        if (const auto * col_const = checkAndGetColumnConst<ColumnAggregateFunction>(arguments[0].column.get()))
        {
            bloom_col_const = typeid_cast<const ColumnAggregateFunction *>(&col_const->getDataColumn());
            bloom_is_const = true;
        }
        else
        {
            bloom_col = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());
        }

        if (bloom_is_const)
        {
            if (!bloom_col_const)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} must be a Bloom filter state column, got {}",
                    getName(), arguments[0].column->getName());
        }
        else if (!bloom_col)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a Bloom filter state column, got {}",
                getName(), arguments[0].column->getName());
        }

        /// Second argument: value to check
        const ColumnVector<T> * value_col = nullptr;
        T value_const{};
        bool value_is_const = false;

        if (const auto * col_const = checkAndGetColumnConst<ColumnVector<T>>(arguments[1].column.get()))
        {
            value_const = col_const->template getValue<T>();
            value_is_const = true;
        }
        else
        {
            value_col = checkAndGetColumn<ColumnVector<T>>(arguments[1].column.get());
        }

        if (!value_is_const && !value_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a column of type {}, got {}",
                getName(), arguments[1].type->getName(), arguments[1].column->getName());

        const size_t bloom_data_offset = getBloomFilterDataOffset(bloom_is_const ? *bloom_col_const : *bloom_col);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateFunctionGroupBloomFilterData & bloom_data = bloom_is_const
                ? getBloomFilterData(*bloom_col_const, 0, bloom_data_offset)
                : getBloomFilterData(*bloom_col, i, bloom_data_offset);

            T value = value_is_const ? value_const : value_col->getData()[i];

            vec_to[i] = bloom_data.contains(reinterpret_cast<const char *>(&value), sizeof(T)) ? 1 : 0;
        }
    }

    void executeStringType(
        const ColumnsWithTypeAndName & arguments,
        size_t input_rows_count,
        typename ColumnVector<UInt8>::Container & vec_to) const
    {
        /// First argument: Bloom filter state
        const ColumnAggregateFunction * bloom_col = nullptr;
        const ColumnAggregateFunction * bloom_col_const = nullptr;
        bool bloom_is_const = false;

        if (const auto * col_const = checkAndGetColumnConst<ColumnAggregateFunction>(arguments[0].column.get()))
        {
            bloom_col_const = typeid_cast<const ColumnAggregateFunction *>(&col_const->getDataColumn());
            bloom_is_const = true;
        }
        else
        {
            bloom_col = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());
        }

        if (bloom_is_const)
        {
            if (!bloom_col_const)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} must be a Bloom filter state column, got {}",
                    getName(), arguments[0].column->getName());
        }
        else if (!bloom_col)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a Bloom filter state column, got {}",
                getName(), arguments[0].column->getName());
        }

        const size_t bloom_data_offset = getBloomFilterDataOffset(bloom_is_const ? *bloom_col_const : *bloom_col);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateFunctionGroupBloomFilterData & bloom_data = bloom_is_const
                ? getBloomFilterData(*bloom_col_const, 0, bloom_data_offset)
                : getBloomFilterData(*bloom_col, i, bloom_data_offset);

            std::string_view value = arguments[1].column->getDataAt(i);
            vec_to[i] = bloom_data.contains(value.data(), value.size()) ? 1 : 0;
        }
    }

    template <typename T>
    void executeDecimalType(
        const ColumnsWithTypeAndName & arguments,
        size_t input_rows_count,
        typename ColumnVector<UInt8>::Container & vec_to) const
    {
        /// First argument: Bloom filter state (may be const or column)
        const ColumnAggregateFunction * bloom_col = nullptr;
        const ColumnAggregateFunction * bloom_col_const = nullptr;
        bool bloom_is_const = false;

        if (const auto * col_const = checkAndGetColumnConst<ColumnAggregateFunction>(arguments[0].column.get()))
        {
            bloom_col_const = typeid_cast<const ColumnAggregateFunction *>(&col_const->getDataColumn());
            bloom_is_const = true;
        }
        else
        {
            bloom_col = typeid_cast<const ColumnAggregateFunction *>(arguments[0].column.get());
        }

        if (bloom_is_const)
        {
            if (!bloom_col_const)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} must be a Bloom filter state column, got {}",
                    getName(), arguments[0].column->getName());
        }
        else if (!bloom_col)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a Bloom filter state column, got {}",
                getName(), arguments[0].column->getName());
        }

        /// Second argument: value to check
        const ColumnDecimal<T> * value_col = nullptr;
        T value_const{};
        bool value_is_const = false;

        if (const auto * col_const = checkAndGetColumnConst<ColumnDecimal<T>>(arguments[1].column.get()))
        {
            value_const = col_const->template getValue<T>();
            value_is_const = true;
        }
        else
        {
            value_col = checkAndGetColumn<ColumnDecimal<T>>(arguments[1].column.get());
        }

        if (!value_is_const && !value_col)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a column of type {}, got {}",
                getName(), arguments[1].type->getName(), arguments[1].column->getName());

        const size_t bloom_data_offset = getBloomFilterDataOffset(bloom_is_const ? *bloom_col_const : *bloom_col);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const AggregateFunctionGroupBloomFilterData & bloom_data = bloom_is_const
                ? getBloomFilterData(*bloom_col_const, 0, bloom_data_offset)
                : getBloomFilterData(*bloom_col, i, bloom_data_offset);

            T value = value_is_const ? value_const : value_col->getData()[i];

            vec_to[i] = bloom_data.contains(reinterpret_cast<const char *>(&value), sizeof(T)) ? 1 : 0;
        }
    }
};

}
