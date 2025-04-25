#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/levenshteinDistance.h>
#include <Common/PODArray.h>
#include <Common/iota.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>

#include <numeric>
#include <span>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

/// arrayLevenshteinDistance([1,2,3,4], [1,3,2,4]) = 2
/// arrayLevenshteinDistanceWeighted([1,2,3,4], [1,3,2,4]) = 2
template <typename T>
class FunctionArrayLevenshtein : public IFunction
{
public:
    static constexpr auto name = T::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayLevenshtein<T>>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return T::arguments; }
    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args_descriptors;
        args_descriptors = FunctionArgumentDescriptors{
            {"from", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"to", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"from_weights", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
            {"to_weights", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
        };
        validateFunctionArguments(*this, arguments, args_descriptors);
        std::vector<DataTypePtr> nested_types;
        nested_types.reserve(2);
        for (size_t index = 2; index < 4; ++index)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[index].type.get());
            const DataTypePtr nested_type = array_type->getNestedType();
            nested_types.emplace_back(nested_type);
            if (!(isFloat(nested_type) || isInteger(nested_type)))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be a numeric array. Found {} instead.",
                    toString(index + 1),
                    getName(),
                    nested_type->getName());
        }

        if (nested_types[0]->getName() != nested_types[1]->getName())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Arguments 3 and 4 of function {} must be arrays of the same types. Found {} and {} instead.",
                getName(),
                nested_types[0]->getName(),
                nested_types[1]->getName());
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t num_arguments = arguments.size();

        Columns holders(num_arguments);
        std::vector<const ColumnArray *> columns(num_arguments);

        for (size_t i = 0; i < num_arguments; ++i)
        {
            holders[i] = arguments[i].column->convertToFullColumnIfConst();
            if (holders[i]->size() != input_rows_count)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Function {} has unequal number of rows in columns: "
                    "expected {}, got {} for column {}",
                    getName(),
                    input_rows_count,
                    holders[i]->size(),
                    holders[i]->getName());
            columns[i] = assert_cast<const ColumnArray*>(holders[i].get());
        }
        return execute(columns);
    }
private:
    ColumnPtr execute(std::vector<const ColumnArray *>) const
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unknown function {}. "
            "Supported names: 'arrayLevenshteinDistance', 'arrayLevenshteinDistanceWeighted', 'arraySimilarity'",
            getName());
    }

    template <typename N, typename Result>
    bool levenshteinString(std::vector<const ColumnArray *> columns, Result::Container & res_values) const
    {
        const N * from_data = checkAndGetColumn<N>(&columns[0]->getData());
        const N * to_data = checkAndGetColumn<N>(&columns[1]->getData());
        if (!from_data || !to_data)
            return false;
        if (T::arguments == 4)
        {
            if constexpr (std::is_same_v<Result, ColumnFloat64>)
            {
                if (!(
                    levenshteinWeightedString<N, UInt8>(columns, res_values)
                    || levenshteinWeightedString<N, UInt16>(columns, res_values)
                    || levenshteinWeightedString<N, UInt32>(columns, res_values)
                    || levenshteinWeightedString<N, UInt64>(columns, res_values)
                    || levenshteinWeightedString<N, UInt128>(columns, res_values)
                    || levenshteinWeightedString<N, UInt256>(columns, res_values)
                    || levenshteinWeightedString<N, Int8>(columns, res_values)
                    || levenshteinWeightedString<N, Int16>(columns, res_values)
                    || levenshteinWeightedString<N, Int32>(columns, res_values)
                    || levenshteinWeightedString<N, Int64>(columns, res_values)
                    || levenshteinWeightedString<N, Int128>(columns, res_values)
                    || levenshteinWeightedString<N, Int256>(columns, res_values)
                    || levenshteinWeightedString<N, Float32>(columns, res_values)
                    || levenshteinWeightedString<N, Float64>(columns, res_values)))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected code branch of function {}. No fitting levenshteinWeightedString",
                        getName());
                return true;
            }
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected code branch of function {}. Wrong expected result type in levenshteinString",
                getName());
        }
        const ColumnArray::Offsets & from_offsets = columns[0]->getOffsets();
        ColumnArray::Offset prev_from_offset = 0;

        const ColumnArray::Offsets & to_offsets = columns[1]->getOffsets();
        ColumnArray::Offset prev_to_offset = 0;
        const auto extract_array = [](const N * data, size_t prev_offset, size_t count)
        {
            std::vector<StringRef> temp;
            temp.reserve(count);
            for (size_t j = 0; j < count; ++j) { temp.emplace_back(data->getDataAt(prev_offset + j)); }
            return temp;
        };

        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            const size_t m = from_offsets[row] - prev_from_offset;
            const size_t n = to_offsets[row] - prev_to_offset;
            const std::vector<StringRef> from = extract_array(from_data, prev_from_offset, m);
            const std::vector<StringRef> to = extract_array(to_data, prev_to_offset, n);
            prev_from_offset = from_offsets[row];
            prev_to_offset = to_offsets[row];
            res_values[row] = levenshteinDistance<StringRef>(from, to);
        }
        return true;
    }

    template <typename N, typename Result>
    bool levenshteinNumber(std::vector<const ColumnArray *> columns, Result::Container & res_values) const
    {
        const ColumnVectorOrDecimal<N> * column_from = checkAndGetColumn<ColumnVectorOrDecimal<N>>(&columns[0]->getData());
        const ColumnVectorOrDecimal<N> * column_to = checkAndGetColumn<ColumnVectorOrDecimal<N>>(&columns[1]->getData());
        if (!column_from || !column_to)
            return false;

        if (T::arguments == 4)
        {
            if constexpr (std::is_same_v<Result, ColumnFloat64>)
            {
                if (!(
                    levenshteinWeightedNumber<N, UInt8>(columns, res_values)
                    || levenshteinWeightedNumber<N, UInt16>(columns, res_values)
                    || levenshteinWeightedNumber<N, UInt32>(columns, res_values)
                    || levenshteinWeightedNumber<N, UInt64>(columns, res_values)
                    || levenshteinWeightedNumber<N, UInt128>(columns, res_values)
                    || levenshteinWeightedNumber<N, UInt256>(columns, res_values)
                    || levenshteinWeightedNumber<N, Int8>(columns, res_values)
                    || levenshteinWeightedNumber<N, Int16>(columns, res_values)
                    || levenshteinWeightedNumber<N, Int32>(columns, res_values)
                    || levenshteinWeightedNumber<N, Int64>(columns, res_values)
                    || levenshteinWeightedNumber<N, Int128>(columns, res_values)
                    || levenshteinWeightedNumber<N, Int256>(columns, res_values)
                    || levenshteinWeightedNumber<N, Float32>(columns, res_values)
                    || levenshteinWeightedNumber<N, Float64>(columns, res_values)))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected code branch of function {}. No fitting levenshteinWeightedNumber",
                        getName());
                return true;
            }
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected code branch of function {}. Wrong expected result type in levenshteinNumber",
                getName());
        }

        const ColumnArray::Offsets & from_offsets = columns[0]->getOffsets();
        ColumnArray::Offset prev_from_offset = 0;

        const ColumnArray::Offsets & to_offsets = columns[1]->getOffsets();
        ColumnArray::Offset prev_to_offset = 0;

        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            std::span<const N> from(column_from->getData().begin() + prev_from_offset, from_offsets[row] - prev_from_offset);
            prev_from_offset = from_offsets[row];
            std::span<const N> to(column_to->getData().begin() + prev_to_offset, to_offsets[row] - prev_to_offset);
            prev_to_offset = to_offsets[row];

            res_values[row] = levenshteinDistance<N>(from, to);
        }
        return true;
    }

    template<typename Result>
    void levenshteinGeneric(std::vector<const ColumnArray *> columns, Result::Container & res_values) const
    {
        if (T::arguments == 4)
        {
            if constexpr (std::is_same_v<Result, ColumnFloat64>)
            {
                if (!(
                    levenshteinWeightedGeneric<UInt8>(columns, res_values)
                    || levenshteinWeightedGeneric<UInt16>(columns, res_values)
                    || levenshteinWeightedGeneric<UInt32>(columns, res_values)
                    || levenshteinWeightedGeneric<UInt64>(columns, res_values)
                    || levenshteinWeightedGeneric<UInt128>(columns, res_values)
                    || levenshteinWeightedGeneric<UInt256>(columns, res_values)
                    || levenshteinWeightedGeneric<Int8>(columns, res_values)
                    || levenshteinWeightedGeneric<Int16>(columns, res_values)
                    || levenshteinWeightedGeneric<Int32>(columns, res_values)
                    || levenshteinWeightedGeneric<Int64>(columns, res_values)
                    || levenshteinWeightedGeneric<Int128>(columns, res_values)
                    || levenshteinWeightedGeneric<Int256>(columns, res_values)
                    || levenshteinWeightedGeneric<Float32>(columns, res_values)
                    || levenshteinWeightedGeneric<Float64>(columns, res_values)))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected code branch of function {}. No fitting levenshteinWeightedGeneric",
                        getName());
                return;
            }
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected code branch of function {}. Wrong expected result type in levenshteinGeneric",
                getName());
        }

        const ColumnArray * column_from = columns[0];
        const ColumnArray * column_to = columns[1];
        for (size_t row = 0; row < column_from->size(); row++)
        {
            // Effective Levenshtein realization from Common/levenshteinDistance
            Array from = (*column_from)[row].safeGet<Array>();
            Array to = (*column_to)[row].safeGet<Array>();
            res_values[row] = levenshteinDistance<Field>(from, to);
        }
    }

    template <typename N, typename W>
    bool levenshteinWeightedString(std::vector<const ColumnArray *> columns, ColumnFloat64::Container & res_values) const
    {
        const N * from_data = checkAndGetColumn<N>(&columns[0]->getData());
        const N * to_data = checkAndGetColumn<N>(&columns[1]->getData());
        if (!from_data || !to_data)
            return false;
        const ColumnArray::Offsets & from_offsets = columns[0]->getOffsets();
        ColumnArray::Offset prev_from_offset = 0;

        const ColumnArray::Offsets & to_offsets = columns[1]->getOffsets();
        ColumnArray::Offset prev_to_offset = 0;

        const ColumnVector<W> * column_from_weights = checkAndGetColumn<ColumnVector<W>>(&columns[2]->getData());
        const ColumnVector<W> * column_to_weights = checkAndGetColumn<ColumnVector<W>>(&columns[3]->getData());
        if (!column_from_weights || !column_to_weights)
            return false;

        const ColumnArray::Offsets & from_weights_offsets = columns[2]->getOffsets();
        ColumnArray::Offset prev_from_weights_offset = 0;

        const ColumnArray::Offsets & to_weights_offsets = columns[3]->getOffsets();
        ColumnArray::Offset prev_to_weights_offset = 0;

        const auto extract_array = [](const N * data, size_t prev_offset, size_t count)
        {
            std::vector<StringRef> temp;
            temp.reserve(count);
            for (size_t j = 0; j < count; ++j) { temp.emplace_back(data->getDataAt(prev_offset + j)); }
            return temp;
        };

        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            const size_t m = from_offsets[row] - prev_from_offset;
            const size_t n = to_offsets[row] - prev_to_offset;
            const std::vector<StringRef> from = extract_array(from_data, prev_from_offset, m);
            const std::vector<StringRef> to = extract_array(to_data, prev_to_offset, n);
            prev_from_offset = from_offsets[row];
            prev_to_offset = to_offsets[row];

            std::span<const W> from_weights(column_from_weights->getData().begin() + prev_from_weights_offset, from_weights_offsets[row] - prev_from_weights_offset);
            prev_from_weights_offset = from_weights_offsets[row];
            std::span<const W> to_weights(column_to_weights->getData().begin() + prev_to_weights_offset, to_weights_offsets[row] - prev_to_weights_offset);
            prev_to_weights_offset = to_weights_offsets[row];

            res_values[row] = static_cast<Float64>(DB::levenshteinDistanceWeighted<StringRef, W>(from, to, from_weights, to_weights));
        }
        return true;
    }

    template <typename N, typename W>
    bool levenshteinWeightedNumber(std::vector<const ColumnArray *> columns, ColumnFloat64::Container & res_values) const
    {
        const ColumnVectorOrDecimal<N> * column_from = checkAndGetColumn<ColumnVectorOrDecimal<N>>(&columns[0]->getData());
        const ColumnVectorOrDecimal<N> * column_to = checkAndGetColumn<ColumnVectorOrDecimal<N>>(&columns[1]->getData());
        if (!column_from || !column_to)
            // just to be on the safe side, it's already checked
            return false;

        const ColumnArray::Offsets & from_offsets = columns[0]->getOffsets();
        ColumnArray::Offset prev_from_offset = 0;
        const ColumnArray::Offsets & to_offsets = columns[1]->getOffsets();
        ColumnArray::Offset prev_to_offset = 0;

        const ColumnVector<W> * column_from_weights = checkAndGetColumn<ColumnVector<W>>(&columns[2]->getData());
        const ColumnVector<W> * column_to_weights = checkAndGetColumn<ColumnVector<W>>(&columns[3]->getData());
        if (!column_from_weights || !column_to_weights)
            return false;

        const ColumnArray::Offsets & from_weights_offsets = columns[2]->getOffsets();
        ColumnArray::Offset prev_from_weights_offset = 0;
        const ColumnArray::Offsets & to_weights_offsets = columns[3]->getOffsets();
        ColumnArray::Offset prev_to_weights_offset = 0;

        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            std::span<const N> from(column_from->getData().begin() + prev_from_offset, from_offsets[row] - prev_from_offset);
            prev_from_offset = from_offsets[row];
            std::span<const N> to(column_to->getData().begin() + prev_to_offset, to_offsets[row] - prev_to_offset);
            prev_to_offset = to_offsets[row];

            std::span<const W> from_weights(column_from_weights->getData().begin() + prev_from_weights_offset, from_weights_offsets[row] - prev_from_weights_offset);
            prev_from_weights_offset = from_weights_offsets[row];
            std::span<const W> to_weights(column_to_weights->getData().begin() + prev_to_weights_offset, to_weights_offsets[row] - prev_to_weights_offset);
            prev_to_weights_offset = to_weights_offsets[row];

            res_values[row] = static_cast<Float64>(DB::levenshteinDistanceWeighted<N, W>(from, to, from_weights, to_weights));
        }
        return true;
    }

    template<typename W>
    bool levenshteinWeightedGeneric(std::vector<const ColumnArray *> columns, ColumnFloat64::Container & res_values) const
    {
        const ColumnArray * column_from = columns[0];
        const ColumnArray * column_to = columns[1];

        const ColumnVector<W> * column_from_weights = checkAndGetColumn<ColumnVector<W>>(&columns[2]->getData());
        const ColumnVector<W> * column_to_weights = checkAndGetColumn<ColumnVector<W>>(&columns[3]->getData());
        if (!column_from_weights || !column_to_weights)
            return false;
        const ColumnArray::Offsets & from_weights_offsets = columns[2]->getOffsets();
        ColumnArray::Offset prev_from_weights_offset = 0;

        const ColumnArray::Offsets & to_weights_offsets = columns[3]->getOffsets();
        ColumnArray::Offset prev_to_weights_offset = 0;

        for (size_t row = 0; row < column_from->size(); row++)
        {
            // Effective Levenshtein realization from Common/levenshteinDistance
            Array from = (*column_from)[row].safeGet<Array>();
            Array to = (*column_to)[row].safeGet<Array>();

            std::span<const W> from_weights(column_from_weights->getData().begin() + prev_from_weights_offset, from_weights_offsets[row] - prev_from_weights_offset);
            prev_from_weights_offset = from_weights_offsets[row];
            std::span<const W> to_weights(column_to_weights->getData().begin() + prev_to_weights_offset, to_weights_offsets[row] - prev_to_weights_offset);
            prev_to_weights_offset = to_weights_offsets[row];

            res_values[row] = static_cast<Float64>(DB::levenshteinDistanceWeighted<Field, W>(from, to, from_weights, to_weights));
        }
        return true;
    }

    ColumnPtr levenshteinImpl(std::vector<const ColumnArray *> columns) const
    {
        auto res = ColumnUInt32::create();
        ColumnUInt32::Container & res_values = res->getData();
        res_values.resize(columns[0]->size());
        if (levenshteinNumber<UInt8, ColumnUInt32>(columns, res_values) || levenshteinNumber<UInt16, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<UInt32, ColumnUInt32>(columns, res_values) || levenshteinNumber<UInt64, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<UInt128, ColumnUInt32>(columns, res_values) || levenshteinNumber<UInt256, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<Int8, ColumnUInt32>(columns, res_values) || levenshteinNumber<Int16, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<Int32, ColumnUInt32>(columns, res_values) || levenshteinNumber<Int64, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<Int128, ColumnUInt32>(columns, res_values) || levenshteinNumber<Int256, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<Float32, ColumnUInt32>(columns, res_values) || levenshteinNumber<Float64, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<Decimal32, ColumnUInt32>(columns, res_values) || levenshteinNumber<Decimal64, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<Decimal128, ColumnUInt32>(columns, res_values) || levenshteinNumber<Decimal256, ColumnUInt32>(columns, res_values)
            || levenshteinNumber<DateTime64, ColumnUInt32>(columns, res_values)
            || levenshteinString<ColumnString, ColumnUInt32>(columns, res_values) || levenshteinString<ColumnFixedString, ColumnUInt32>(columns, res_values))
            return res;
        levenshteinGeneric<ColumnUInt32>(columns, res_values);
        return res;
    }

    ColumnPtr weightedLevenshteinImpl(std::vector<const ColumnArray *> columns) const
    {
        for (size_t i = 0; i < 2; i++)
        {
            const ColumnArray * hs_column = columns[i];
            const ColumnArray * weights_column = columns[i+2];
            for (size_t row = 0; row < hs_column->size(); row++)
            {
                Array hs = (*hs_column)[row].safeGet<Array>();
                Array weights = (*weights_column)[row].safeGet<Array>();
                if (hs.size() != weights.size())
                    throw Exception(
                        ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                        "Arguments {} ({}, size {}) and {} ({}, size {}) of function {} must be arrays of the same size",
                        toString(i + 1),
                        hs_column->getName(),
                        hs.size(),
                        toString(i + 3),
                        weights_column->getName(),
                        weights.size(),
                        getName());
            }
        }
        auto res = ColumnFloat64::create();
        ColumnFloat64::Container & res_values = res->getData();
        res_values.resize(columns[0]->size());
        if (levenshteinNumber<UInt8, ColumnFloat64>(columns, res_values) || levenshteinNumber<UInt16, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<UInt32, ColumnFloat64>(columns, res_values) || levenshteinNumber<UInt64, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<UInt128, ColumnFloat64>(columns, res_values) || levenshteinNumber<UInt256, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<Int8, ColumnFloat64>(columns, res_values) || levenshteinNumber<Int16, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<Int32, ColumnFloat64>(columns, res_values) || levenshteinNumber<Int64, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<Int128, ColumnFloat64>(columns, res_values) || levenshteinNumber<Int256, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<Float32, ColumnFloat64>(columns, res_values) || levenshteinNumber<Float64, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<Decimal32, ColumnFloat64>(columns, res_values) || levenshteinNumber<Decimal64, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<Decimal128, ColumnFloat64>(columns, res_values) || levenshteinNumber<Decimal256, ColumnFloat64>(columns, res_values)
            || levenshteinNumber<DateTime64, ColumnFloat64>(columns, res_values)
            || levenshteinString<ColumnString, ColumnFloat64>(columns, res_values) || levenshteinString<ColumnFixedString, ColumnFloat64>(columns, res_values))
            return res;
        levenshteinGeneric<ColumnFloat64>(columns, res_values);
        return res;
    }

    template <typename W>
    bool similarity(std::vector<const ColumnArray *> columns, ColumnPtr distance, ColumnFloat64::Container & res_values) const
    {
        const ColumnVector<W> * column_from_weights = checkAndGetColumn<ColumnVector<W>>(&columns[2]->getData());
        const ColumnVector<W> * column_to_weights = checkAndGetColumn<ColumnVector<W>>(&columns[3]->getData());
        if (!column_from_weights || !column_to_weights)
            return false;

        const ColumnArray::Offsets & from_weights_offsets = columns[2]->getOffsets();
        ColumnArray::Offset prev_from_weights_offset = 0;
        const ColumnArray::Offsets & to_weights_offsets = columns[3]->getOffsets();
        ColumnArray::Offset prev_to_weights_offset = 0;

        for (size_t row = 0; row < distance->size(); row++)
        {
            std::span<const W> from_weights(column_from_weights->getData().begin() + prev_from_weights_offset, from_weights_offsets[row] - prev_from_weights_offset);
            prev_from_weights_offset = from_weights_offsets[row];
            std::span<const W> to_weights(column_to_weights->getData().begin() + prev_to_weights_offset, to_weights_offsets[row] - prev_to_weights_offset);
            prev_to_weights_offset = to_weights_offsets[row];

            if (distance->getFloat64(row) == 0)
            {
                res_values[row] = 1.0;
                continue;
            }
            W weights_sum = std::accumulate(from_weights.begin(), from_weights.end(), W{}) +
                            std::accumulate(to_weights.begin(), to_weights.end(), W{});
            if (weights_sum == 0)
            {
                res_values[row] = 1.0;
                continue;
            }
            res_values[row] = 1.0 - (distance->getFloat64(row) / weights_sum);
        }
        return true;
    }
};

struct SimpleLevenshtein
{
    static constexpr auto name{"arrayLevenshteinDistance"};
    static constexpr size_t arguments = 2;
};

template <>
DataTypePtr FunctionArrayLevenshtein<SimpleLevenshtein>::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    FunctionArgumentDescriptors args_descriptors;
    args_descriptors = FunctionArgumentDescriptors{
        {"from", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
        {"to", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
    };
    validateFunctionArguments(*this, arguments, args_descriptors);
    return std::make_shared<DataTypeUInt32>();
}


template <>
ColumnPtr FunctionArrayLevenshtein<SimpleLevenshtein>::execute(std::vector<const ColumnArray *> columns) const
{
    return levenshteinImpl(columns);
}

struct Weighted
{
    static constexpr auto name{"arrayLevenshteinDistanceWeighted"};
    static constexpr size_t arguments = 4;
};

template <>
ColumnPtr FunctionArrayLevenshtein<Weighted>::execute(std::vector<const ColumnArray *> columns) const
{
    return weightedLevenshteinImpl(columns);
}

struct Similarity
{
    static constexpr auto name{"arraySimilarity"};
    static constexpr size_t arguments = 4;
};

template <>
ColumnPtr FunctionArrayLevenshtein<Similarity>::execute(std::vector<const ColumnArray *> columns) const
{
    ColumnPtr distance = weightedLevenshteinImpl(columns);
    auto result = ColumnFloat64::create();
    ColumnFloat64::Container & res_values = result->getData();
    res_values.resize(distance->size());
    if (!(
        similarity<UInt8>(columns, distance, res_values) || similarity<UInt16>(columns, distance, res_values)
        || similarity<UInt16>(columns, distance, res_values) || similarity<UInt64>(columns, distance, res_values)
        || similarity<UInt128>(columns, distance, res_values) || similarity<UInt256>(columns, distance, res_values)
        || similarity<Int8>(columns, distance, res_values) || similarity<Int16>(columns, distance, res_values)
        || similarity<Int16>(columns, distance, res_values) || similarity<Int64>(columns, distance, res_values)
        || similarity<Int128>(columns, distance, res_values) || similarity<Int256>(columns, distance, res_values)
        || similarity<Float32>(columns, distance, res_values) || similarity<Float64>(columns, distance, res_values)))
        throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unexpected code branch of function {}. No matching column types for {} and {}",
                getName(),
                columns[2]->getName(),
                columns[3]->getName());
    return result;
}

REGISTER_FUNCTION(ArrayLevenshtein)
{
    factory.registerFunction<FunctionArrayLevenshtein<SimpleLevenshtein>>(
        {.description = R"(
Calculates Levenshtein distance for two arrays.
)",
         .syntax{"arrayLevenshteinDistance(from, to)"},
         .arguments{{"from", "first array"}, {"to", "second array"}},
         .returned_value{"Levenshtein distance between the first and the second arrays"},
         .examples{{{
             "Query",
             "SELECT arrayLevenshteinDistance([1, 2, 4], [1, 2, 3])",
             R"(
┌─arrayLevenshteinDistance([1, 2, 4], [1, 2, 3])─┐
│                                              1 │
└────────────────────────────────────────────────┘
)",
         }}},
         .category{"Arrays"}});

    factory.registerFunction<FunctionArrayLevenshtein<Weighted>>(
        {.description = R"(
Calculates Levenshtein distance for two arrays with custom weights for each element. Number of elements for array and its weights should match
)",
         .syntax{"arrayLevenshteinDistanceWeighted(from, to, from_weights, to_weights)"},
         .arguments{
             {"from", "first array"},
             {"to", "second array"},
             {"from_weights", "weights for the first array"},
             {"to_weights", "weights for the second array"},
         },
         .returned_value{"Levenshtein distance between the first and the second arrays with custom weights for each element"},
         .examples{{{
            "Query",
            "SELECT arrayLevenshteinDistanceWeighted(['A', 'B', 'C'], ['A', 'K', 'L'], [1.0, 2, 3], [3.0, 4, 5])",
            R"(
┌─arrayLevenshteinDistanceWeighted(['A', 'B', 'C'], ['A', 'K', 'L'], [1.0, 2, 3], [3.0, 4, 5])─┐
│                                                                                           14 │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
)",
         }}},
         .category{"Arrays"}});

    factory.registerFunction<FunctionArrayLevenshtein<Similarity>>(
        {.description = R"(
Calculates arrays' similarity from 0 to 1 based on weighed Levenshtein distance. Accepts the same arguments as `arrayLevenshteinDistanceWeighted` function.
)",
         .syntax{"arraySimilarity(from, to, from_weights, to_weights)"},
         .arguments{
             {"from", "first array"},
             {"to", "second array"},
             {"from_weights", "weights for the first array"},
             {"to_weights", "weights for the second array"},
         },
         .returned_value{"Similarity of two arrays based on the weighted Levenshtein distance"},
         .examples{{{
            "Query",
            "SELECT arraySimilarity(['A', 'B', 'C'], ['A', 'K', 'L'], [1.0, 2, 3], [3.0, 4, 5])",
            R"(
┌─arraySimilarity(['A', 'B', 'C'], ['A', 'K', 'L'], [1.0, 2, 3], [3.0, 4, 5])─┐
│                                                          0.2222222222222222 │
└─────────────────────────────────────────────────────────────────────────────┘
)",
         }}},
         .category{"Arrays"}});

}
}
