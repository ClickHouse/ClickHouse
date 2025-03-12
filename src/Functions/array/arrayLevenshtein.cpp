#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
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

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

/// arrayLevenshtein([1,2,3,4], [1,3,2,4]) = 2
/// arrayLevenshteinWeighted([1,2,3,4], [1,3,2,4]) = 2
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
            // TODO: extend to work with different types?
            if (!WhichDataType(nested_type).isFloat64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be array of Float64. Found {} instead.",
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
            "Supported names: 'arrayLevenshtein', 'arrayLevenshteinWeighted', 'arraySimilarity'",
            T::name);
    }

    template<typename N>
    UInt32 levenshteinDistance(const std::vector<N> from, const std::vector<N> to) const
    {
        const size_t m = from.size();
        const size_t n = to.size();
        if (m==0 || n==0)
        {
            return static_cast<UInt32>(m + n);
        }
        PODArrayWithStackMemory<size_t, 32> v0(n + 1);

        iota(v0.data() + 1, n, size_t(1));

        for (size_t j = 1; j <= m; ++j)
        {
            v0[0] = j;
            size_t prev = j - 1;
            for (size_t i = 1; i <= n; ++i)
            {
                size_t old = v0[i];
                v0[i] = std::min(prev + (from[j - 1] != to[i - 1]),
                        std::min(v0[i - 1], v0[i]) + 1);
                prev = old;
            }
        }
        return static_cast<UInt32>(v0[n]);
    }

    template <typename N>
    bool levenshteinString(std::vector<const ColumnArray *> columns, ColumnUInt32::Container & res_values) const
    {
        const N * from_data = checkAndGetColumn<N>(&columns[0]->getData());
        const N * to_data = checkAndGetColumn<N>(&columns[1]->getData());
        if (!from_data || !to_data)
            return false;
        const ColumnArray::Offsets & from_offsets = columns[0]->getOffsets();
        ColumnArray::Offset prev_from_offset = 0;

        const ColumnArray::Offsets & to_offsets = columns[1]->getOffsets();
        ColumnArray::Offset prev_to_offset = 0;

        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            const size_t m = from_offsets[row] - prev_from_offset;
            const size_t n = to_offsets[row] - prev_to_offset;
            const std::vector<StringRef> & from = [&]()
            {
                std::vector<StringRef> temp;
                temp.reserve(m);
                for (size_t j = 0; j < m; ++j) { temp.emplace_back(from_data->getDataAt(prev_from_offset + j)); }
                return temp;
            }();
            const std::vector<StringRef> & to = [&]()
            {
                std::vector<StringRef> temp;
                temp.reserve(m);
                for (size_t j = 0; j < n; ++j) { temp.emplace_back(to_data->getDataAt(prev_to_offset + j)); }
                return temp;
            }();
            prev_from_offset = from_offsets[row];
            prev_to_offset = to_offsets[row];
            res_values[row] = levenshteinDistance<StringRef>(from, to);
        }
        return true;
    }

    template <typename N>
    bool levenshteinNumber(std::vector<const ColumnArray *> columns, ColumnUInt32::Container & res_values) const
    {
        const ColumnVectorOrDecimal<N> * column_from = checkAndGetColumn<ColumnVectorOrDecimal<N>>(&columns[0]->getData());
        const ColumnVectorOrDecimal<N> * column_to = checkAndGetColumn<ColumnVectorOrDecimal<N>>(&columns[1]->getData());
        if (!column_from || !column_to)
            return false;
        const PaddedPODArray<N> & vec_from = column_from->getData();
        const ColumnArray::Offsets & from_offsets = columns[0]->getOffsets();
        ColumnArray::Offset prev_from_offset = 0;

        const PaddedPODArray<N> & vec_to = column_to->getData();
        const ColumnArray::Offsets & to_offsets = columns[1]->getOffsets();
        ColumnArray::Offset prev_to_offset = 0;

        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            const std::vector<N> from(vec_from.begin() + prev_from_offset, vec_from.begin() + from_offsets[row]);
            prev_from_offset = from_offsets[row];
            const std::vector<N> to(vec_to.begin() + prev_to_offset, vec_to.begin() + to_offsets[row]);
            prev_to_offset = to_offsets[row];

            res_values[row] = levenshteinDistance<N>(from, to);
        }
        return true;
    }

    void levenshteinGeneric(std::vector<const ColumnArray *> columns, ColumnUInt32::Container & res_values) const
    {
        const ColumnArray * column_from = columns[0];
        const ColumnArray * column_to = columns[1];
        for (size_t row = 0; row < column_from->size(); row++)
        {
            // Effective Levenshtein realization from Common/levenshteinDistance
            Array from = (*column_from)[row].safeGet<Array>();
            Array to = (*column_to)[row].safeGet<Array>();
            const std::vector<Field> from_vec(from.begin(), from.end());
            const std::vector<Field> to_vec(to.begin(), to.end());
            res_values[row] = levenshteinDistance<Field>(from_vec, to_vec);
        }
    }

    template<typename N>
    Float64 levenshteinDistanceWeighted(const std::vector<N> from, const std::vector<N> to,
                                        const std::vector<Float64> from_weights, const std::vector<Float64> to_weights) const
    {
        auto sum_vec = [](const std::vector<Float64> & vector) -> Float64
        {
            return std::accumulate(vector.begin(), vector.end(), .0);
        };
        const size_t m = std::min(from.size(), to.size());
        const size_t n = std::max(from.size(), to.size());
        if (m==0 || n==0)
        {
            return sum_vec(from_weights) + sum_vec(to_weights);
        }
        // Consume minimum memory by allocating sliding vectors for min `m`
        auto & lhs = (from.size() <= to.size()) ? from : to;
        const std::vector<Float64> & lhs_w = (from.size() <= to.size()) ? from_weights : to_weights;
        auto & rhs = (from.size() <= to.size()) ? to : from;
        const std::vector<Float64> & rhs_w = (from.size() <= to.size()) ? to_weights : from_weights;

        PODArrayWithStackMemory<Float64, 64> v0(m + 1);
        PODArrayWithStackMemory<Float64, 64> v1(m + 1);

        v0[0] = 0;
        std::partial_sum(lhs_w.begin(), lhs_w.end(), v0.begin() + 1);

        for (size_t i = 0; i < n; ++i)
        {
            v1[0] = v0[0] + rhs_w[i];
            for (size_t j = 0; j < m; ++j)
            {
                if (lhs[j] == rhs[i])
                {
                    v1[j + 1] = v0[j];
                    continue;
                }

                v1[j+1] = std::min({v0[j + 1] + rhs_w[i],          // deletion
                                    v1[j] + lhs_w[j],              // insertion
                                    v0[j] + lhs_w[j] + rhs_w[i]}); // substitusion
            }
            std::swap(v0, v1);
        }
        return v0[m];
    }

    template <typename N>
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

        const ColumnVector<Float64> * column_from_weights = checkAndGetColumn<ColumnVector<Float64>>(&columns[2]->getData());
        const ColumnVector<Float64> * column_to_weights = checkAndGetColumn<ColumnVector<Float64>>(&columns[3]->getData());
        if (!column_from_weights || !column_to_weights)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Function {} wrong type of weight columns",
                getName());
        const PaddedPODArray<Float64> & vec_from_weights = column_from_weights->getData();
        const ColumnArray::Offsets & from_weights_offsets = columns[2]->getOffsets();
        ColumnArray::Offset prev_from_weights_offset = 0;

        const PaddedPODArray<Float64> & vec_to_weights = column_to_weights->getData();
        const ColumnArray::Offsets & to_weights_offsets = columns[3]->getOffsets();
        ColumnArray::Offset prev_to_weights_offset = 0;

        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            const size_t m = from_offsets[row] - prev_from_offset;
            const size_t n = to_offsets[row] - prev_to_offset;
            const std::vector<StringRef> & from = [&]()
            {
                std::vector<StringRef> temp;
                temp.reserve(m);
                for (size_t j = 0; j < m; ++j) { temp.emplace_back(from_data->getDataAt(prev_from_offset + j)); }
                return temp;
            }();
            const std::vector<StringRef> & to = [&]()
            {
                std::vector<StringRef> temp;
                temp.reserve(m);
                for (size_t j = 0; j < n; ++j) { temp.emplace_back(to_data->getDataAt(prev_to_offset + j)); }
                return temp;
            }();
            prev_from_offset = from_offsets[row];
            prev_to_offset = to_offsets[row];

            const std::vector<Float64> from_weights(vec_from_weights.begin() + prev_from_weights_offset, vec_from_weights.begin() + from_weights_offsets[row]);
            prev_from_weights_offset = from_weights_offsets[row];
            const std::vector<Float64> to_weights(vec_to_weights.begin() + prev_to_weights_offset, vec_to_weights.begin() + to_weights_offsets[row]);
            prev_to_weights_offset = to_weights_offsets[row];

            res_values[row] = levenshteinDistanceWeighted<StringRef>(from, to, from_weights, to_weights);
        }
        return true;
    }

    template <typename N>
    bool levenshteinWeightedNumber(std::vector<const ColumnArray *> columns, ColumnFloat64::Container & res_values) const
    {
        const ColumnVectorOrDecimal<N> * column_from = checkAndGetColumn<ColumnVectorOrDecimal<N>>(&columns[0]->getData());
        const ColumnVectorOrDecimal<N> * column_to = checkAndGetColumn<ColumnVectorOrDecimal<N>>(&columns[1]->getData());
        if (!column_from || !column_to)
            return false;
        const PaddedPODArray<N> & vec_from = column_from->getData();
        const ColumnArray::Offsets & from_offsets = columns[0]->getOffsets();
        ColumnArray::Offset prev_from_offset = 0;

        const PaddedPODArray<N> & vec_to = column_to->getData();
        const ColumnArray::Offsets & to_offsets = columns[1]->getOffsets();
        ColumnArray::Offset prev_to_offset = 0;

        const ColumnVector<Float64> * column_from_weights = checkAndGetColumn<ColumnVector<Float64>>(&columns[2]->getData());
        const ColumnVector<Float64> * column_to_weights = checkAndGetColumn<ColumnVector<Float64>>(&columns[3]->getData());
        if (!column_from_weights || !column_to_weights)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Function {} wrong type of weight columns",
                getName());
        const PaddedPODArray<Float64> & vec_from_weights = column_from_weights->getData();
        const ColumnArray::Offsets & from_weights_offsets = columns[2]->getOffsets();
        ColumnArray::Offset prev_from_weights_offset = 0;

        const PaddedPODArray<Float64> & vec_to_weights = column_to_weights->getData();
        const ColumnArray::Offsets & to_weights_offsets = columns[3]->getOffsets();
        ColumnArray::Offset prev_to_weights_offset = 0;

        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            const std::vector<N> from(vec_from.begin() + prev_from_offset, vec_from.begin() + from_offsets[row]);
            prev_from_offset = from_offsets[row];
            const std::vector<N> to(vec_to.begin() + prev_to_offset, vec_to.begin() + to_offsets[row]);
            prev_to_offset = to_offsets[row];

            const std::vector<Float64> from_weights(vec_from_weights.begin() + prev_from_weights_offset, vec_from_weights.begin() + from_weights_offsets[row]);
            prev_from_weights_offset = from_weights_offsets[row];
            const std::vector<Float64> to_weights(vec_to_weights.begin() + prev_to_weights_offset, vec_to_weights.begin() + to_weights_offsets[row]);
            prev_to_weights_offset = to_weights_offsets[row];

            res_values[row] = levenshteinDistanceWeighted<N>(from, to, from_weights, to_weights);
        }
        return true;
    }

    void levenshteinWeightedGeneric(std::vector<const ColumnArray *> columns, ColumnFloat64::Container & res_values) const
    {
        const ColumnArray * column_from = columns[0];
        const ColumnArray * column_to = columns[1];

        const ColumnVector<Float64> * column_from_weights = checkAndGetColumn<ColumnVector<Float64>>(&columns[2]->getData());
        const ColumnVector<Float64> * column_to_weights = checkAndGetColumn<ColumnVector<Float64>>(&columns[3]->getData());
        if (!column_from_weights || !column_to_weights)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Function {} wrong type of weight columns",
                getName());
        const PaddedPODArray<Float64> & vec_from_weights = column_from_weights->getData();
        const ColumnArray::Offsets & from_weights_offsets = columns[2]->getOffsets();
        ColumnArray::Offset prev_from_weights_offset = 0;

        const PaddedPODArray<Float64> & vec_to_weights = column_to_weights->getData();
        const ColumnArray::Offsets & to_weights_offsets = columns[3]->getOffsets();
        ColumnArray::Offset prev_to_weights_offset = 0;

        for (size_t row = 0; row < column_from->size(); row++)
        {
            // Effective Levenshtein realization from Common/levenshteinDistance
            Array from = (*column_from)[row].safeGet<Array>();
            Array to = (*column_to)[row].safeGet<Array>();
            const std::vector<Field> from_vec(from.begin(), from.end());
            const std::vector<Field> to_vec(to.begin(), to.end());

            const std::vector<Float64> from_weights(vec_from_weights.begin() + prev_from_weights_offset, vec_from_weights.begin() + from_weights_offsets[row]);
            prev_from_weights_offset = from_weights_offsets[row];
            const std::vector<Float64> to_weights(vec_to_weights.begin() + prev_to_weights_offset, vec_to_weights.begin() + to_weights_offsets[row]);
            prev_to_weights_offset = to_weights_offsets[row];

            res_values[row] = levenshteinDistanceWeighted<Field>(from_vec, to_vec, from_weights, to_weights);
        }
    }

    MutableColumnPtr weightedLevenshteinImpl(std::vector<const ColumnArray *> columns) const
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
        if (levenshteinWeightedNumber<UInt8>(columns, res_values) || levenshteinWeightedNumber<UInt16>(columns, res_values)
            || levenshteinWeightedNumber<UInt32>(columns, res_values) || levenshteinWeightedNumber<UInt64>(columns, res_values)
            || levenshteinWeightedNumber<UInt128>(columns, res_values) || levenshteinWeightedNumber<UInt256>(columns, res_values)
            || levenshteinWeightedNumber<Int8>(columns, res_values) || levenshteinWeightedNumber<Int16>(columns, res_values)
            || levenshteinWeightedNumber<Int32>(columns, res_values) || levenshteinWeightedNumber<Int64>(columns, res_values)
            || levenshteinWeightedNumber<Int128>(columns, res_values) || levenshteinWeightedNumber<Int256>(columns, res_values)
            || levenshteinWeightedNumber<Float32>(columns, res_values) || levenshteinWeightedNumber<Float64>(columns, res_values)
            || levenshteinWeightedNumber<Decimal32>(columns, res_values) || levenshteinWeightedNumber<Decimal64>(columns, res_values)
            || levenshteinWeightedNumber<Decimal128>(columns, res_values) || levenshteinWeightedNumber<Decimal256>(columns, res_values)
            || levenshteinWeightedNumber<DateTime64>(columns, res_values)
            || levenshteinWeightedString<ColumnString>(columns, res_values) || levenshteinWeightedString<ColumnFixedString>(columns, res_values))
            return res;
        levenshteinWeightedGeneric(columns, res_values);
        return res;
    }
};

struct SimpleLevenshtein
{
    static constexpr auto name{"arrayLevenshtein"};
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
    auto res = ColumnUInt32::create();
    ColumnUInt32::Container & res_values = res->getData();
    res_values.resize(columns[0]->size());
    if (levenshteinNumber<UInt8>(columns, res_values) || levenshteinNumber<UInt16>(columns, res_values)
        || levenshteinNumber<UInt32>(columns, res_values) || levenshteinNumber<UInt64>(columns, res_values)
        || levenshteinNumber<UInt128>(columns, res_values) || levenshteinNumber<UInt256>(columns, res_values)
        || levenshteinNumber<Int8>(columns, res_values) || levenshteinNumber<Int16>(columns, res_values)
        || levenshteinNumber<Int32>(columns, res_values) || levenshteinNumber<Int64>(columns, res_values)
        || levenshteinNumber<Int128>(columns, res_values) || levenshteinNumber<Int256>(columns, res_values)
        || levenshteinNumber<Float32>(columns, res_values) || levenshteinNumber<Float64>(columns, res_values)
        || levenshteinNumber<Decimal32>(columns, res_values) || levenshteinNumber<Decimal64>(columns, res_values)
        || levenshteinNumber<Decimal128>(columns, res_values) || levenshteinNumber<Decimal256>(columns, res_values)
        || levenshteinNumber<DateTime64>(columns, res_values)
        || levenshteinString<ColumnString>(columns, res_values) || levenshteinString<ColumnFixedString>(columns, res_values))
        return res;
    levenshteinGeneric(columns, res_values);
    return res;
}

struct Weighted
{
    static constexpr auto name{"arrayLevenshteinWeighted"};
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
     MutableColumnPtr distance = weightedLevenshteinImpl(columns);
     auto result = ColumnFloat64::create();
     ColumnFloat64::Container & res_values = result->getData();
     res_values.resize(distance->size());
     const ColumnVector<Float64> * column_from_weights = checkAndGetColumn<ColumnVector<Float64>>(&columns[2]->getData());
     const ColumnVector<Float64> * column_to_weights = checkAndGetColumn<ColumnVector<Float64>>(&columns[3]->getData());
     if (!column_from_weights || !column_to_weights)
         throw Exception(
                 ErrorCodes::LOGICAL_ERROR,
                 "Function {} wrong type of weight columns",
                 getName());
     const PaddedPODArray<Float64> & vec_from_weights = column_from_weights->getData();
     const ColumnArray::Offsets & from_weights_offsets = columns[2]->getOffsets();
     ColumnArray::Offset prev_from_weights_offset = 0;

     const PaddedPODArray<Float64> & vec_to_weights = column_to_weights->getData();
     const ColumnArray::Offsets & to_weights_offsets = columns[3]->getOffsets();
     ColumnArray::Offset prev_to_weights_offset = 0;
     for (size_t row = 0; row < distance->size(); row++)
     {
         const std::vector<Float64> from_weights(vec_from_weights.begin() + prev_from_weights_offset, vec_from_weights.begin() + from_weights_offsets[row]);
         prev_from_weights_offset = from_weights_offsets[row];
         const std::vector<Float64> to_weights(vec_to_weights.begin() + prev_to_weights_offset, vec_to_weights.begin() + to_weights_offsets[row]);
         prev_to_weights_offset = to_weights_offsets[row];

         if (distance->getFloat64(row) == 0)
         {
             res_values[row] = 1.0;
             continue;
         }
         Float64 weights_sum = std::accumulate(from_weights.begin(), from_weights.end(), 0.0) +
                               std::accumulate(to_weights.begin(), to_weights.end(), 0.0);
         if (weights_sum == 0.0)
         {
             res_values[row] = 1.0;
             continue;
         }
         res_values[row] = 1.0 - (distance->getFloat64(row) / weights_sum);
     }
     return result;
}

REGISTER_FUNCTION(ArrayLevenshtein)
{
    factory.registerFunction<FunctionArrayLevenshtein<SimpleLevenshtein>>(
        {.description = R"(
Calculates Levenshtein distance for two arrays.
)",
         .syntax{"arrayLevenshtein(lhs, rhs)"},
         .arguments{{"lhs", "left-hand side array"}, {"rhs", "right-hand side array"}},
         .returned_value{"Levenshtein distance between left-hand and right-hand arrays"},
         .examples{{{
             "Query",
             "SELECT arrayLevenshtein([1, 2, 3, 4], [1, 2, 3, 4])",
             R"(
┌─arrayLevenshtein([1, 2, 4], [1, 2, 3])─┐
│                                      1 │
└────────────────────────────────────────┘
)",
         }}},
         .category{"Arrays"}});

    factory.registerFunction<FunctionArrayLevenshtein<Weighted>>(
        {.description = R"(
Calculates Levenshtein distance for two arrays with custom weights for each element. Number of elements for array and its weights should match
)",
         .syntax{"arrayLevenshteinWeighted(lhs, rhs, lhs_weights, rhs_weights)"},
         .arguments{
             {"lhs", "left-hand side array"},
             {"rhs", "right-hand side array"},
             {"lhs_weights", "right-hand side weights"},
             {"rhs_weights", "right-hand side weights"},
         },
         .returned_value{"Levenshtein distance between left-hand and right-hand arrays with custom weights for each element"},
         .examples{{{
            "Query",
            "SELECT arrayLevenshteinWeighted(['A', 'B', 'C'], ['A', 'K', 'L'], [1.0, 2, 3], [3.0, 4, 5])",
            R"(
┌─arrayLevenshteinWeighted(['A', 'B', 'C'], ['A', 'K', 'L'], [1.0, 2, 3], [3.0, 4, 5])─┐
│                                                                                   14 │
└──────────────────────────────────────────────────────────────────────────────────────┘
)",
         }}},
         .category{"Arrays"}});

    factory.registerFunction<FunctionArrayLevenshtein<Similarity>>(
        {.description = R"(
Calculates arrays' similarity from 0 to 1 based on weighed Levenshtein distance. Accepts the same arguments as `arrayLevenshteinWeighted` function.
)",
         .syntax{"arraySimilarity(lhs, rhs, lhs_weights, rhs_weights)"},
         .arguments{
             {"lhs", "left-hand side array"},
             {"rhs", "right-hand side array"},
             {"lhs_weights", "right-hand side weights"},
             {"rhs_weights", "right-hand side weights"},
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
