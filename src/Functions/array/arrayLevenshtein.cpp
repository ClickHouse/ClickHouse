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
        for (size_t index = 2; index < 4; ++index)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[index].type.get());
            auto nested_type = array_type->getNestedType();
            if (!WhichDataType(nested_type).isFloat64())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be array of Float64. Found {} instead.",
                    toString(index + 1),
                    getName(),
                    nested_type->getName());
        }
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count
    ) const override
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

    template <typename N>
    bool simpleLevenshteinString(std::vector<const ColumnArray *> columns, ColumnUInt32::Container & res_values) const
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
            if (m==0 || n==0)
            {
                prev_from_offset = from_offsets[row];
                prev_to_offset = to_offsets[row];
                res_values[row] = static_cast<UInt32>(std::max(m, n));
                continue;
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
                    v0[i] = std::min(prev + (from_data->getDataAt(prev_from_offset + j - 1) != to_data->getDataAt(prev_to_offset + i - 1)),
                            std::min(v0[i - 1], v0[i]) + 1);
                    prev = old;
                }
            }
            prev_from_offset = from_offsets[row];
            prev_to_offset = to_offsets[row];
            res_values[row] = static_cast<UInt32>(v0[n]);
        }
        return true;
    }

    template <typename N>
    bool simpleLevenshteinNumber(std::vector<const ColumnArray *> columns, ColumnUInt32::Container & res_values) const
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
            const PaddedPODArray<N> from(vec_from.begin() + prev_from_offset, vec_from.begin() + from_offsets[row]);
            prev_from_offset = from_offsets[row];
            const PaddedPODArray<N> to(vec_to.begin() + prev_to_offset, vec_to.begin() + to_offsets[row]);
            prev_to_offset = to_offsets[row];
            const size_t m = from.size();
            const size_t n = to.size();
            if (m==0 || n==0)
            {
                res_values[row] = static_cast<UInt32>(std::max(m, n));
                continue;
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
            res_values[row] = static_cast<UInt32>(v0[n]);
        }
        return true;
    }

    ColumnPtr weightedLevenshteinImpl(std::vector<const ColumnArray *> columns, bool similarity) const
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
                        "Arguments {} ({}, size {}) and {} ({}, size {}) of function {} must be arrays of the same size.",
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
        auto get_float = [](const Field & element) -> Float64 { return element.safeGet<Float64>(); };
        auto sum_array = [](const Array & array) -> Float64 {
            return std::accumulate(array.begin(), array.end(), 0.0, [](Float64 acc, const Field &field){return acc + field.safeGet<Float64>();});
        };
        for (size_t row = 0; row < columns[0]->size(); row++)
        {
            // Levenshtein with sliding vectors and weighted elements
            // https://www.codeproject.com/Articles/13525/Fast-memory-efficient-Levenshtein-algorithm
            Array lhs = (*columns[0])[row].safeGet<Array>();
            Array rhs = (*columns[1])[row].safeGet<Array>();
            Array lhs_w = (*columns[2])[row].safeGet<Array>();
            Array rhs_w = (*columns[3])[row].safeGet<Array>();
            const size_t m = lhs.size();
            const size_t n = rhs.size();
            if (m==0 || n==0)
            {
                if (similarity)
                    res_values[row] = m == n;
                else
                    res_values[row] = sum_array(lhs_w) + sum_array(rhs_w);
                continue;
            }
            PODArrayWithStackMemory<Float64, 64> v0(m + 1);
            PODArrayWithStackMemory<Float64, 64> v1(m + 1);
            v0[0] = 0;
            for (size_t i = 0; i < m; i++)
            {
                v0[i + 1] = v0[i] + get_float(lhs_w[i]);
            }

            for (size_t i = 0; i < n; ++i)
            {
                v1[0] = v0[0] + get_float(rhs_w[i]);
                for (size_t j = 0; j < m; ++j)
                {
                    if (lhs[j] == rhs[i])
                    {
                        v1[j + 1] = v0[j];
                        continue;
                    }

                    v1[j+1] = std::min({v0[j + 1] + get_float(rhs_w[i]),                     // deletion
                                        v1[j] + get_float(lhs_w[j]),                         // insertion
                                        v0[j] + get_float(lhs_w[j]) + get_float(rhs_w[i])}); // substitusion
                }
                std::swap(v0, v1);
            }
            if (!similarity)
            {
                // weighed Levenshtein
                res_values[row] = v0[m];
                continue;
            }
            // arrays similarity
            Float64 weights_sum = sum_array(lhs_w) + sum_array(rhs_w);
            if (weights_sum == 0.0)
            {
                res_values[row] = 1.0;
                continue;
            }
            res_values[row] = 1 - (v0[m]/weights_sum);
        }
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
    const ColumnArray * column_lhs = columns[0];
    const ColumnArray * column_rhs = columns[1];
    auto res = ColumnUInt32::create();
    ColumnUInt32::Container & res_values = res->getData();
    res_values.resize(column_lhs->size());
    if (simpleLevenshteinNumber<UInt8>(columns, res_values) || simpleLevenshteinNumber<UInt16>(columns, res_values)
        || simpleLevenshteinNumber<UInt32>(columns, res_values) || simpleLevenshteinNumber<UInt64>(columns, res_values)
        || simpleLevenshteinNumber<UInt128>(columns, res_values) || simpleLevenshteinNumber<UInt256>(columns, res_values)
        || simpleLevenshteinNumber<Int8>(columns, res_values) || simpleLevenshteinNumber<Int16>(columns, res_values)
        || simpleLevenshteinNumber<Int32>(columns, res_values) || simpleLevenshteinNumber<Int64>(columns, res_values)
        || simpleLevenshteinNumber<Int128>(columns, res_values) || simpleLevenshteinNumber<Int256>(columns, res_values)
        || simpleLevenshteinNumber<Float32>(columns, res_values) || simpleLevenshteinNumber<Float64>(columns, res_values)
        || simpleLevenshteinNumber<Decimal32>(columns, res_values) || simpleLevenshteinNumber<Decimal64>(columns, res_values)
        || simpleLevenshteinNumber<Decimal128>(columns, res_values) || simpleLevenshteinNumber<Decimal256>(columns, res_values)
        || simpleLevenshteinNumber<DateTime64>(columns, res_values)
        || simpleLevenshteinString<ColumnString>(columns, res_values) || simpleLevenshteinString<ColumnFixedString>(columns, res_values) )
        return res;
    for (size_t row = 0; row < column_lhs->size(); row++)
    {
        // Effective Levenshtein realization from Common/levenshteinDistance
        Array lhs = (*column_lhs)[row].safeGet<Array>();
        Array rhs = (*column_rhs)[row].safeGet<Array>();
        const size_t m = lhs.size();
        const size_t n = rhs.size();
        if (m==0 || n==0)
        {
            res_values[row] = static_cast<UInt32>(std::max(m, n));
            continue;
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
                v0[i] = std::min(prev + (lhs[j - 1] != rhs[i - 1]),
                                 std::min(v0[i - 1], v0[i]) + 1);
                prev = old;
            }
        }
        res_values[row] = static_cast<UInt32>(v0[n]);
    }
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
    return weightedLevenshteinImpl(columns, false);
}

struct Similarity
{
    static constexpr auto name{"arraySimilarity"};
    static constexpr size_t arguments = 4;
};

template <>
ColumnPtr FunctionArrayLevenshtein<Similarity>::execute(std::vector<const ColumnArray *> columns) const
{
    return weightedLevenshteinImpl(columns, true);
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
