#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/NaNUtils.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>

#include <numeric>
#include <pdqsort.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

constexpr size_t GINI_MAX_ARRAY_SIZE = 1'000'000'000;

/** The state simply collects all values; the coefficient is computed at finalize time.
  * Gini requires the values sorted, which cannot be done incrementally with a
  * mergeable small state, so we keep every value (O(n) memory), like quantileExact.
  */
struct AggregateFunctionGiniData
{
    /// The memory will be allocated to several elements at once, so that the state occupies 64 bytes.
    static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<Float64>);
    using Array = PODArrayWithStackMemory<Float64, bytes_in_arena>;
    Array array;

    void add(Float64 x)
    {
        /// Skip NaNs as they are not compatible with comparison sorting.
        if (!isNaN(x))
            array.push_back(x);
    }

    void merge(const AggregateFunctionGiniData & rhs)
    {
        array.insert(rhs.array.begin(), rhs.array.end());
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(array.size(), buf);
        buf.write(reinterpret_cast<const char *>(array.data()), array.size() * sizeof(array[0]));
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        if (unlikely(size > GINI_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size in aggregate function gini (maximum: {})", GINI_MAX_ARRAY_SIZE);
        array.resize(size);
        buf.readStrict(reinterpret_cast<char *>(array.data()), size * sizeof(array[0]));
    }

    /** Gini coefficient of the collected values.
      * For values x sorted ascending, with indices i from 1 to n:
      *     G = 2 * sum(i * x_i) / (n * sum(x_i)) - (n + 1) / n
      * Returns NaN when there are fewer than 2 values or when the sum of values is 0
      * (the coefficient is undefined in these cases).
      */
    Float64 getResult()
    {
        const size_t n = array.size();
        if (n < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        pdqsort(array.begin(), array.end());

        Float64 sum = 0.0;
        Float64 weighted_sum = 0.0;
        for (size_t i = 0; i < n; ++i)
        {
            sum += array[i];
            weighted_sum += static_cast<Float64>(i + 1) * array[i];
        }

        if (sum == 0.0)
            return std::numeric_limits<Float64>::quiet_NaN();

        return 2.0 * weighted_sum / (static_cast<Float64>(n) * sum) - (static_cast<Float64>(n) + 1.0) / static_cast<Float64>(n);
    }
};

class AggregateFunctionGini final
    : public IAggregateFunctionDataHelper<AggregateFunctionGiniData, AggregateFunctionGini>
{
public:
    String getName() const override { return "gini"; }

    explicit AggregateFunctionGini(const DataTypePtr & argument_type)
        : IAggregateFunctionDataHelper<AggregateFunctionGiniData, AggregateFunctionGini>({argument_type}, {}, std::make_shared<DataTypeFloat64>())
    {}

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        data(place).add(columns[0]->getFloat64(row_num));
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(data(place).getResult());
    }
};

AggregateFunctionPtr createAggregateFunctionGini(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    if (!isNumber(argument_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument of aggregate function {}, must be a number",
                        argument_type->getName(), name);

    return std::make_shared<AggregateFunctionGini>(argument_type);
}

}

void registerAggregateFunctionGini(AggregateFunctionFactory & factory);
void registerAggregateFunctionGini(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the [Gini coefficient](https://en.wikipedia.org/wiki/Gini_coefficient) of a numeric column, a measure of inequality in a distribution.

The result ranges from `0` (perfect equality: all values are the same) to a maximum approaching `1` as `n` grows (perfect inequality: one value holds everything and the rest are zero). For a finite sample of `n` values the maximum is `(n - 1) / n`.

To get the exact value, all the passed values are collected and then sorted. Therefore, the function consumes `O(n)` memory, where `n` is the number of values passed. `NaN` values are skipped.
    )";
    FunctionDocumentation::Syntax syntax = "gini(expr)";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression resulting in a numeric data type.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"The Gini coefficient, or `NaN` if there are fewer than 2 values or the sum of the values is 0.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Equal values",
        R"(
SELECT gini(x) FROM (SELECT 100 AS x FROM numbers(10));
        )",
        R"(
┌─gini(x)─┐
│       0 │
└─────────┘
        )"
    },
    {
        "Skewed distribution",
        R"(
SELECT gini(x) FROM (SELECT number AS x FROM numbers(10));
        )",
        R"(
┌─────────────gini(x)─┐
│ 0.3666666666666665  │
└─────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction("gini", {createAggregateFunctionGini, documentation});
}

}
