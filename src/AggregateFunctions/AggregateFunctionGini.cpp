#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionNull.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/NaNUtils.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>

#include <type_traits>
#include <pdqsort.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
template <typename Value>
struct AggregateFunctionGiniData
{
    /// The memory will be allocated to several elements at once, so that the state occupies 64 bytes.
    static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<Value>);
    using Array = PODArrayWithStackMemory<Value, bytes_in_arena>;
    Array array;

    [[noreturn]] static void throwTooLargeArraySize()
    {
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                        "Too large array size in aggregate function `gini` (maximum: {})", GINI_MAX_ARRAY_SIZE);
    }

    static void checkArraySize(size_t size)
    {
        if (unlikely(size > GINI_MAX_ARRAY_SIZE))
            throwTooLargeArraySize();
    }

    static bool isNaNValue(const Value & value)
    {
        if constexpr (std::is_same_v<Value, BFloat16>)
            return value.isNaN();
        else
            return isNaN(value);
    }

    static bool isFiniteValue(const Value & value)
    {
        if constexpr (std::is_same_v<Value, BFloat16>)
            return value.isFinite();
        else
            return isFinite(value);
    }

    static long double toLongDouble(const Value & value)
    {
        if constexpr (is_decimal<Value>)
            return static_cast<long double>(value.value);
        else if constexpr (std::is_same_v<Value, BFloat16>)
            return static_cast<long double>(static_cast<Float32>(value));
        else
            return static_cast<long double>(value);
    }

    void add(const Value & x)
    {
        /// Skip NaNs as they are not compatible with comparison sorting.
        if (isNaNValue(x))
            return;

        if (!isFiniteValue(x))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function `gini` does not support infinite values");

        if (x < Value{})
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function `gini` does not support negative values");

        if (unlikely(array.size() >= GINI_MAX_ARRAY_SIZE))
            throwTooLargeArraySize();

        array.push_back(x);
    }

    void merge(const AggregateFunctionGiniData<Value> & rhs)
    {
        if (unlikely(array.size() > GINI_MAX_ARRAY_SIZE || rhs.array.size() > GINI_MAX_ARRAY_SIZE - array.size()))
            throwTooLargeArraySize();

        array.insert(rhs.array.begin(), rhs.array.end());
    }

    void serialize(WriteBuffer & buf) const
    {
        checkArraySize(array.size());
        writeVarUInt(array.size(), buf);
        buf.write(reinterpret_cast<const char *>(array.data()), array.size() * sizeof(array[0]));
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        checkArraySize(size);
        array.resize(size);
        buf.readStrict(reinterpret_cast<char *>(array.data()), size * sizeof(array[0]));
    }

    /** Gini coefficient of the collected values. For sorted values x, the sum of
      * all pairwise differences can be computed from adjacent differences:
      *     sum_{i < j}(x_j - x_i) = sum_{i=1}^{n-1} i * (n - i) * (x_{i+1} - x_i)
      * and G = pairwise_difference_sum / (n * sum(x)). Values are normalized by
      * the maximum before accumulation to avoid overflow. Computing adjacent
      * differences in the input type also preserves differences between large
      * integers and decimals which would be lost by an early `Float64` conversion.
      * Returns NaN when there are fewer than 2 values or when the sum of values is 0
      * (the coefficient is undefined in these cases).
      */
    Float64 getResult()
    {
        const size_t n = array.size();
        if (n < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        pdqsort(array.begin(), array.end());

        const Value & maximum = array.back();
        if (maximum == Value{} || !isFiniteValue(maximum))
            return std::numeric_limits<Float64>::quiet_NaN();

        const long double maximum_float = toLongDouble(maximum);
        long double normalized_sum = 0;
        for (const auto & value : array)
            normalized_sum += toLongDouble(value) / maximum_float;

        long double normalized_pairwise_difference_sum = 0;
        for (size_t i = 0; i + 1 < n; ++i)
        {
            long double difference = 0;
            if constexpr (is_floating_point<Value> || std::is_same_v<Value, BFloat16>)
                difference = toLongDouble(array[i + 1]) - toLongDouble(array[i]);
            else
                difference = toLongDouble(array[i + 1] - array[i]);

            normalized_pairwise_difference_sum += difference / maximum_float
                * static_cast<long double>(i + 1) * static_cast<long double>(n - i - 1);
        }

        return static_cast<Float64>(normalized_pairwise_difference_sum / (static_cast<long double>(n) * normalized_sum));
    }
};

template <typename Value>
class AggregateFunctionGini final
    : public IAggregateFunctionDataHelper<AggregateFunctionGiniData<Value>, AggregateFunctionGini<Value>>
{
private:
    using ColVecType = ColumnVectorOrDecimal<Value>;

public:
    String getName() const override { return "gini"; }

    explicit AggregateFunctionGini(const DataTypePtr & argument_type)
        : IAggregateFunctionDataHelper<AggregateFunctionGiniData<Value>, AggregateFunctionGini<Value>>(
            {argument_type}, {}, std::make_shared<DataTypeFloat64>())
    {}

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties &) const override
    {
        return std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function, arguments, params);
    }

    UnorderedSetWithMemoryTracking<size_t> getArgumentsThatCanBeOnlyNull() const override
    {
        return {0};
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).add(column.getData()[row_num]);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).getResult());
    }
};

AggregateFunctionPtr createAggregateFunctionGini(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    if (argument_type->onlyNull())
        return std::make_shared<AggregateFunctionGini<Float64>>(std::make_shared<DataTypeFloat64>());

    if (!isNumber(argument_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument of aggregate function {}, must be a number",
                        argument_type->getName(), name);

    WhichDataType which(argument_type);
    AggregateFunctionPtr function(createWithNumericType<AggregateFunctionGini>(*argument_type, argument_type));
    if (function)
        return function;

    if (which.idx == TypeIndex::Decimal32) return std::make_shared<AggregateFunctionGini<Decimal32>>(argument_type);
    if (which.idx == TypeIndex::Decimal64) return std::make_shared<AggregateFunctionGini<Decimal64>>(argument_type);
    if (which.idx == TypeIndex::Decimal128) return std::make_shared<AggregateFunctionGini<Decimal128>>(argument_type);
    if (which.idx == TypeIndex::Decimal256) return std::make_shared<AggregateFunctionGini<Decimal256>>(argument_type);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of aggregate function {}, must be a number",
                    argument_type->getName(), name);
}

}

void registerAggregateFunctionGini(AggregateFunctionFactory & factory);
void registerAggregateFunctionGini(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the [Gini coefficient](https://en.wikipedia.org/wiki/Gini_coefficient) of a column of finite, non-negative numeric values, a measure of inequality in a distribution.

The result ranges from `0` (perfect equality: all values are the same) to a maximum approaching `1` as `n` grows (perfect inequality: one value holds everything and the rest are zero). For a finite sample of `n` values the maximum is `(n - 1) / n`.

All passed values are collected in their input type and then sorted before a numerically stable final calculation. The coefficient is computed from normalized adjacent pairwise differences and returned as `Float64`, so it is rounded to `Float64` precision. Therefore, the function consumes `O(n)` memory, where `n` is the number of values passed. `NaN` values are skipped, while infinite values are rejected.
    )";
    FunctionDocumentation::Syntax syntax = "gini(expr)";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression resulting in finite, non-negative numeric values.", {"(U)Int*", "Float*", "Decimal"}}
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
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true };
    factory.registerFunction("gini", {createAggregateFunctionGini, documentation, properties});
}

}
