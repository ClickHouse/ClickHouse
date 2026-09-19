#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <base/extended_types.h>
#include <base/sort.h>
#include <Common/NaNUtils.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <type_traits>


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

constexpr size_t MAD_MAX_ARRAY_SIZE = 1'000'000'000;

template <typename Value>
struct MedianBounds
{
    Value lower;
    Value upper;
};

template <typename Value, typename Array, typename Comparator>
MedianBounds<Value> selectMedianBounds(Array & array, Comparator comparator)
{
    const size_t lower_index = (array.size() - 1) / 2;
    const size_t upper_index = array.size() / 2;

    ::nth_element(array.begin(), array.begin() + lower_index, array.end(), comparator);

    MedianBounds<Value> result{array[lower_index], array[lower_index]};
    if (lower_index != upper_index)
        result.upper = *std::min_element(array.begin() + upper_index, array.end(), comparator);

    return result;
}

template <typename Value>
struct IntegerDistanceComparator
{
    using UnsignedValue = std::make_unsigned_t<Value>;

    UnsignedValue lower_bound;
    UnsignedValue upper_bound;

    explicit IntegerDistanceComparator(Value lower, Value upper)
        : lower_bound(toOrdered(lower))
        , upper_bound(toOrdered(upper))
    {
    }

    static UnsignedValue toOrdered(Value value)
    {
        if constexpr (std::is_signed_v<Value>)
        {
            constexpr auto sign_bit = static_cast<UnsignedValue>(1) << (sizeof(Value) * 8 - 1);
            return static_cast<UnsignedValue>(value) ^ sign_bit;
        }
        else
        {
            return value;
        }
    }

    UnsignedValue distance(Value value) const
    {
        const auto ordered = toOrdered(value);
        if (ordered < lower_bound)
            return lower_bound - ordered;
        if (ordered > upper_bound)
            return ordered - upper_bound;
        return 0;
    }

    bool operator()(Value lhs, Value rhs) const { return distance(lhs) < distance(rhs); }
};

template <typename Value>
struct FloatDistanceComparator
{
    Float64 center;

    static Float64 distance(Value value, Float64 center) { return std::abs(static_cast<Float64>(value) - center); }

    bool operator()(Value lhs, Value rhs) const { return distance(lhs, center) < distance(rhs, center); }
};

Float64 interpolate(Float64 lower, Float64 upper)
{
    if (lower == upper)
        return lower;
    if (!isFinite(lower) || !isFinite(upper))
        return std::numeric_limits<Float64>::infinity();
    return lower + (upper - lower) * 0.5;
}

template <typename Value>
class AggregateFunctionMedianAbsoluteDeviationData
{
public:
    static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<Value>);
    using Array = PODArrayWithStackMemory<Value, bytes_in_arena>;

    Array array;

    void add(Value value)
    {
        if (isNaN(value))
            return;

        if (!isFinite(value))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function `mad` does not support infinite values");

        reserveForAdditional(1);
        array.push_back(value);
    }

    void addBatch(const Value * values, size_t row_begin, size_t row_end, const UInt8 * null_map, const UInt8 * if_map)
    {
        if (row_begin == row_end)
            return;

        const size_t size = row_end - row_begin;
        if constexpr (is_integer<Value>)
        {
            if (!null_map && !if_map)
            {
                reserveForAdditional(size);
                array.insert(values + row_begin, values + row_end);
                return;
            }
        }

        reserveForAdditional(size);
        for (size_t i = row_begin; i < row_end; ++i)
        {
            if ((null_map && null_map[i]) || (if_map && !if_map[i]))
                continue;
            add(values[i]);
        }
    }

    void merge(const AggregateFunctionMedianAbsoluteDeviationData & rhs)
    {
        reserveForAdditional(rhs.array.size());
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

    Float64 getResult()
    {
        if (array.empty())
            return std::numeric_limits<Float64>::quiet_NaN();

        const auto first_median = selectMedianBounds<Value>(array, std::less<Value>{});

        if constexpr (is_integer<Value>)
        {
            const IntegerDistanceComparator<Value> comparator(first_median.lower, first_median.upper);
            const auto second_median = selectMedianBounds<Value>(array, comparator);

            const Int128 numerator = static_cast<Int128>(comparator.distance(second_median.lower))
                + static_cast<Int128>(comparator.distance(second_median.upper))
                + static_cast<Int128>(comparator.upper_bound - comparator.lower_bound);
            return static_cast<Float64>(numerator) * 0.5;
        }
        else
        {
            const Float64 lower = static_cast<Float64>(first_median.lower);
            const Float64 upper = static_cast<Float64>(first_median.upper);
            const Float64 center = std::midpoint(lower, upper);
            const FloatDistanceComparator<Value> comparator{center};
            const auto second_median = selectMedianBounds<Value>(array, comparator);
            return interpolate(comparator.distance(second_median.lower, center), comparator.distance(second_median.upper, center));
        }
    }

private:
    static void checkArraySize(size_t size)
    {
        if (unlikely(size > MAD_MAX_ARRAY_SIZE))
            throw Exception(
                ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size in aggregate function `mad` (maximum: {})", MAD_MAX_ARRAY_SIZE);
    }

    void reserveForAdditional(size_t additional)
    {
        if (unlikely(array.size() > MAD_MAX_ARRAY_SIZE || additional > MAD_MAX_ARRAY_SIZE - array.size()))
            checkArraySize(MAD_MAX_ARRAY_SIZE + 1);

        array.reserve(array.size() + additional);
    }
};

template <typename Value>
class AggregateFunctionMedianAbsoluteDeviation final : public IAggregateFunctionDataHelper<
                                                           AggregateFunctionMedianAbsoluteDeviationData<Value>,
                                                           AggregateFunctionMedianAbsoluteDeviation<Value>>
{
private:
    using Data = AggregateFunctionMedianAbsoluteDeviationData<Value>;
    using ColVecType = ColumnVector<Value>;

public:
    explicit AggregateFunctionMedianAbsoluteDeviation(const DataTypePtr & argument_type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionMedianAbsoluteDeviation<Value>>(
              {argument_type}, {}, std::make_shared<DataTypeFloat64>())
    {
    }

    String getName() const override { return "mad"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        this->data(place).add(column.getData()[row_num]);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        const UInt8 * if_map = nullptr;
        if (if_argument_pos >= 0)
            if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        this->data(place).addBatch(column.getData().data(), row_begin, row_end, nullptr, if_map);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        const UInt8 * if_map = nullptr;
        if (if_argument_pos >= 0)
            if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();

        this->data(place).addBatch(column.getData().data(), row_begin, row_end, null_map, if_map);
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

AggregateFunctionPtr createAggregateFunctionMedianAbsoluteDeviation(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

// NOLINTBEGIN(bugprone-macro-parentheses) -- TYPE is a type used as a template argument.
#define DISPATCH(TYPE) \
    if (WhichDataType(argument_type).idx == TypeIndex::TYPE) \
        return std::make_shared<AggregateFunctionMedianAbsoluteDeviation<TYPE>>(argument_type);
    FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
// NOLINTEND(bugprone-macro-parentheses)

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of argument of aggregate function {}, must be a basic numeric type",
        argument_type->getName(),
        name);
}

}

void registerAggregateFunctionMedianAbsoluteDeviation(AggregateFunctionFactory & factory);
void registerAggregateFunctionMedianAbsoluteDeviation(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the median absolute deviation (MAD) of a numeric data sequence.

The median absolute deviation is the median of the absolute deviations from the median of the data: `median(abs(x - median(x)))`. Both medians use the inclusive R-7 method. The result is raw and unscaled, and is returned as `Float64`.

All values are retained in their input type. The second median is selected by ordering the retained values by their distance from the first median, so finalization uses `O(1)` additional memory instead of materializing a second array of deviations. `NaN` values are skipped, while infinite values are rejected.
    )";
    FunctionDocumentation::Syntax syntax = "mad(expr)";
    FunctionDocumentation::Arguments arguments = {{"expr", "Expression resulting in numeric data.", {"(U)Int*", "Float*"}}};
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = {"The median absolute deviation, or `NaN` if the data is empty.", {"Float64"}};
    FunctionDocumentation::Examples examples
        = {{"Computing the median absolute deviation",
            R"(
SELECT mad(x) FROM (SELECT arrayJoin([0, 10, 20, 30]) AS x);
        )",
            R"(
┌─mad(x)─┐
│      10 │
└────────┘
        )"},
           {"Using the alias",
            R"(
SELECT medianAbsoluteDeviation(number) FROM numbers(10);
        )",
            R"(
┌─medianAbsoluteDeviation(number)─┐
│                              2.5 │
└─────────────────────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("mad", {createAggregateFunctionMedianAbsoluteDeviation, documentation});
    factory.registerAlias("medianAbsoluteDeviation", "mad");
}

}
