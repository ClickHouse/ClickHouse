#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <cmath>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Kahan-Babuska-Neumaier summation step, the same as `kahansum.Inc` in Prometheus.
void kahanAdd(Float64 x, Float64 & sum, Float64 & compensation)
{
    const Float64 t = sum + x;
    if (std::isinf(t))
        compensation = 0;
    else if (std::abs(sum) >= std::abs(x))
        compensation += (sum - t) + x;
    else
        compensation += (x - t) + sum;
    sum = t;
}

/// The average as PromQL `avg` computes it: a compensated sum that turns into a compensated mean once the sum would overflow.
struct AvgPrometheusData
{
    /// The sum of the values, or their mean once `is_mean` is set.
    Float64 value = 0;
    Float64 compensation = 0;
    UInt64 count = 0;
    bool is_mean = false;

    void add(Float64 x)
    {
        ++count;
        if (count == 1)
        {
            value = x;
            return;
        }

        if (!is_mean)
        {
            Float64 new_value = value;
            Float64 new_compensation = compensation;
            kahanAdd(x, new_value, new_compensation);
            if (!std::isinf(new_value))
            {
                value = new_value;
                compensation = new_compensation;
                return;
            }
            is_mean = true;
            value /= static_cast<Float64>(count - 1);
            compensation /= static_cast<Float64>(count - 1);
        }

        const Float64 n = static_cast<Float64>(count);
        value *= (n - 1) / n;
        compensation *= (n - 1) / n;
        kahanAdd(x / n, value, compensation);
    }

    void merge(const AvgPrometheusData & rhs)
    {
        if (rhs.count == 0)
            return;
        if (count == 0)
        {
            *this = rhs;
            return;
        }

        if (!is_mean && !rhs.is_mean)
        {
            Float64 new_value = value;
            Float64 new_compensation = compensation;
            kahanAdd(rhs.value, new_value, new_compensation);
            if (!std::isinf(new_value))
            {
                value = new_value;
                compensation = new_compensation + rhs.compensation;
                count += rhs.count;
                return;
            }
        }

        /// Add the means weighted by their counts, which cannot overflow.
        const Float64 total = static_cast<Float64>(count + rhs.count);
        const Float64 weight = (is_mean ? static_cast<Float64>(count) : 1) / total;
        const Float64 rhs_weight = (rhs.is_mean ? static_cast<Float64>(rhs.count) : 1) / total;
        value *= weight;
        compensation *= weight;
        kahanAdd(rhs.value * rhs_weight, value, compensation);
        compensation += rhs.compensation * rhs_weight;
        count += rhs.count;
        is_mean = true;
    }

    Float64 get() const
    {
        if (is_mean)
            return value + compensation;
        const Float64 n = static_cast<Float64>(count);
        return value / n + compensation / n;
    }
};

template <typename T>
class AggregateFunctionAvgPrometheus final : public IAggregateFunctionDataHelper<AvgPrometheusData, AggregateFunctionAvgPrometheus<T>>
{
public:
    explicit AggregateFunctionAvgPrometheus(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AvgPrometheusData, AggregateFunctionAvgPrometheus<T>>(
            argument_types_, {}, std::make_shared<DataTypeFloat64>())
    {
    }

    String getName() const override { return "avgPrometheus"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(static_cast<Float64>(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]));
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & data = this->data(place);
        writeBinaryLittleEndian(data.value, buf);
        writeBinaryLittleEndian(data.compensation, buf);
        writeVarUInt(data.count, buf);
        writeBinary(data.is_mean, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & data = this->data(place);
        readBinaryLittleEndian(data.value, buf);
        readBinaryLittleEndian(data.compensation, buf);
        readVarUInt(data.count, buf);
        readBinary(data.is_mean, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).get());
    }
};

AggregateFunctionPtr createAggregateFunctionAvgPrometheus(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionAvgPrometheus>(*argument_types[0], argument_types));
    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_types[0]->getName(), name);
    return res;
}

}

void registerAggregateFunctionAvgPrometheus(AggregateFunctionFactory & factory);
void registerAggregateFunctionAvgPrometheus(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the arithmetic mean the way the `avg` aggregation operator of PromQL does.

The values are summed with the Kahan-Babuska-Neumaier compensated summation algorithm.
If the sum would overflow, the function continues with an incremental mean, so the average of large finite values stays finite.
Slower than [`avg`](/reference/functions/aggregate-functions/avg).
    )";
    FunctionDocumentation::Syntax syntax = "avgPrometheus(x)";
    FunctionDocumentation::Arguments arguments = {{"x", "Input values.", {"(U)Int*", "Float*"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the arithmetic mean, or `NaN` if the input is empty.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "The sum overflows",
        R"(
SELECT avg(x), avgPrometheus(x) FROM values('x Float64', 1e308, 1e308);
        )",
        R"(
┌─avg(x)─┬─avgPrometheus(x)─┐
│    inf │            1e308 │
└────────┴──────────────────┘
        )"
    },
    {
        "Compensated summation",
        R"(
SELECT avg(x), avgPrometheus(x) FROM values('x Float64', 1, 1e100, 1, -1e100);
        )",
        R"(
┌─avg(x)─┬─avgPrometheus(x)─┐
│      0 │              0.5 │
└────────┴──────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("avgPrometheus", {createAggregateFunctionAvgPrometheus, documentation});
}

}
