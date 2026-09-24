#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/ExponentiallySmoothedCounter.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** See the comments in ExponentiallySmoothedCounter.h
  */
class AggregateFunctionExponentialMovingAverage final
    : public IAggregateFunctionDataHelper<ExponentiallySmoothedAverage, AggregateFunctionExponentialMovingAverage>
{
private:
    String name;
    Float64 half_decay;

public:
    AggregateFunctionExponentialMovingAverage(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<ExponentiallySmoothedAverage, AggregateFunctionExponentialMovingAverage>(argument_types_, params, createResultType())
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one parameter: "
                "half decay time.", getName());

        half_decay = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
    }

    String getName() const override
    {
        return "exponentialMovingAverage";
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & value = columns[0]->getFloat64(row_num);
        const auto & time = columns[1]->getFloat64(row_num);
        data(place).add(value, time, half_decay);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs), half_decay);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(data(place).value, buf);
        writeBinary(data(place).time, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(data(place).value, buf);
        readBinary(data(place).time, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column = assert_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(data(place).get(half_decay));
    }
};

void registerAggregateFunctionExponentialMovingAverage(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the exponential moving average of values for the determined time.

Each `value` corresponds to the determinate `timeunit`.
The half-life `x` is the time lag at which the exponential weights decay by one-half.
The function returns a weighted average: the older the time point, the less weight the corresponding value is considered to be.
    )";
    FunctionDocumentation::Syntax syntax = "exponentialMovingAverage(x)(value, timeunit)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Value.", {"(U)Int*", "Float*", "Decimal"}},
        {"timeunit", "Timeunit. Timeunit is not timestamp (seconds), it's an index of the time interval. Can be calculated using `intDiv`.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"x", "Half-life period.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an exponentially smoothed moving average of the values for the past `x` time at the latest point of time.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic exponential moving average",
        R"(
-- Input table with temperature data
SELECT exponentialMovingAverage(5)(temperature, timestamp)
FROM VALUES('temperature Int32, timestamp Int32',
    (95, 1), (95, 2), (95, 3), (96, 4), (96, 5), (96, 6), (96, 7),
    (97, 8), (97, 9), (97, 10), (97, 11), (98, 12), (98, 13), (98, 14),
    (98, 15), (99, 16), (99, 17), (99, 18), (100, 19), (100, 20))
        )",
        R"(
┌─exponentialM⋯ timestamp)─┐
│        92.25779635374204 │
└──────────────────────────┘
        )"
    },
    {
        "Example with the `bar` function",
        R"(
SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 1, 50) AS bar
FROM
(
    SELECT
        (number = 0) OR (number >= 25) AS value,
        number AS time,
        exponentialMovingAverage(10)(value, time) OVER (Rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
)
        )",
        R"(
┌─value─┬─time─┬─round(exp_smooth, 3)─┬─bar────────────────────────────────────────┐
│     1 │    0 │                0.067 │ ███▎                                       │
│     0 │    1 │                0.062 │ ███                                        │
│     0 │    2 │                0.058 │ ██▊                                        │
│     0 │    3 │                0.054 │ ██▋                                        │
│     0 │    4 │                0.051 │ ██▌                                        │
│     0 │    5 │                0.047 │ ██▎                                        │
│     0 │    6 │                0.044 │ ██▏                                        │
│     0 │    7 │                0.041 │ ██                                         │
│     0 │    8 │                0.038 │ █▊                                         │
│     0 │    9 │                0.036 │ █▋                                         │
│     0 │   10 │                0.033 │ █▋                                         │
│     0 │   11 │                0.031 │ █▌                                         │
│     0 │   12 │                0.029 │ █▍                                         │
│     0 │   13 │                0.027 │ █▎                                         │
│     0 │   14 │                0.025 │ █▎                                         │
│     0 │   15 │                0.024 │ █▏                                         │
│     0 │   16 │                0.022 │ █                                          │
│     0 │   17 │                0.021 │ █                                          │
│     0 │   18 │                0.019 │ ▊                                          │
│     0 │   19 │                0.018 │ ▊                                          │
│     0 │   20 │                0.017 │ ▋                                          │
│     0 │   21 │                0.016 │ ▋                                          │
│     0 │   22 │                0.015 │ ▋                                          │
│     0 │   23 │                0.014 │ ▋                                          │
│     0 │   24 │                0.013 │ ▋                                          │
│     1 │   25 │                0.079 │ ███▊                                       │
│     1 │   26 │                 0.14 │ ███████                                    │
│     1 │   27 │                0.198 │ █████████▊                                 │
│     1 │   28 │                0.252 │ ████████████▌                              │
│     1 │   29 │                0.302 │ ███████████████                            │
│     1 │   30 │                0.349 │ █████████████████▍                         │
│     1 │   31 │                0.392 │ ███████████████████▌                       │
│     1 │   32 │                0.433 │ █████████████████████▋                     │
│     1 │   33 │                0.471 │ ███████████████████████▌                   │
│     1 │   34 │                0.506 │ █████████████████████████▎                 │
│     1 │   35 │                0.539 │ ██████████████████████████▊                │
│     1 │   36 │                 0.57 │ ████████████████████████████▌              │
│     1 │   37 │                0.599 │ █████████████████████████████▊             │
│     1 │   38 │                0.626 │ ███████████████████████████████▎           │
│     1 │   39 │                0.651 │ ████████████████████████████████▌          │
│     1 │   40 │                0.674 │ █████████████████████████████████▋         │
│     1 │   41 │                0.696 │ ██████████████████████████████████▋        │
│     1 │   42 │                0.716 │ ███████████████████████████████████▋       │
│     1 │   43 │                0.735 │ ████████████████████████████████████▋      │
│     1 │   44 │                0.753 │ █████████████████████████████████████▋     │
│     1 │   45 │                 0.77 │ ██████████████████████████████████████▍    │
│     1 │   46 │                0.785 │ ███████████████████████████████████████▎   │
│     1 │   47 │                  0.8 │ ███████████████████████████████████████▊   │
│     1 │   48 │                0.813 │ ████████████████████████████████████████▋  │
│     1 │   49 │                0.825 │ █████████████████████████████████████████▎ │
└───────┴──────┴──────────────────────┴────────────────────────────────────────────┘
        )"
    },
    {
        "Window function usage with time-based calculation",
        R"(
CREATE TABLE data
ENGINE = Memory AS
SELECT
    10 AS value,
    toDateTime('2020-01-01') + (3600 * number) AS time
FROM numbers_mt(10);

-- Calculate timeunit using intDiv
SELECT
    value,
    time,
    exponentialMovingAverage(1)(value, intDiv(toUInt32(time), 3600)) OVER (ORDER BY time ASC) AS res,
    intDiv(toUInt32(time), 3600) AS timeunit
FROM data
ORDER BY time ASC
        )",
        R"(
┌─value─┬────────────────time─┬─────────res─┬─timeunit─┐
│    10 │ 2020-01-01 00:00:00 │           5 │   438288 │
│    10 │ 2020-01-01 01:00:00 │         7.5 │   438289 │
│    10 │ 2020-01-01 02:00:00 │        8.75 │   438290 │
│    10 │ 2020-01-01 03:00:00 │       9.375 │   438291 │
│    10 │ 2020-01-01 04:00:00 │      9.6875 │   438292 │
│    10 │ 2020-01-01 05:00:00 │     9.84375 │   438293 │
│    10 │ 2020-01-01 06:00:00 │    9.921875 │   438294 │
│    10 │ 2020-01-01 07:00:00 │   9.9609375 │   438295 │
│    10 │ 2020-01-01 08:00:00 │  9.98046875 │   438296 │
│    10 │ 2020-01-01 09:00:00 │ 9.990234375 │   438297 │
└───────┴─────────────────────┴─────────────┴──────────┘
        )"
    }
    };
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};
    factory.registerFunction("exponentialMovingAverage",
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *) -> AggregateFunctionPtr
        {
            assertBinary(name, argument_types);
            for (const auto & type : argument_types)
                if (!isNumber(*type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Both arguments for aggregate function {} must have numeric type, got {}", name, type->getName());
            return std::make_shared<AggregateFunctionExponentialMovingAverage>(argument_types, params);
        },
        documentation
    });
}

}
