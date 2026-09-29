#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <WindowFunctions/IWindowFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/WindowTransform.h>
#include <WindowFunctions/helpers.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>

#include <cmath>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct ExponentialTimeDecayedSumState
{
    Float64 previous_time;
    Float64 previous_sum;
};

struct ExponentialTimeDecayedAvgState
{
    Float64 previous_time;
    Float64 previous_sum;
    Float64 previous_count;
};

struct WindowFunctionExponentialTimeDecayedSum final : public StatefulWindowFunction<ExponentialTimeDecayedSumState>
{
    static constexpr size_t ARGUMENT_VALUE = 0;
    static constexpr size_t ARGUMENT_TIME = 1;

    static Float64 getDecayLength(const Array & parameters_, const std::string & name_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one parameter", name_);
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }

    WindowFunctionExponentialTimeDecayedSum(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(getDecayLength(parameters_, name_))
    {
        if (argument_types.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly two arguments", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_VALUE]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be a number, '{}' given",
                ARGUMENT_VALUE,
                argument_types[ARGUMENT_VALUE]->getName());
        }

        if (!isNumber(argument_types[ARGUMENT_TIME]) && !isDateTime(argument_types[ARGUMENT_TIME]) && !isDateTime64(argument_types[ARGUMENT_TIME]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be DateTime, DateTime64 or a number, '{}' given",
                ARGUMENT_TIME,
                argument_types[ARGUMENT_TIME]->getName());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);

        Float64 result = 0;

        if (transform->frame_start < transform->frame_end)
        {
            RowNumber frame_back = transform->prevRowNumber(transform->frame_end);
            Float64 back_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, frame_back);

            if (transform->prev_frame_start <= transform->frame_start
                && transform->frame_start < transform->prev_frame_end
                && transform->prev_frame_end <= transform->frame_end)
            {
                for (RowNumber i = transform->prev_frame_start; i < transform->frame_start; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    result -= std::exp((prev_t - back_t) / decay_length) * prev_val;
                }
                result += std::exp((state.previous_time - back_t) / decay_length) * state.previous_sum;
                for (RowNumber i = transform->prev_frame_end; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    result += std::exp((prev_t - back_t) / decay_length) * prev_val;
                }
            }
            else
            {
                for (RowNumber i = transform->frame_start; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    result += std::exp((prev_t - back_t) / decay_length) * prev_val;
                }
            }

            state.previous_sum = result;
            state.previous_time = back_t;
        }

        WindowRowAccess::insertResultFloat64(transform, function_index, result);
    }

    private:
        const Float64 decay_length;
};

struct WindowFunctionExponentialTimeDecayedMax final : public StatelessWindowFunction
{
    static constexpr size_t ARGUMENT_VALUE = 0;
    static constexpr size_t ARGUMENT_TIME = 1;

    static Float64 getDecayLength(const Array & parameters_, const std::string & name_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one parameter", name_);
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }

    WindowFunctionExponentialTimeDecayedMax(const std::string & name_, const DataTypes & argument_types_, const Array & parameters_)
        : StatelessWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(getDecayLength(parameters_, name_))
    {
        if (argument_types.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly two arguments", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_VALUE]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be a number, '{}' given",
                ARGUMENT_VALUE,
                argument_types[ARGUMENT_VALUE]->getName());
        }

        if (!isNumber(argument_types[ARGUMENT_TIME]) && !isDateTime(argument_types[ARGUMENT_TIME]) && !isDateTime64(argument_types[ARGUMENT_TIME]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be DateTime, DateTime64 or a number, '{}' given",
                ARGUMENT_TIME,
                argument_types[ARGUMENT_TIME]->getName());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        Float64 result = std::numeric_limits<Float64>::quiet_NaN();

        if (transform->frame_start < transform->frame_end)
        {
            result = std::numeric_limits<Float64>::lowest();
            RowNumber frame_back = transform->prevRowNumber(transform->frame_end);
            Float64 back_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, frame_back);

            for (RowNumber i = transform->frame_start; i < transform->frame_end; transform->advanceRowNumber(i))
            {
                Float64 value = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_VALUE, i);
                Float64 t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);

                /// Avoiding extra calls to `exp` and multiplications.
                if (value > result || t > back_t || result < 0)
                {
                    result = std::max(std::exp((t - back_t) / decay_length) * value, result);
                }
            }
        }

        WindowRowAccess::insertResultFloat64(transform, function_index, result);
    }

    private:
        const Float64 decay_length;
};

struct WindowFunctionExponentialTimeDecayedCount final : public StatefulWindowFunction<ExponentialTimeDecayedSumState>
{
    static constexpr size_t ARGUMENT_TIME = 0;

    static Float64 getDecayLength(const Array & parameters_, const std::string & name_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one parameter", name_);
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }

    WindowFunctionExponentialTimeDecayedCount(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(getDecayLength(parameters_, name_))
    {
        if (argument_types.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one argument", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_TIME]) && !isDateTime(argument_types[ARGUMENT_TIME]) && !isDateTime64(argument_types[ARGUMENT_TIME]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be DateTime, DateTime64 or a number, '{}' given",
                ARGUMENT_TIME,
                argument_types[ARGUMENT_TIME]->getName());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);

        Float64 result = 0;

        if (transform->frame_start < transform->frame_end)
        {
            RowNumber frame_back = transform->prevRowNumber(transform->frame_end);
            Float64 back_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, frame_back);

            if (transform->prev_frame_start <= transform->frame_start
                && transform->frame_start < transform->prev_frame_end
                && transform->prev_frame_end <= transform->frame_end)
            {
                for (RowNumber i = transform->prev_frame_start; i < transform->frame_start; transform->advanceRowNumber(i))
                {
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    result -= std::exp((prev_t - back_t) / decay_length);
                }
                result += std::exp((state.previous_time - back_t) / decay_length) * state.previous_sum;
                for (RowNumber i = transform->prev_frame_end; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    result += std::exp((prev_t - back_t) / decay_length);
                }
            }
            else
            {
                for (RowNumber i = transform->frame_start; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    result += std::exp((prev_t - back_t) / decay_length);
                }
            }

            state.previous_sum = result;
            state.previous_time = back_t;
        }

        WindowRowAccess::insertResultFloat64(transform, function_index, result);
    }

    private:
        const Float64 decay_length;
};

struct WindowFunctionExponentialTimeDecayedAvg final : public StatefulWindowFunction<ExponentialTimeDecayedAvgState>
{
    static constexpr size_t ARGUMENT_VALUE = 0;
    static constexpr size_t ARGUMENT_TIME = 1;

    static Float64 getDecayLength(const Array & parameters_, const std::string & name_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one parameter", name_);
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }

    WindowFunctionExponentialTimeDecayedAvg(const std::string & name_,
            const DataTypes & argument_types_, const Array & parameters_)
        : StatefulWindowFunction(name_, argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(getDecayLength(parameters_, name_))
    {
        if (argument_types.size() != 2)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly two arguments", name_);
        }

        if (!isNumber(argument_types[ARGUMENT_VALUE]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be a number, '{}' given",
                ARGUMENT_VALUE,
                argument_types[ARGUMENT_VALUE]->getName());
        }

        if (!isNumber(argument_types[ARGUMENT_TIME]) && !isDateTime(argument_types[ARGUMENT_TIME]) && !isDateTime64(argument_types[ARGUMENT_TIME]))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Argument {} must be DateTime, DateTime64 or a number, '{}' given",
                ARGUMENT_TIME,
                argument_types[ARGUMENT_TIME]->getName());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void windowInsertResultInto(const WindowTransform * transform,
        size_t function_index) const override
    {
        const auto & workspace = transform->workspaces[function_index];
        auto & state = getState(workspace);

        Float64 count = 0;
        Float64 sum = 0;
        Float64 result = std::numeric_limits<Float64>::quiet_NaN();

        if (transform->frame_start < transform->frame_end)
        {
            RowNumber frame_back = transform->prevRowNumber(transform->frame_end);
            Float64 back_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, frame_back);

            if (transform->prev_frame_start <= transform->frame_start
                && transform->frame_start < transform->prev_frame_end
                && transform->prev_frame_end <= transform->frame_end)
            {
                for (RowNumber i = transform->prev_frame_start; i < transform->frame_start; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    Float64 decay = std::exp((prev_t - back_t) / decay_length);
                    sum -= decay * prev_val;
                    count -= decay;
                }

                {
                    Float64 decay = std::exp((state.previous_time - back_t) / decay_length);
                    sum += decay * state.previous_sum;
                    count += decay * state.previous_count;
                }

                for (RowNumber i = transform->prev_frame_end; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    Float64 decay = std::exp((prev_t - back_t) / decay_length);
                    sum += decay * prev_val;
                    count += decay;
                }
            }
            else
            {
                for (RowNumber i = transform->frame_start; i < transform->frame_end; transform->advanceRowNumber(i))
                {
                    Float64 prev_val = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_VALUE, i);
                    Float64 prev_t = WindowRowAccess::getArgumentFloat64(transform, function_index, ARGUMENT_TIME, i);
                    Float64 decay = std::exp((prev_t - back_t) / decay_length);
                    sum += decay * prev_val;
                    count += decay;
                }
            }

            state.previous_sum = sum;
            state.previous_count = count;
            state.previous_time = back_t;

            result = sum/count;
        }

        WindowRowAccess::insertResultFloat64(transform, function_index, result);
    }

    private:
        const Float64 decay_length;
};

}

void registerWindowFunctionsExponentialTimeDecayed(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties);
void registerWindowFunctionsExponentialTimeDecayed(AggregateFunctionFactory & factory, const AggregateFunctionProperties & properties)
{
    FunctionDocumentation::Description exponentialTimeDecayedSum_description = R"(
Returns the sum of exponentially smoothed moving average values of a time series at the index `t` in time.
    )";
    FunctionDocumentation::Syntax exponentialTimeDecayedSum_syntax = "exponentialTimeDecayedSum(x)(v, t)";
    FunctionDocumentation::Arguments exponentialTimeDecayedSum_arguments = {
        {"v", "Value.", {"(U)Int*", "Float*", "Decimal"}},
        {"t", "Time.", {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::Parameters exponentialTimeDecayedSum_parameters = {
        {"x", "Time difference required for a value's weight to decay to 1/e.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue exponentialTimeDecayedSum_returned_value = {"Returns the sum of exponentially smoothed moving average values at the given point in time.", {"Float64"}};
    FunctionDocumentation::Examples exponentialTimeDecayedSum_examples = {
    {
        "Window function usage with visual representation",
        R"(
SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 10, 50) AS bar
FROM
    (
    SELECT
    (number = 0) OR (number >= 25) AS value,
    number AS time,
    exponentialTimeDecayedSum(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
    );
        )",
        R"(
┌─value─┬─time─┬─round(exp_smooth, 3)─┬─bar───────────────────────────────────────────────┐
│     1 │    0 │                    1 │ █████                                             │
│     0 │    1 │                0.905 │ ████▌                                             │
│     0 │    2 │                0.819 │ ████                                              │
│     0 │    3 │                0.741 │ ███▋                                              │
│     0 │    4 │                 0.67 │ ███▎                                              │
│     0 │    5 │                0.607 │ ███                                               │
│     0 │    6 │                0.549 │ ██▋                                               │
│     0 │    7 │                0.497 │ ██▍                                               │
│     0 │    8 │                0.449 │ ██▏                                               │
│     0 │    9 │                0.407 │ ██                                                │
│     0 │   10 │                0.368 │ █▊                                                │
│     0 │   11 │                0.333 │ █▋                                                │
│     0 │   12 │                0.301 │ █▌                                                │
│     0 │   13 │                0.273 │ █▎                                                │
│     0 │   14 │                0.247 │ █▏                                                │
│     0 │   15 │                0.223 │ █                                                 │
│     0 │   16 │                0.202 │ █                                                 │
│     0 │   17 │                0.183 │ ▉                                                 │
│     0 │   18 │                0.165 │ ▊                                                 │
│     0 │   19 │                 0.15 │ ▋                                                 │
│     0 │   20 │                0.135 │ ▋                                                 │
│     0 │   21 │                0.122 │ ▌                                                 │
│     0 │   22 │                0.111 │ ▌                                                 │
│     0 │   23 │                  0.1 │ ▌                                                 │
│     0 │   24 │                0.091 │ ▍                                                 │
│     1 │   25 │                1.082 │ █████▍                                            │
│     1 │   26 │                1.979 │ █████████▉                                        │
│     1 │   27 │                2.791 │ █████████████▉                                    │
│     1 │   28 │                3.525 │ █████████████████▋                                │
│     1 │   29 │                 4.19 │ ████████████████████▉                             │
│     1 │   30 │                4.791 │ ███████████████████████▉                          │
│     1 │   31 │                5.335 │ ██████████████████████████▋                       │
│     1 │   32 │                5.827 │ █████████████████████████████▏                    │
│     1 │   33 │                6.273 │ ███████████████████████████████▎                  │
│     1 │   34 │                6.676 │ █████████████████████████████████▍                │
│     1 │   35 │                7.041 │ ███████████████████████████████████▏              │
│     1 │   36 │                7.371 │ ████████████████████████████████████▊             │
│     1 │   37 │                7.669 │ ██████████████████████████████████████▎           │
│     1 │   38 │                7.939 │ ███████████████████████████████████████▋          │
│     1 │   39 │                8.184 │ ████████████████████████████████████████▉         │
│     1 │   40 │                8.405 │ ██████████████████████████████████████████        │
│     1 │   41 │                8.605 │ ███████████████████████████████████████████       │
│     1 │   42 │                8.786 │ ███████████████████████████████████████████▉      │
│     1 │   43 │                 8.95 │ ████████████████████████████████████████████▊     │
│     1 │   44 │                9.098 │ █████████████████████████████████████████████▍    │
│     1 │   45 │                9.233 │ ██████████████████████████████████████████████▏   │
│     1 │   46 │                9.354 │ ██████████████████████████████████████████████▊   │
│     1 │   47 │                9.464 │ ███████████████████████████████████████████████▎  │
│     1 │   48 │                9.563 │ ███████████████████████████████████████████████▊  │
│     1 │   49 │                9.653 │ ████████████████████████████████████████████████▎ │
└───────┴──────┴──────────────────────┴───────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category exponentialTimeDecayedSum_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn exponentialTimeDecayedSum_introduced_in = {21, 12};
    FunctionDocumentation exponentialTimeDecayedSum_documentation = {exponentialTimeDecayedSum_description, exponentialTimeDecayedSum_syntax, exponentialTimeDecayedSum_arguments, exponentialTimeDecayedSum_parameters, exponentialTimeDecayedSum_returned_value, exponentialTimeDecayedSum_examples, exponentialTimeDecayedSum_introduced_in, exponentialTimeDecayedSum_category};
    factory.registerFunction("exponentialTimeDecayedSum", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionExponentialTimeDecayedSum>(
                name, argument_types, parameters);
        }, exponentialTimeDecayedSum_documentation, properties});

    FunctionDocumentation::Description exponentialTimeDecayedMax_description = R"(
Returns the maximum of the computed exponentially smoothed moving average at index `t` in time with that at `t-1`.
    )";
    FunctionDocumentation::Syntax exponentialTimeDecayedMax_syntax = "exponentialTimeDecayedMax(x)(value, timeunit)";
    FunctionDocumentation::Arguments exponentialTimeDecayedMax_arguments = {
        {"value", "Value.", {"(U)Int*", "Float*", "Decimal"}},
        {"timeunit", "Timeunit.", {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::Parameters exponentialTimeDecayedMax_parameters = {
        {"x", "Half-life period.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue exponentialTimeDecayedMax_returned_value = {"Returns the maximum of the exponentially smoothed weighted moving average at `t` and `t-1`.", {"Float64"}};
    FunctionDocumentation::Examples exponentialTimeDecayedMax_examples = {
    {
        "Window function usage with visual representation",
        R"(
SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 5, 50) AS bar
FROM
    (
    SELECT
    (number = 0) OR (number >= 25) AS value,
    number AS time,
    exponentialTimeDecayedMax(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
    );
        )",
        R"(
┌─value─┬─time─┬─round(exp_smooth, 3)─┬─bar────────┐
│     1 │    0 │                    1 │ ██████████ │
│     0 │    1 │                0.905 │ █████████  │
│     0 │    2 │                0.819 │ ████████▏  │
│     0 │    3 │                0.741 │ ███████▍   │
│     0 │    4 │                 0.67 │ ██████▋    │
│     0 │    5 │                0.607 │ ██████     │
│     0 │    6 │                0.549 │ █████▍     │
│     0 │    7 │                0.497 │ ████▉      │
│     0 │    8 │                0.449 │ ████▍      │
│     0 │    9 │                0.407 │ ████       │
│     0 │   10 │                0.368 │ ███▋       │
│     0 │   11 │                0.333 │ ███▎       │
│     0 │   12 │                0.301 │ ███        │
│     0 │   13 │                0.273 │ ██▋        │
│     0 │   14 │                0.247 │ ██▍        │
│     0 │   15 │                0.223 │ ██▏        │
│     0 │   16 │                0.202 │ ██         │
│     0 │   17 │                0.183 │ █▊         │
│     0 │   18 │                0.165 │ █▋         │
│     0 │   19 │                 0.15 │ █▍         │
│     0 │   20 │                0.135 │ █▎         │
│     0 │   21 │                0.122 │ █▏         │
│     0 │   22 │                0.111 │ █          │
│     0 │   23 │                  0.1 │ █          │
│     0 │   24 │                0.091 │ ▉          │
│     1 │   25 │                    1 │ ██████████ │
│     1 │   26 │                    1 │ ██████████ │
│     1 │   27 │                    1 │ ██████████ │
│     1 │   28 │                    1 │ ██████████ │
│     1 │   29 │                    1 │ ██████████ │
│     1 │   30 │                    1 │ ██████████ │
│     1 │   31 │                    1 │ ██████████ │
│     1 │   32 │                    1 │ ██████████ │
│     1 │   33 │                    1 │ ██████████ │
│     1 │   34 │                    1 │ ██████████ │
│     1 │   35 │                    1 │ ██████████ │
│     1 │   36 │                    1 │ ██████████ │
│     1 │   37 │                    1 │ ██████████ │
│     1 │   38 │                    1 │ ██████████ │
│     1 │   39 │                    1 │ ██████████ │
│     1 │   40 │                    1 │ ██████████ │
│     1 │   41 │                    1 │ ██████████ │
│     1 │   42 │                    1 │ ██████████ │
│     1 │   43 │                    1 │ ██████████ │
│     1 │   44 │                    1 │ ██████████ │
│     1 │   45 │                    1 │ ██████████ │
│     1 │   46 │                    1 │ ██████████ │
│     1 │   47 │                    1 │ ██████████ │
│     1 │   48 │                    1 │ ██████████ │
│     1 │   49 │                    1 │ ██████████ │
└───────┴──────┴──────────────────────┴────────────┘
        )"
    }
    };
    FunctionDocumentation::Category exponentialTimeDecayedMax_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn exponentialTimeDecayedMax_introduced_in = {21, 12};
    FunctionDocumentation exponentialTimeDecayedMax_documentation = {exponentialTimeDecayedMax_description, exponentialTimeDecayedMax_syntax, exponentialTimeDecayedMax_arguments, exponentialTimeDecayedMax_parameters, exponentialTimeDecayedMax_returned_value, exponentialTimeDecayedMax_examples, exponentialTimeDecayedMax_introduced_in, exponentialTimeDecayedMax_category};
    factory.registerFunction("exponentialTimeDecayedMax", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionExponentialTimeDecayedMax>(
                name, argument_types, parameters);
        }, exponentialTimeDecayedMax_documentation, properties});

    FunctionDocumentation::Description exponentialTimeDecayedCount_description = R"(
Returns the cumulative exponential decay over a time series at the index `t` in time.
    )";
    FunctionDocumentation::Syntax exponentialTimeDecayedCount_syntax = "exponentialTimeDecayedCount(x)(t)";
    FunctionDocumentation::Arguments exponentialTimeDecayedCount_arguments = {
        {"t", "Time.", {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::Parameters exponentialTimeDecayedCount_parameters = {
        {"x", "Half-life period.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue exponentialTimeDecayedCount_returned_value = {"Returns the cumulative exponential decay at the given point in time.", {"Float64"}};
    FunctionDocumentation::Examples exponentialTimeDecayedCount_examples = {
    {
        "Window function usage with visual representation",
        R"(
SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 20, 50) AS bar
FROM
(
    SELECT
        (number % 5) = 0 AS value,
        number AS time,
        exponentialTimeDecayedCount(10)(time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
)
        )",
        R"(
┌─value─┬─time─┬─round(exp_smooth, 3)─┬─bar────────────────────────┐
│     1 │    0 │                    1 │ ██▌                        │
│     0 │    1 │                1.905 │ ████▊                      │
│     0 │    2 │                2.724 │ ██████▊                    │
│     0 │    3 │                3.464 │ ████████▋                  │
│     0 │    4 │                4.135 │ ██████████▎                │
│     1 │    5 │                4.741 │ ███████████▊               │
│     0 │    6 │                 5.29 │ █████████████▏             │
│     0 │    7 │                5.787 │ ██████████████▍            │
│     0 │    8 │                6.236 │ ███████████████▌           │
│     0 │    9 │                6.643 │ ████████████████▌          │
│     1 │   10 │                 7.01 │ █████████████████▌         │
│     0 │   11 │                7.343 │ ██████████████████▎        │
│     0 │   12 │                7.644 │ ███████████████████        │
│     0 │   13 │                7.917 │ ███████████████████▊       │
│     0 │   14 │                8.164 │ ████████████████████▍      │
│     1 │   15 │                8.387 │ ████████████████████▉      │
│     0 │   16 │                8.589 │ █████████████████████▍     │
│     0 │   17 │                8.771 │ █████████████████████▉     │
│     0 │   18 │                8.937 │ ██████████████████████▎    │
│     0 │   19 │                9.086 │ ██████████████████████▋    │
│     1 │   20 │                9.222 │ ███████████████████████    │
│     0 │   21 │                9.344 │ ███████████████████████▎   │
│     0 │   22 │                9.455 │ ███████████████████████▋   │
│     0 │   23 │                9.555 │ ███████████████████████▉   │
│     0 │   24 │                9.646 │ ████████████████████████   │
│     1 │   25 │                9.728 │ ████████████████████████▎  │
│     0 │   26 │                9.802 │ ████████████████████████▌  │
│     0 │   27 │                9.869 │ ████████████████████████▋  │
│     0 │   28 │                 9.93 │ ████████████████████████▊  │
│     0 │   29 │                9.985 │ ████████████████████████▉  │
│     1 │   30 │               10.035 │ █████████████████████████  │
│     0 │   31 │                10.08 │ █████████████████████████▏ │
│     0 │   32 │               10.121 │ █████████████████████████▎ │
│     0 │   33 │               10.158 │ █████████████████████████▍ │
│     0 │   34 │               10.191 │ █████████████████████████▍ │
│     1 │   35 │               10.221 │ █████████████████████████▌ │
│     0 │   36 │               10.249 │ █████████████████████████▌ │
│     0 │   37 │               10.273 │ █████████████████████████▋ │
│     0 │   38 │               10.296 │ █████████████████████████▋ │
│     0 │   39 │               10.316 │ █████████████████████████▊ │
│     1 │   40 │               10.334 │ █████████████████████████▊ │
│     0 │   41 │               10.351 │ █████████████████████████▉ │
│     0 │   42 │               10.366 │ █████████████████████████▉ │
│     0 │   43 │               10.379 │ █████████████████████████▉ │
│     0 │   44 │               10.392 │ █████████████████████████▉ │
│     1 │   45 │               10.403 │ ██████████████████████████ │
│     0 │   46 │               10.413 │ ██████████████████████████ │
│     0 │   47 │               10.422 │ ██████████████████████████ │
│     0 │   48 │                10.43 │ ██████████████████████████ │
│     0 │   49 │               10.438 │ ██████████████████████████ │
└───────┴──────┴──────────────────────┴────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category exponentialTimeDecayedCount_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn exponentialTimeDecayedCount_introduced_in = {21, 12};
    FunctionDocumentation exponentialTimeDecayedCount_documentation = {exponentialTimeDecayedCount_description, exponentialTimeDecayedCount_syntax, exponentialTimeDecayedCount_arguments, exponentialTimeDecayedCount_parameters, exponentialTimeDecayedCount_returned_value, exponentialTimeDecayedCount_examples, exponentialTimeDecayedCount_introduced_in, exponentialTimeDecayedCount_category};
    factory.registerFunction("exponentialTimeDecayedCount", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionExponentialTimeDecayedCount>(
                name, argument_types, parameters);
        }, exponentialTimeDecayedCount_documentation, properties});

    FunctionDocumentation::Description exponentialTimeDecayedAvg_description = R"(
Returns the exponentially smoothed weighted moving average of values of a time series at point `t` in time.
    )";
    FunctionDocumentation::Syntax exponentialTimeDecayedAvg_syntax = "exponentialTimeDecayedAvg(x)(v, t)";
    FunctionDocumentation::Arguments exponentialTimeDecayedAvg_arguments = {
        {"v", "Value.", {"(U)Int*", "Float*", "Decimal"}},
        {"t", "Time.", {"(U)Int*", "Float*", "Decimal", "DateTime", "DateTime64"}}
    };
    FunctionDocumentation::Parameters exponentialTimeDecayedAvg_parameters = {
        {"x", "Half-life period.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::ReturnedValue exponentialTimeDecayedAvg_returned_value = {"Returns an exponentially smoothed weighted moving average at index `t` in time.", {"Float64"}};
    FunctionDocumentation::Examples exponentialTimeDecayedAvg_examples = {
    {
        "Window function usage with visual representation",
        R"(
SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 5, 50) AS bar
FROM
    (
    SELECT
    (number = 0) OR (number >= 25) AS value,
    number AS time,
    exponentialTimeDecayedAvg(10)(value, time) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
    )
        )",
        R"(
┌─value─┬─time─┬─round(exp_smooth, 3)─┬─bar────────┐
│     1 │    0 │                    1 │ ██████████ │
│     0 │    1 │                0.475 │ ████▊      │
│     0 │    2 │                0.301 │ ███        │
│     0 │    3 │                0.214 │ ██▏        │
│     0 │    4 │                0.162 │ █▌         │
│     0 │    5 │                0.128 │ █▎         │
│     0 │    6 │                0.104 │ █          │
│     0 │    7 │                0.086 │ ▊          │
│     0 │    8 │                0.072 │ ▋          │
│     0 │    9 │                0.061 │ ▌          │
│     0 │   10 │                0.052 │ ▌          │
│     0 │   11 │                0.045 │ ▍          │
│     0 │   12 │                0.039 │ ▍          │
│     0 │   13 │                0.034 │ ▎          │
│     0 │   14 │                 0.03 │ ▎          │
│     0 │   15 │                0.027 │ ▎          │
│     0 │   16 │                0.024 │ ▏          │
│     0 │   17 │                0.021 │ ▏          │
│     0 │   18 │                0.018 │ ▏          │
│     0 │   19 │                0.016 │ ▏          │
│     0 │   20 │                0.015 │ ▏          │
│     0 │   21 │                0.013 │ ▏          │
│     0 │   22 │                0.012 │            │
│     0 │   23 │                 0.01 │            │
│     0 │   24 │                0.009 │            │
│     1 │   25 │                0.111 │ █          │
│     1 │   26 │                0.202 │ ██         │
│     1 │   27 │                0.283 │ ██▊        │
│     1 │   28 │                0.355 │ ███▌       │
│     1 │   29 │                 0.42 │ ████▏      │
│     1 │   30 │                0.477 │ ████▊      │
│     1 │   31 │                0.529 │ █████▎     │
│     1 │   32 │                0.576 │ █████▊     │
│     1 │   33 │                0.618 │ ██████▏    │
│     1 │   34 │                0.655 │ ██████▌    │
│     1 │   35 │                0.689 │ ██████▉    │
│     1 │   36 │                0.719 │ ███████▏   │
│     1 │   37 │                0.747 │ ███████▍   │
│     1 │   38 │                0.771 │ ███████▋   │
│     1 │   39 │                0.793 │ ███████▉   │
│     1 │   40 │                0.813 │ ████████▏  │
│     1 │   41 │                0.831 │ ████████▎  │
│     1 │   42 │                0.848 │ ████████▍  │
│     1 │   43 │                0.862 │ ████████▌  │
│     1 │   44 │                0.876 │ ████████▊  │
│     1 │   45 │                0.888 │ ████████▉  │
│     1 │   46 │                0.898 │ ████████▉  │
│     1 │   47 │                0.908 │ █████████  │
│     1 │   48 │                0.917 │ █████████▏ │
│     1 │   49 │                0.925 │ █████████▏ │
└───────┴──────┴──────────────────────┴────────────┘
        )"
    }
    };
    FunctionDocumentation::Category exponentialTimeDecayedAvg_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn exponentialTimeDecayedAvg_introduced_in = {21, 12};
    FunctionDocumentation exponentialTimeDecayedAvg_documentation = {exponentialTimeDecayedAvg_description, exponentialTimeDecayedAvg_syntax, exponentialTimeDecayedAvg_arguments, exponentialTimeDecayedAvg_parameters, exponentialTimeDecayedAvg_returned_value, exponentialTimeDecayedAvg_examples, exponentialTimeDecayedAvg_introduced_in, exponentialTimeDecayedAvg_category};
    factory.registerFunction("exponentialTimeDecayedAvg", {[](const std::string & name,
            const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return std::make_shared<WindowFunctionExponentialTimeDecayedAvg>(
                name, argument_types, parameters);
        }, exponentialTimeDecayedAvg_documentation, properties});
}

}
