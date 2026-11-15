#include <Columns/ColumnVector.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <base/defines.h>
#include <set>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
        extern const int INCORRECT_DATA;
    }

    class ExecutableFunctionRunningConcurrency : public IExecutableFunction
    {
    public:
        String getName() const override
        {
            return "runningConcurrency";
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            return executeForType(arguments, input_rows_count);
        }

        bool useDefaultImplementationForConstants() const override
        {
            return true;
        }

    private:
        template <typename ArgDataType>
        ColumnPtr executeTyped(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
        {
            using ColVecArg = typename ArgDataType::ColumnType;
            const ColVecArg * col_begin = checkAndGetColumn<ColVecArg>(arguments[0].column.get());
            const ColVecArg * col_end   = checkAndGetColumn<ColVecArg>(arguments[1].column.get());
            if (!col_begin || !col_end)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Constant columns are not supported at the moment");
            const typename ColVecArg::Container & vec_begin = col_begin->getData();
            const typename ColVecArg::Container & vec_end   = col_end->getData();

            using ColVecConc = ColumnVector<UInt32>;
            typename ColVecConc::MutablePtr col_concurrency = ColVecConc::create(input_rows_count);
            typename ColVecConc::Container & vec_concurrency = col_concurrency->getData();

            std::multiset<typename ArgDataType::FieldType> ongoing_until;
            auto begin_serializaion = arguments[0].type->getDefaultSerialization();
            auto end_serialization = arguments[1].type->getDefaultSerialization();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const auto begin = vec_begin[i];
                const auto end   = vec_end[i];

                if (unlikely(begin > end))
                {
                    const FormatSettings default_format{};
                    WriteBufferFromOwnString buf_begin;
                    WriteBufferFromOwnString buf_end;
                    begin_serializaion->serializeTextQuoted(*(arguments[0].column), i, buf_begin, default_format);
                    end_serialization->serializeTextQuoted(*(arguments[1].column), i, buf_end, default_format);
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect order of events: {} > {}", buf_begin.str(), buf_end.str());
                }

                ongoing_until.insert(end);

                // Erase all the elements from "ongoing_until" which
                // are less than or equal to "begin", i.e. durations
                // that have already ended. We consider "begin" to be
                // inclusive, and "end" to be exclusive.
                ongoing_until.erase(
                    ongoing_until.begin(), ongoing_until.upper_bound(begin));

                vec_concurrency[i] = static_cast<UInt32>(ongoing_until.size());
            }

            return col_concurrency;
        }

        ColumnPtr executeForType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
        {
            WhichDataType which(arguments[0].type);
            if (which.isDate())
                return executeTyped<DataTypeDate>(arguments, input_rows_count);
            if (which.isDateTime())
                return executeTyped<DataTypeDateTime>(arguments, input_rows_count);
            if (which.isDateTime64())
                return executeTyped<DataTypeDateTime64>(arguments, input_rows_count);
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments for function runningConcurrency must be Date, DateTime, or DateTime64.");
        }
    };

    class FunctionBaseRunningConcurrency : public IFunctionBase
    {
    public:
        explicit FunctionBaseRunningConcurrency(DataTypes argument_types_, DataTypePtr return_type_)
            : argument_types(std::move(argument_types_))
            , return_type(std::move(return_type_)) {}

        String getName() const override
        {
            return "runningConcurrency";
        }

        const DataTypes & getArgumentTypes() const override
        {
            return argument_types;
        }

        const DataTypePtr & getResultType() const override
        {
            return return_type;
        }

        ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
        {
            return std::make_unique<ExecutableFunctionRunningConcurrency>();
        }

        bool isStateful() const override
        {
            return true;
        }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    private:
        DataTypes argument_types;
        DataTypePtr return_type;
    };

    class RunningConcurrencyOverloadResolver : public IFunctionOverloadResolver
    {
    public:
        static constexpr auto name = "runningConcurrency";

        static FunctionOverloadResolverPtr create(ContextPtr)
        {
            return std::make_unique<RunningConcurrencyOverloadResolver>();
        }

        String getName() const override
        {
            return name;
        }

        FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
        {
            // The type of the second argument must match with that of the first one.
            if (unlikely(!arguments[1].type->equals(*(arguments[0].type))))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} must be called with two arguments having the same type.", getName());
            }

            // Validate the argument type early so that unsupported types
            // (e.g. NULL literals) are rejected before execution.
            WhichDataType which(arguments[0].type);
            if (!which.isDate() && !which.isDateTime() && !which.isDateTime64())
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments for function {} must be Date, DateTime, or DateTime64.", getName());
            }

            DataTypes argument_types = { arguments[0].type, arguments[1].type };
            return std::make_unique<FunctionBaseRunningConcurrency>(argument_types, return_type);
        }

        DataTypePtr getReturnTypeImpl(const DataTypes &) const override
        {
            return std::make_shared<DataTypeUInt32>();
        }

        size_t getNumberOfArguments() const override
        {
            return 2;
        }

        bool isInjective(const ColumnsWithTypeAndName &) const override
        {
            return false;
        }

        bool isStateful() const override
        {
            return true;
        }

        bool useDefaultImplementationForNulls() const override
        {
            return false;
        }
    };

    REGISTER_FUNCTION(RunningConcurrency)
    {
        FunctionDocumentation::Description description = R"(
Calculates the number of concurrent events.
Each event has a start time and an end time.
The start time is included in the event, while the end time is excluded.
Columns with a start time and an end time must be of the same data type.
The function calculates the total number of active (concurrent) events for each event start time.

:::tip Requirements
Events must be ordered by the start time in ascending order.
If this requirement is violated the function raises an exception.
Every data block is processed separately.
If events from different data blocks overlap then they can not be processed correctly.
:::

:::warning Deprecated
It is advised to use [window functions](/sql-reference/window-functions) instead.
:::
)";
        FunctionDocumentation::Syntax syntax = "runningConcurrency(start, end)";
        FunctionDocumentation::Arguments arguments = {
            {"start", "A column with the start time of events.", {"Date", "DateTime", "DateTime64"}},
            {"end", "A column with the end time of events.", {"Date", "DateTime", "DateTime64"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of concurrent events at each event start time.", {"UInt32"}};
        FunctionDocumentation::Examples examples = {
        {
            "Usage example",
            R"(
SELECT start, runningConcurrency(start, end) FROM example_table;
            )",
            R"(
┌──────start─┬─runningConcurrency(start, end)─┐
│ 2025-03-03 │                              1 │
│ 2025-03-06 │                              2 │
│ 2025-03-07 │                              3 │
│ 2025-03-11 │                              2 │
└────────────┴────────────────────────────────┘
            )"
        }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {21, 3};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

        factory.registerFunction<RunningConcurrencyOverloadResolver>(documentation);
    }
}
