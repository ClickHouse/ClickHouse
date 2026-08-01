#include <Functions/FunctionFactory.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesThrowDuplicateSeriesIf(condition, group) checks the `condition` and if it's true
/// throws an exception with the following message "Multiple series have the same tags <tags>,
/// duplicate series in the same result set are not allowed".
/// If the `condition` is false the function returns 0.
class FunctionTimeSeriesThrowDuplicateSeriesIf : public IFunction
{
public:
    static constexpr auto name = "timeSeriesThrowDuplicateSeriesIf";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionTimeSeriesThrowDuplicateSeriesIf>(context); }

    explicit FunctionTimeSeriesThrowDuplicateSeriesIf(ContextPtr context)
        : tags_collector(context->getQueryContext()->getTimeSeriesTagsCollector())
    {
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeUInt8>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with two arguments: {}(condition, group)",
                name, name);
        }

        if (!isNativeNumber(arguments[0].type))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a number (passed: {})",
                name, arguments[0].type->getName());
        }

        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForGroup(name, arguments, 1);
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnUInt8::create();

        if (arguments.size() != 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with two arguments: {}(condition, group)",
                name, name);
        }

        const auto & condition_column_and_type = arguments.front();
        size_t pos = findPositionOfNonZero(condition_column_and_type.column, condition_column_and_type.type);

        if (pos != static_cast<size_t>(-1))
        {
            auto group = arguments[1].column->getUInt(pos);
            auto tags = tags_collector->getTagsByGroup(group);

            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Multiple series have the same tags {}, duplicate series in the same result set are not allowed",
                ContextTimeSeriesTagsCollector::toString(tags));
        }

        return ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
    }

private:
    size_t findPositionOfNonZero(const ColumnPtr & column, const DataTypePtr & data_type) const
    {
        WhichDataType which{data_type};
        if (which.isUInt8())
            return findPositionOfNonZeroImpl<UInt8>(column, data_type);
        if (which.isUInt16())
            return findPositionOfNonZeroImpl<UInt16>(column, data_type);
        if (which.isUInt32())
            return findPositionOfNonZeroImpl<UInt32>(column, data_type);
        if (which.isUInt64())
            return findPositionOfNonZeroImpl<UInt64>(column, data_type);
        if (which.isInt8())
            return findPositionOfNonZeroImpl<Int8>(column, data_type);
        if (which.isInt16())
            return findPositionOfNonZeroImpl<Int16>(column, data_type);
        if (which.isInt32())
            return findPositionOfNonZeroImpl<Int32>(column, data_type);
        if (which.isInt64())
            return findPositionOfNonZeroImpl<Int64>(column, data_type);
        if (which.isFloat32())
            return findPositionOfNonZeroImpl<Float32>(column, data_type);
        if (which.isFloat64())
            return findPositionOfNonZeroImpl<Float64>(column, data_type);

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument of function {} must be a number (passed: {})",
            getName(), data_type->getName());
    }

    template <typename T>
    size_t findPositionOfNonZeroImpl(const ColumnPtr & column, const DataTypePtr & data_type) const
    {
        const auto * in = checkAndGetColumn<ColumnVector<T>>(column.get());

        if (!in)
            in = checkAndGetColumnConstData<ColumnVector<T>>(column.get());

        if (!in)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a number (passed: {})",
                getName(), data_type->getName());
        }

        const auto & in_data = in->getData();
        for (size_t i = 0; i != in->size(); ++i)
        {
            if (in_data[i] != 0)
                return i;
        }

        return static_cast<size_t>(-1); /// Not found.
    }

    std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector;
};


REGISTER_FUNCTION(TimeSeriesThrowDuplicateSeriesIf)
{
    FunctionDocumentation::Description description = R"(
Checks the `condition` and if it's true throws an exception with the following message
`Multiple series have the same tags <tags>, duplicate series in the same result set are not allowed`.
If the `condition` is false the function returns `0`.
This function is similar to [throwIf()](/sql-reference/functions/other-functions#throwIf),
but uses a different error code and formats the error message differently.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesThrowDuplicateSeriesIf(condition, group)";
    FunctionDocumentation::Arguments arguments = {
        {"condition",
         "Condition to check, usually contains function [count()](/sql-reference/aggregate-functions/reference/count#count)",
         {"UInt8"}},
        {"group", "Group of tags.", {"UInt64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {{
        "Example",
        R"(
CREATE TABLE test(tags Array(Tuple(String, String))) engine=Memory;

INSERT INTO test VALUES ([('__name__', 'up')]);

SELECT timeSeriesTagsToGroup(tags) AS group
FROM test
GROUP BY group
HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, group) = 0;  -- OK

INSERT INTO test VALUES ([('__name__', 'up')]);

SELECT timeSeriesTagsToGroup(tags) AS group
FROM test
GROUP BY group
HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, group) = 0;  -- Throws exception "Multiple series have the same tags {'__name__': 'up'}"
        )",
        "",
    }};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesThrowDuplicateSeriesIf>(documentation);
}
}
