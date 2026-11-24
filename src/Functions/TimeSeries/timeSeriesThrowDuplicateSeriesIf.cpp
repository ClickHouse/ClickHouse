#include <Functions/FunctionFactory.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Function timeSeriesThrowDuplicateSeriesIf(condition, tags) checks the `condition` and if it's true
/// throws an exception with the following message "Multiple series have the same tags <tags>,
/// duplicate series in the same result set are not allowed".
/// If the `condition` is false the function returns 0.
class FunctionTimeSeriesThrowDuplicateSeriesIf final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesThrowDuplicateSeriesIf";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesThrowDuplicateSeriesIf>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 2; }

    /// Constant arguments are passed as they are, the result is never constant.
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return false; }

    /// Must not throw for the default values of a LowCardinality dictionary or for the rows behind a NULL.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return false; }

    /// Evaluating at analysis time could throw for rows skipped by short-circuit evaluation.
    [[nodiscard]] bool isSuitableForConstantFolding() const override { return false; }

    /// The check must see all the rows of a result set: a filter moved below it (e.g. a join runtime filter) would hide duplicates.
    [[nodiscard]] bool isStateful() const override { return true; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(NativeNumber, Array(Tuple(String, String))) -> UInt8";
    }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeUInt8>();
    }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with two arguments: {}(condition, tags)",
                name, name);
        }

        if (!isNativeNumber(arguments[0].type))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument of function {} must be a number (passed: {})",
                name, arguments[0].type->getName());
        }

        TimeSeriesTagsFunctionHelpers::checkArgumentTypeForTags(name, arguments, 1);
    }

    [[nodiscard]] ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnUInt8::create();

        if (arguments.size() != 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with two arguments: {}(condition, tags)",
                name, name);
        }

        const auto & condition_column_and_type = arguments.front();
        size_t pos = findPositionOfNonZero(condition_column_and_type.column, condition_column_and_type.type);

        if (pos != static_cast<size_t>(-1))
        {
            auto tags_vector = TimeSeriesTagsFunctionHelpers::extractTagsFromArgument(name, arguments, 1);
            chassert(pos < tags_vector.size());

            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Multiple series have the same tags {}, duplicate series in the same result set are not allowed",
                ContextTimeSeriesTagsCollector::toString(tags_vector[pos]));
        }

        return ColumnUInt8::create(input_rows_count, static_cast<UInt8>(0));
    }

private:
    [[nodiscard]] size_t findPositionOfNonZero(const ColumnPtr & column, const DataTypePtr & data_type) const
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
    [[nodiscard]] size_t findPositionOfNonZeroImpl(const ColumnPtr & column, const DataTypePtr & data_type) const
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
        if (memoryIsZero(in_data.data(), 0, in_data.size() * sizeof(in_data[0])))
            return static_cast<size_t>(-1); /// Not found.

        for (size_t i = 0; i != in->size(); ++i)
        {
            if (in_data[i] != 0)
                return i;
        }

        return static_cast<size_t>(-1); /// Not found.
    }
};


REGISTER_FUNCTION(TimeSeriesThrowDuplicateSeriesIf)
{
    FunctionDocumentation::Description description = R"(
Checks the `condition` and if it's true throws an exception with the following message
`Multiple series have the same tags <tags>, duplicate series in the same result set are not allowed`.
If the `condition` is false the function returns `0`.
This function is similar to [throwIf()](/reference/functions/regular-functions/other-functions#throwIf),
but uses a different error code and formats the error message differently.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesThrowDuplicateSeriesIf(condition, tags)";
    FunctionDocumentation::Arguments arguments = {
        {"condition",
         "Condition to check, usually contains function [count()](/reference/functions/aggregate-functions/count#count)",
         {"UInt8"}},
        {"tags", "A set of tags.", {"Array(Tuple(String, String))"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "One series per set of tags",
        R"(
CREATE TABLE test(tags Array(Tuple(String, String))) engine=Memory;

INSERT INTO test VALUES ([('__name__', 'up')]);

SELECT timeSeriesThrowDuplicateSeriesIf(count() > 1, tags)
FROM test
GROUP BY tags;
        )",
        "0",
    },
    {
        "Two series with the same tags",
        R"(
INSERT INTO test VALUES ([('__name__', 'up')]);

SELECT timeSeriesThrowDuplicateSeriesIf(count() > 1, tags)
FROM test
GROUP BY tags;
        )",
        R"(
Received exception:
Code: 768. DB::Exception: Multiple series have the same tags {'__name__': 'up'}, duplicate series in the same result set are not allowed. (DUPLICATE_TIME_SERIES)
        )",
    }};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesThrowDuplicateSeriesIf>(documentation);
}
}
