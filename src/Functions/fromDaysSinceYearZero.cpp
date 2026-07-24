#include <Functions/IFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/castColumn.h>
#include <base/int8_to_string.h>

#include <Common/DateLUT.h>

#include <array>
#include <cmath>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{

struct DateTraits
{
    static constexpr auto name = "fromDaysSinceYearZero";
    using ReturnDataType = DataTypeDate;
};

struct DateTraits32
{
    static constexpr auto name = "fromDaysSinceYearZero32";
    using ReturnDataType = DataTypeDate32;
};

template <typename Traits>
class FunctionFromDaysSinceYearZero : public IFunction
{
public:
    static constexpr auto name = Traits::name;
    using RawReturnType = typename Traits::ReturnDataType::FieldType;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFromDaysSinceYearZero>(); }

    String getName() const override { return name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{{"days", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), nullptr, "Integer"}};

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<typename Traits::ReturnDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto res_column = Traits::ReturnDataType::ColumnType::create(input_rows_count);
        const auto & src_column = arguments[0];

        auto try_type = [&]<typename T>(T)
        {
            using ColVecType = ColumnVector<T>;

            if (const ColVecType * col_vec = checkAndGetColumn<ColVecType>(src_column.column.get()))
            {
                execute<T>(*col_vec, *res_column, input_rows_count);
                return true;
            }
            return false;
        };

        const bool success = try_type(UInt8{}) || try_type(UInt16{}) || try_type(UInt32{}) || try_type(UInt64{})
                                || try_type(Int8{}) || try_type(Int16{}) || try_type(Int32{}) || try_type(Int64{});

        if (!success)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal column while execute function {}", getName());

        return res_column;
    }

    template <typename T, typename ColVecType, typename ResCol>
    void execute(const ColVecType & col, ResCol & result_column, size_t rows_count) const
    {
        const auto & src_data = col.getData();
        auto & dst_data = result_column.getData();
        dst_data.resize(rows_count);

        for (size_t i = 0; i < rows_count; ++i)
        {
            auto value = src_data[i];
            if (value < 0)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Expected a non-negative integer, got: {}", std::to_string(value));
            /// prevent potential signed integer overflows (aka. undefined behavior) with Date32 results
            auto value_uint64 = static_cast<UInt64>(value); /// NOLINT(bugprone-signed-char-misuse,cert-str34-c)
            dst_data[i] = static_cast<RawReturnType>(value_uint64 - ToDaysSinceYearZeroImpl::DAYS_BETWEEN_YEARS_0_AND_1970);
        }
    }
};


}

REGISTER_FUNCTION(FromDaysSinceYearZero)
{
    FunctionDocumentation::Description description_fromDaysSinceYearZero = R"(
For a given number of days elapsed since [1 January 0000](https://en.wikipedia.org/wiki/Year_zero), returns the corresponding date in the [proleptic Gregorian calendar defined by ISO 8601](https://en.wikipedia.org/wiki/Gregorian_calendar#Proleptic_Gregorian_calendar).

The calculation is the same as in MySQL's `FROM_DAYS()` function. The result is undefined if it cannot be represented within the bounds of the [Date](../data-types/date.md) type.
    )";
    FunctionDocumentation::Syntax syntax_fromDaysSinceYearZero = R"(
fromDaysSinceYearZero(days)
    )";
    FunctionDocumentation::Arguments arguments_fromDaysSinceYearZero =
    {
        {"days", "The number of days passed since year zero.", {"UInt32"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fromDaysSinceYearZero = {"Returns the date corresponding to the number of days passed since year zero.", {"Date"}};
    FunctionDocumentation::Examples examples_fromDaysSinceYearZero =
    {
        {"Convert days since year zero to dates", R"(
SELECT
fromDaysSinceYearZero(739136) AS date1,
fromDaysSinceYearZero(toDaysSinceYearZero(toDate('2023-09-08'))) AS date2
        )",
        R"(
┌──────date1─┬──────date2─┐
│ 2023-09-08 │ 2023-09-08 │
└────────────┴────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_fromDaysSinceYearZero = {23, 11};
    FunctionDocumentation::Category category_fromDaysSinceYearZero = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_fromDaysSinceYearZero = {description_fromDaysSinceYearZero, syntax_fromDaysSinceYearZero, arguments_fromDaysSinceYearZero, returned_value_fromDaysSinceYearZero, examples_fromDaysSinceYearZero, introduced_in_fromDaysSinceYearZero, category_fromDaysSinceYearZero};
    factory.registerFunction<FunctionFromDaysSinceYearZero<DateTraits>>(documentation_fromDaysSinceYearZero);

    FunctionDocumentation::Description description_fromDaysSinceYearZero32 = R"(
For a given number of days elapsed since [1 January 0000](https://en.wikipedia.org/wiki/Year_zero), returns the corresponding date in the [proleptic Gregorian calendar defined by ISO 8601](https://en.wikipedia.org/wiki/Gregorian_calendar#Proleptic_Gregorian_calendar).
The calculation is the same as in MySQL's `FROM_DAYS()` function. The result is undefined if it cannot be represented within the bounds of the [`Date32`](../data-types/date32.md) type.
    )";
    FunctionDocumentation::Syntax syntax_fromDaysSinceYearZero32 = R"(
fromDaysSinceYearZero32(days)
    )";
    FunctionDocumentation::Arguments arguments_fromDaysSinceYearZero32 =
    {
        {"days", "The number of days passed since year zero.", {"UInt32"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fromDaysSinceYearZero32 = {"Returns the date corresponding to the number of days passed since year zero.", {"Date32"}};
    FunctionDocumentation::Examples examples_fromDaysSinceYearZero32 =
    {
        {"Convert days since year zero to dates", R"(
SELECT
fromDaysSinceYearZero32(739136) AS date1,
fromDaysSinceYearZero32(toDaysSinceYearZero(toDate('2023-09-08'))) AS date2
        )",
        R"(
┌──────date1─┬──────date2─┐
│ 2023-09-08 │ 2023-09-08 │
└────────────┴────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_fromDaysSinceYearZero32 = {23, 11};
    FunctionDocumentation::Category category_fromDaysSinceYearZero32 = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_fromDaysSinceYearZero32 = {description_fromDaysSinceYearZero32, syntax_fromDaysSinceYearZero32, arguments_fromDaysSinceYearZero32, returned_value_fromDaysSinceYearZero32, examples_fromDaysSinceYearZero32, introduced_in_fromDaysSinceYearZero32, category_fromDaysSinceYearZero32};
    factory.registerFunction<FunctionFromDaysSinceYearZero<DateTraits32>>(documentation_fromDaysSinceYearZero32);

    factory.registerAlias("FROM_DAYS", FunctionFromDaysSinceYearZero<DateTraits>::name, FunctionFactory::Case::Insensitive);
}

}
