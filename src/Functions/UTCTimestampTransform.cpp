#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/LocalDateTime.h>
#include <Common/logger_useful.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/TimezoneMixin.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    class UTCTimestampTransform : public IFunction
    {
    public:
        UTCTimestampTransform(const char * name_, bool to_utc_) : function_name(name_), to_utc(to_utc_) {}

        static FunctionPtr create(ContextPtr, const char * name, bool to_utc)
        {
            return std::make_shared<UTCTimestampTransform>(name, to_utc);
        }

        String getName() const override { return function_name; }

        size_t getNumberOfArguments() const override { return 2; }

        bool useDefaultImplementationForConstants() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

        DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
        {
            FunctionArgumentDescriptors mandatory_args{
                {"datetime", &isDateTimeOrDateTime64, nullptr, "DateTime or DateTime64"},
                {"timezone", &isString, nullptr, "String"}
            };
            validateFunctionArguments(function_name, arguments, mandatory_args);

            if (dynamic_cast<const TimezoneMixin *>(arguments[0].type.get())->hasExplicitTimeZone())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument should not have explicit time zone.", function_name);

            DataTypePtr date_time_type;
            if (WhichDataType{arguments[0].type}.isDateTime())
                date_time_type = std::make_shared<DataTypeDateTime>();
            else
            {
                const DataTypeDateTime64 * date_time_64 = static_cast<const DataTypeDateTime64 *>(arguments[0].type.get());
                date_time_type = std::make_shared<DataTypeDateTime64>(date_time_64->getScale());
            }
            return date_time_type;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            if (arguments.size() != 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2.", function_name);
            const ColumnWithTypeAndName & arg1 = arguments[0];
            const ColumnWithTypeAndName & arg2 = arguments[1];
            const auto * time_zone_const_col = checkAndGetColumnConstData<ColumnString>(arg2.column.get());
            if (!time_zone_const_col)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of 2nd argument of function {}. Excepted const(String).", arg2.column->getName(), function_name);
            String time_zone_val{time_zone_const_col->getDataAt(0)};
            const DateLUTImpl & time_zone = DateLUT::instance(time_zone_val);
            if (WhichDataType(arg1.type).isDateTime())
            {
                const auto & date_time_col = checkAndGetColumn<ColumnDateTime>(*arg1.column);
                using ColVecTo = DataTypeDateTime::ColumnType;

                typename ColVecTo::MutablePtr result_column = ColVecTo::create(input_rows_count);
                typename ColVecTo::Container & result_data = result_column->getData();

                auto safe_add = [](UInt32 value, UInt32 offset) -> UInt32
                {
                    if (value > std::numeric_limits<UInt32>::max() - offset)
                        return std::numeric_limits<UInt32>::max();
                    return value + offset;
                };

                auto safe_subtract = [](UInt32 value, UInt32 offset) -> UInt32
                {
                    if (value < offset)
                        return 0;
                    return value - offset;
                };

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    UInt32 date_time_val = date_time_col.getElement(i);
                    auto time_zone_offset = time_zone.timezoneOffset(date_time_val);
                    UInt32 abs_offset = static_cast<UInt32>(std::abs(time_zone_offset));

                    if (to_utc)
                    {
                        // Convert from local time to UTC
                        // UTC = Local - Offset (for positive offsets like UTC+3)
                        // UTC = Local + |Offset| (for negative offsets like UTC-5)
                        result_data[i] = (time_zone_offset >= 0)
                            ? safe_subtract(date_time_val, abs_offset)
                            : safe_add(date_time_val, abs_offset);
                    }
                    else
                    {
                        // Convert from UTC to local time
                        // Local = UTC + Offset (for positive offsets like UTC+3)
                        // Local = UTC - |Offset| (for negative offsets like UTC-5)
                        result_data[i] = (time_zone_offset >= 0)
                            ? safe_add(date_time_val, abs_offset)
                            : safe_subtract(date_time_val, abs_offset);
                    }
                }
                return result_column;
            }
            if (WhichDataType(arg1.type).isDateTime64())
            {
                const auto & date_time_col = checkAndGetColumn<ColumnDateTime64>(*arg1.column);
                const DataTypeDateTime64 * date_time_type = static_cast<const DataTypeDateTime64 *>(arg1.type.get());
                UInt32 col_scale = date_time_type->getScale();
                Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(col_scale);
                using ColDecimalTo = DataTypeDateTime64::ColumnType;
                typename ColDecimalTo::MutablePtr result_column = ColDecimalTo::create(input_rows_count, col_scale);
                typename ColDecimalTo::Container & result_data = result_column->getData();
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    DateTime64 date_time_val = date_time_col.getElement(i);
                    Int64 seconds = date_time_val.value / scale_multiplier;
                    Int64 micros = date_time_val.value % scale_multiplier;
                    auto time_zone_offset = time_zone.timezoneOffset(seconds);
                    Int64 time_val = seconds;
                    if (to_utc)
                        time_val -= time_zone_offset;
                    else
                        time_val += time_zone_offset;
                    DateTime64 date_time_64(time_val * scale_multiplier + micros);
                    result_data[i] = date_time_64;
                }
                return result_column;
            }
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument can only be DateTime/DateTime64. ", function_name);
        }

    private:
        const char * function_name;
        bool to_utc;
    };
}

REGISTER_FUNCTION(UTCTimestampTransform)
{
    FunctionDocumentation::Description description_toUTCTimestamp = R"(
Converts a date or date with time value from one time zone to UTC timezone timestamp. This function is mainly included for compatibility with Apache Spark and similar frameworks.
    )";
    FunctionDocumentation::Syntax syntax_toUTCTimestamp = R"(
toUTCTimestamp(datetime, time_zone)
    )";
    FunctionDocumentation::Arguments arguments_toUTCTimestamp = {
        {"datetime", "A date or date with time type const value or an expression.", {"DateTime", "DateTime64"}},
        {"time_zone", "A String type const value or an expression representing the time zone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_toUTCTimestamp = {"Returns a date or date with time in UTC timezone.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples_toUTCTimestamp = {
        {"Convert timezone to UTC", R"(
SELECT toUTCTimestamp(toDateTime('2023-03-16'), 'Asia/Shanghai')
        )",
        R"(
┌─toUTCTimestamp(toDateTime('2023-03-16'), 'Asia/Shanghai')─┐
│                                     2023-03-15 16:00:00 │
└─────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_toUTCTimestamp = {23, 8};
    FunctionDocumentation::Category category_toUTCTimestamp = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_toUTCTimestamp = {description_toUTCTimestamp, syntax_toUTCTimestamp, arguments_toUTCTimestamp, {}, returned_value_toUTCTimestamp, examples_toUTCTimestamp, introduced_in_toUTCTimestamp, category_toUTCTimestamp};

    factory.registerFunction("toUTCTimestamp", [](ContextPtr){ return UTCTimestampTransform::create({}, "toUTCTimestamp", true); }, documentation_toUTCTimestamp);
    factory.registerAlias("to_utc_timestamp", "toUTCTimestamp", FunctionFactory::Case::Insensitive);

    FunctionDocumentation::Description description_fromUTCTimestamp = R"(
Converts a date or date with time value from UTC timezone to a date or date with time value with the specified time zone. This function is mainly included for compatibility with Apache Spark and similar frameworks.
    )";
    FunctionDocumentation::Syntax syntax_fromUTCTimestamp = R"(
fromUTCTimestamp(datetime, time_zone)
    )";
    FunctionDocumentation::Arguments arguments_fromUTCTimestamp = {
        {"datetime", "A date or date with time const value or an expression.", {"DateTime", "DateTime64"}},
        {"time_zone", "A String type const value or an expression representing the time zone.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_fromUTCTimestamp = {"Returns DateTime/DateTime64 in the specified timezone.", {"DateTime", "DateTime64"}};
    FunctionDocumentation::Examples examples_fromUTCTimestamp = {
        {"Convert UTC timezone to specified timezone", R"(
SELECT fromUTCTimestamp(toDateTime64('2023-03-16 10:00:00', 3), 'Asia/Shanghai')
        )",
        R"(
┌─fromUTCTimestamp(toDateTime64('2023-03-16 10:00:00',3), 'Asia/Shanghai')─┐
│                                                 2023-03-16 18:00:00.000 │
└─────────────────────────────────────────────────────────────────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_fromUTCTimestamp = {22, 1};
    FunctionDocumentation::Category category_fromUTCTimestamp = FunctionDocumentation::Category::DateAndTime;
    FunctionDocumentation documentation_fromUTCTimestamp = {description_fromUTCTimestamp, syntax_fromUTCTimestamp, arguments_fromUTCTimestamp, {}, returned_value_fromUTCTimestamp, examples_fromUTCTimestamp, introduced_in_fromUTCTimestamp, category_fromUTCTimestamp};

    factory.registerFunction("fromUTCTimestamp", [](ContextPtr){ return UTCTimestampTransform::create({}, "fromUTCTimestamp", false); }, documentation_fromUTCTimestamp);
    factory.registerAlias("from_utc_timestamp", "fromUTCTimestamp", FunctionFactory::Case::Insensitive);
}

}
