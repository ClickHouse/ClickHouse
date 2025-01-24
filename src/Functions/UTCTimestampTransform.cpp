#include <Columns/ColumnConst.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/DateLUT.h>
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
    template <typename Name, bool toUTC>
    class UTCTimestampTransform : public IFunction
    {
    public:
        static FunctionPtr create(ContextPtr) { return std::make_shared<UTCTimestampTransform>(); }
        static constexpr auto name = Name::name;

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 2; }

        bool useDefaultImplementationForConstants() const override { return true; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (arguments.size() != 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2.", name);
            WhichDataType which_type_first(arguments[0]);
            WhichDataType which_type_second(arguments[1]);
            if (!which_type_first.isDateTime() && !which_type_first.isDateTime64())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument type must be datetime.", name);
            if (dynamic_cast<const TimezoneMixin *>(arguments[0].get())->hasExplicitTimeZone())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument should not have explicit time zone.", name);
            if (!which_type_second.isString())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 2nd argument type must be string.", name);
            DataTypePtr date_time_type;
            if (which_type_first.isDateTime())
                date_time_type = std::make_shared<DataTypeDateTime>();
            else
            {
                const DataTypeDateTime64 * date_time_64 = static_cast<const DataTypeDateTime64 *>(arguments[0].get());
                date_time_type = std::make_shared<DataTypeDateTime64>(date_time_64->getScale());
            }
            return date_time_type;
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            if (arguments.size() != 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2.", name);
            const ColumnWithTypeAndName & arg1 = arguments[0];
            const ColumnWithTypeAndName & arg2 = arguments[1];
            const auto * time_zone_const_col = checkAndGetColumnConstData<ColumnString>(arg2.column.get());
            if (!time_zone_const_col)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of 2nd argument of function {}. Excepted const(String).", arg2.column->getName(), name);
            String time_zone_val = time_zone_const_col->getDataAt(0).toString();
            const DateLUTImpl & time_zone = DateLUT::instance(time_zone_val);
            if (WhichDataType(arg1.type).isDateTime())
            {
                const auto & date_time_col = checkAndGetColumn<ColumnDateTime>(*arg1.column);
                using ColVecTo = DataTypeDateTime::ColumnType;
                typename ColVecTo::MutablePtr result_column = ColVecTo::create(input_rows_count);
                typename ColVecTo::Container & result_data = result_column->getData();
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    UInt32 date_time_val = date_time_col.getElement(i);
                    auto time_zone_offset = time_zone.timezoneOffset(date_time_val);
                    if constexpr (toUTC)
                        result_data[i] = date_time_val - static_cast<UInt32>(time_zone_offset);
                    else
                        result_data[i] = date_time_val + static_cast<UInt32>(time_zone_offset);
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
                    if constexpr (toUTC)
                        time_val -= time_zone_offset;
                    else
                        time_val += time_zone_offset;
                    DateTime64 date_time_64(time_val * scale_multiplier + micros);
                    result_data[i] = date_time_64;
                }
                return result_column;
            }
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument can only be datetime/datatime64. ", name);
        }

    };

    struct NameToUTCTimestamp
    {
        static constexpr auto name = "toUTCTimestamp";
    };

    struct NameFromUTCTimestamp
    {
        static constexpr auto name = "fromUTCTimestamp";
    };

    using ToUTCTimestampFunction = UTCTimestampTransform<NameToUTCTimestamp, true>;
    using FromUTCTimestampFunction = UTCTimestampTransform<NameFromUTCTimestamp, false>;
}

REGISTER_FUNCTION(UTCTimestampTransform)
{
    factory.registerFunction<ToUTCTimestampFunction>();
    factory.registerFunction<FromUTCTimestampFunction>();
    factory.registerAlias("to_utc_timestamp", NameToUTCTimestamp::name, FunctionFactory::Case::Insensitive);
    factory.registerAlias("from_utc_timestamp", NameFromUTCTimestamp::name, FunctionFactory::Case::Insensitive);
}

}
