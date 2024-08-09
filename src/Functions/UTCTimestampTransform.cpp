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
    template <typename Name>
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

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
        {
            if (arguments.size() != 2)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {}'s arguments number must be 2.", name);
            ColumnWithTypeAndName arg1 = arguments[0];
            ColumnWithTypeAndName arg2 = arguments[1];
            const auto * time_zone_const_col = checkAndGetColumnConstData<ColumnString>(arg2.column.get());
            if (!time_zone_const_col)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of 2nd argument of function {}. Excepted const(String).", arg2.column->getName(), name);
            String time_zone_val = time_zone_const_col->getDataAt(0).toString();
            auto column = result_type->createColumn();
            if (WhichDataType(arg1.type).isDateTime())
            {
                const auto * date_time_col = checkAndGetColumn<ColumnDateTime>(arg1.column.get());
                for (size_t i = 0; i < date_time_col->size(); ++i)
                {
                    UInt32 date_time_val = date_time_col->getElement(i);
                    LocalDateTime date_time(date_time_val, Name::to ? DateLUT::instance("UTC") : DateLUT::instance(time_zone_val));
                    time_t time_val = date_time.to_time_t(Name::from ? DateLUT::instance("UTC") : DateLUT::instance(time_zone_val));
                    column->insert(time_val);
                }
            }
            else if (WhichDataType(arg1.type).isDateTime64())
            {
                const auto * date_time_col = checkAndGetColumn<ColumnDateTime64>(arg1.column.get());
                const DataTypeDateTime64 * date_time_type = static_cast<const DataTypeDateTime64 *>(arg1.type.get());
                Int64 scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(date_time_type->getScale());
                for (size_t i = 0; i < date_time_col->size(); ++i)
                {
                    DateTime64 date_time_val = date_time_col->getElement(i);
                    Int64 seconds = date_time_val.value / scale_multiplier;
                    Int64 micros = date_time_val.value % scale_multiplier;
                    LocalDateTime date_time(seconds, Name::to ? DateLUT::instance("UTC") : DateLUT::instance(time_zone_val));
                    time_t time_val = date_time.to_time_t(Name::from ? DateLUT::instance("UTC") : DateLUT::instance(time_zone_val));
                    DateTime64 date_time_64(time_val * scale_multiplier + micros);
                    column->insert(date_time_64);
                }
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {}'s 1st argument can only be datetime/datatime64. ", name);
            return column;
        }

    };

    struct NameToUTCTimestamp
    {
        static constexpr auto name = "toUTCTimestamp";
        static constexpr auto from = false;
        static constexpr auto to = true;
    };

    struct NameFromUTCTimestamp
    {
        static constexpr auto name = "fromUTCTimestamp";
        static constexpr auto from = true;
        static constexpr auto to = false;
    };

    using ToUTCTimestampFunction = UTCTimestampTransform<NameToUTCTimestamp>;
    using FromUTCTimestampFunction = UTCTimestampTransform<NameFromUTCTimestamp>;
}

REGISTER_FUNCTION(UTCTimestampTransform)
{
    factory.registerFunction<ToUTCTimestampFunction>();
    factory.registerFunction<FromUTCTimestampFunction>();
    factory.registerAlias("to_utc_timestamp", NameToUTCTimestamp::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("from_utc_timestamp", NameFromUTCTimestamp::name, FunctionFactory::CaseInsensitive);
}

}
