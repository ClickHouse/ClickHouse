#pragma once
#include <Functions/IFunctionDateOrDateTime.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// See DateTimeTransforms.h
template <typename ToDataType, typename Transform>
class FunctionDateOrDateTimeToSomething : public IFunctionDateOrDateTime<Transform>
{
public:
    static constexpr auto name = Transform::name;

    String getName() const override
    {
        return name;
    }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateOrDateTimeToSomething>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() == 1)
        {
            if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else if (arguments.size() == 2)
        {
            if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->getName() + " of argument of function " + getName()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (!isString(arguments[1].type))
                throw Exception(
                    "Function " + getName() + " supports 1 or 2 arguments. The 1st argument "
                          "must be of type Date or DateTime. The 2nd argument (optional) must be "
                          "a constant string with timezone name",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if ((isDate(arguments[0].type) || isDate32(arguments[0].type)) && (std::is_same_v<ToDataType, DataTypeDate> || std::is_same_v<ToDataType, DataTypeDate32>))
                throw Exception(
                    "The timezone argument of function " + getName() + " is allowed only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 1 or 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        /// For DateTime, if time zone is specified, attach it to type.
        /// If the time zone is specified but empty, throw an exception.
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            std::string time_zone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0);
            /// only validate the time_zone part if the number of arguments is 2. This is mainly
            /// to accommodate functions like toStartOfDay(today()), toStartOfDay(yesterday()) etc.
            if (arguments.size() == 2 && time_zone.empty())
                throw Exception(
                    "Function " + getName() + " supports a 2nd argument (optional) that must be non-empty and be a valid time zone",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            return std::make_shared<ToDataType>(time_zone);
        }
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            Int64 scale = DataTypeDateTime64::default_scale;
            if (const auto * dt64 =  checkAndGetDataType<DataTypeDateTime64>(arguments[0].type.get()))
                scale = dt64->getScale();
            auto source_scale = scale;

            if constexpr (std::is_same_v<ToStartOfMillisecondImpl, Transform>)
            {
                scale = std::max(source_scale, static_cast<Int64>(3));
            }
            else if constexpr (std::is_same_v<ToStartOfMicrosecondImpl, Transform>)
            {
                scale = std::max(source_scale, static_cast<Int64>(6));
            }
            else if constexpr (std::is_same_v<ToStartOfNanosecondImpl, Transform>)
            {
                scale = std::max(source_scale, static_cast<Int64>(9));
            }

            return std::make_shared<ToDataType>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
        }
        else
            return std::make_shared<ToDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return DateTimeTransformImpl<DataTypeDate, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDate32())
            return DateTimeTransformImpl<DataTypeDate32, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDateTime())
            return DateTimeTransformImpl<DataTypeDateTime, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDateTime64())
        {
            const auto scale = static_cast<const DataTypeDateTime64 *>(from_type)->getScale();

            const TransformDateTime64<Transform> transformer(scale);
            return DateTimeTransformImpl<DataTypeDateTime64, ToDataType, decltype(transformer)>::execute(arguments, result_type, input_rows_count, transformer);
        }
        else
            throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

};

}
