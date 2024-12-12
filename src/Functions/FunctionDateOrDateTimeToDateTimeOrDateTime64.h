#pragma once

#include <Core/Settings.h>
#include <Functions/IFunctionDateOrDateTime.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_extended_results_for_datetime_functions;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Transform>
class FunctionDateOrDateTimeToDateTimeOrDateTime64 : public IFunctionDateOrDateTime<Transform>
{
private:
    const bool enable_extended_results_for_datetime_functions = false;

public:
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionDateOrDateTimeToDateTimeOrDateTime64>(context_);
    }

    explicit FunctionDateOrDateTimeToDateTimeOrDateTime64(ContextPtr context_)
        : enable_extended_results_for_datetime_functions(context_->getSettingsRef()[Setting::enable_extended_results_for_datetime_functions])
    {
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        this->checkArguments(arguments, /*is_result_type_date_or_date32*/ false);

        const IDataType * from_type = arguments[0].type.get();

        WhichDataType which(from_type);

        std::string time_zone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, false);

        /// If the time zone is specified but empty, throw an exception.
        /// only validate the time_zone part if the number of arguments is 2.
        if (arguments.size() == 2 && time_zone.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} supports a 2nd argument (optional) that must be a valid time zone",
                this->getName());

        if ((which.isDate32() || which.isDateTime64()) && enable_extended_results_for_datetime_functions)
        {
            Int64 scale = DataTypeDateTime64::default_scale;
            if (which.isDateTime64())
            {
                if (const auto * dt64 =  checkAndGetDataType<DataTypeDateTime64>(arguments[0].type.get()))
                    scale = dt64->getScale();
            }
            return std::make_shared<DataTypeDateTime64>(scale, time_zone);
        }
        else
            return std::make_shared<DataTypeDateTime>(time_zone);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);
        if (which.isDate())
            return DateTimeTransformImpl<DataTypeDate, DataTypeDateTime, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDate32())
        {
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDate32, DataTypeDateTime64, Transform, /*is_extended_result*/ true>::execute(arguments, result_type, input_rows_count);
            else
                return DateTimeTransformImpl<DataTypeDate32, DataTypeDateTime, Transform>::execute(arguments, result_type, input_rows_count);
        }
        else if (which.isDateTime())
            return DateTimeTransformImpl<DataTypeDateTime, DataTypeDateTime, Transform>::execute(arguments, result_type, input_rows_count);
        else if (which.isDateTime64())
        {
            const auto scale = static_cast<const DataTypeDateTime64 *>(from_type)->getScale();

            const TransformDateTime64<Transform> transformer(scale);
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDateTime64, decltype(transformer), /*is_extended_result*/ true>::execute(arguments, result_type, input_rows_count, transformer);
            else
                return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDateTime, decltype(transformer)>::execute(arguments, result_type, input_rows_count, transformer);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}",
                    arguments[0].type->getName(), this->getName());
    }

};

}
