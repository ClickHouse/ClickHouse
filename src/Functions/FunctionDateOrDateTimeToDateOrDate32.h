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
class FunctionDateOrDateTimeToDateOrDate32 : public IFunctionDateOrDateTime<Transform>
{
private:
    const bool enable_extended_results_for_datetime_functions = false;

public:
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionDateOrDateTimeToDateOrDate32>(context_);
    }

    explicit FunctionDateOrDateTimeToDateOrDate32(ContextPtr context_)
        : enable_extended_results_for_datetime_functions(context_->getSettingsRef()[Setting::enable_extended_results_for_datetime_functions])
    {
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        this->checkArguments(arguments, /*is_result_type_date_or_date32*/ true);

        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        /// If the time zone is specified but empty, throw an exception.
        /// only validate the time_zone part if the number of arguments is 2.
        if ((which.isDateTime() || which.isDateTime64()) && arguments.size() == 2
            && extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, false).empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} supports a 2nd argument (optional) that must be a valid time zone",
                this->getName());

        if ((which.isDate32() || which.isDateTime64()) && enable_extended_results_for_datetime_functions)
            return std::make_shared<DataTypeDate32>();
        return std::make_shared<DataTypeDate>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return DateTimeTransformImpl<DataTypeDate, DataTypeDate, Transform>::execute(arguments, result_type, input_rows_count);
        if (which.isDate32())
        {
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDate32, DataTypeDate32, Transform, /*is_extended_result*/ true>::execute(
                    arguments, result_type, input_rows_count);
            return DateTimeTransformImpl<DataTypeDate32, DataTypeDate, Transform>::execute(arguments, result_type, input_rows_count);
        }
        if (which.isDateTime())
            return DateTimeTransformImpl<DataTypeDateTime, DataTypeDate, Transform>::execute(arguments, result_type, input_rows_count);
        if (which.isDateTime64())
        {
            const auto scale = static_cast<const DataTypeDateTime64 *>(from_type)->getScale();

            const TransformDateTime64<Transform> transformer(scale);
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate32, decltype(transformer), /*is_extended_result*/ true>::
                    execute(arguments, result_type, input_rows_count, transformer);
            return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDate, decltype(transformer)>::execute(
                arguments, result_type, input_rows_count, transformer);
        }
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument of function {}",
            arguments[0].type->getName(),
            this->getName());
    }

};

}
