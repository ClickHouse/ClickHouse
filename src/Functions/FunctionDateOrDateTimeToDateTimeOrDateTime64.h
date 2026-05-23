#pragma once

#include <Core/Settings.h>
#include <Functions/IFunctionDateOrDateTime.h>
#include <Interpreters/Context.h>

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
class FunctionDateOrDateTimeToDateTimeOrDateTime64 final : public IFunctionDateOrDateTime<Transform>
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

    String getSignatureString() const override
    {
        /// When extended results are enabled, Date32 and DateTime64 inputs get a DateTime64 return
        /// whose scale comes from the source (or default 3 for Date32). Other inputs return DateTime.
        if (enable_extended_results_for_datetime_functions)
        {
            return
                "(Date | DateTime) -> DateTime"
                " OR (Date | DateTime, const tz String) -> DateTime(tz)"
                " OR (T : Date32 | DateTime64) -> DateTime64(scaleOf(T))"
                " OR (T : Date32 | DateTime64, const tz String) -> DateTime64(scaleOf(T), tz)";
        }
        return
            "(DateOrDateTime) -> DateTime"
            " OR (DateOrDateTime, const tz String) -> DateTime(tz)";
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);
        if (which.isDate())
            return DateTimeTransformImpl<DataTypeDate, DataTypeDateTime, Transform>::execute(arguments, result_type, input_rows_count);
        if (which.isDate32())
        {
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDate32, DataTypeDateTime64, Transform, /*is_extended_result*/ true>::execute(
                    arguments, result_type, input_rows_count);
            return DateTimeTransformImpl<DataTypeDate32, DataTypeDateTime, Transform>::execute(arguments, result_type, input_rows_count);
        }
        if (which.isDateTime())
            return DateTimeTransformImpl<DataTypeDateTime, DataTypeDateTime, Transform>::execute(arguments, result_type, input_rows_count);
        if (which.isDateTime64())
        {
            const auto scale = static_cast<const DataTypeDateTime64 *>(from_type)->getScale();

            const TransformDateTime64<Transform> transformer(scale);
            if (enable_extended_results_for_datetime_functions)
                return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDateTime64, decltype(transformer), /*is_extended_result*/ true>::
                    execute(arguments, result_type, input_rows_count, transformer);
            return DateTimeTransformImpl<DataTypeDateTime64, DataTypeDateTime, decltype(transformer)>::execute(
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
