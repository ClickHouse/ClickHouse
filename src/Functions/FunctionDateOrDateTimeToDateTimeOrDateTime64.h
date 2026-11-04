#pragma once

#include <Core/Settings.h>
#include <Functions/IFunctionDateOrDateTime.h>
#include <Interpreters/Context.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <Functions/compileDateTransform.h>
#endif

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
                "(T : Date | DateTime) -> DateTime(timezoneOf(T))"
                " OR (Date | DateTime, const tz String) -> DateTime(tz)"
                " OR (T : Date32 | DateTime64) -> DateTime64(scaleOf(T), timezoneOf(T))"
                " OR (T : Date32 | DateTime64, const tz String) -> DateTime64(scaleOf(T), tz)";
        }
        return
            "(T : DateOrDateTime) -> DateTime(timezoneOf(T))"
            " OR (DateOrDateTime, const tz String) -> DateTime(tz)";
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        /// Preserve the legacy rejection of an explicitly provided empty timezone; the DSL
        /// `DateTime(tz)` / `DateTime64(scale, tz)` type functions silently fall back to the
        /// server timezone when tz == ''.
        /// The check runs after the signature has accepted the arguments, so a 2nd argument of a
        /// wrong type is reported by the signature, not by the timezone extractor.
        auto result = IFunction::getReturnTypeImpl(arguments);
        if (arguments.size() == 2 && extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, false).empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} supports a 2nd argument (optional) that must be a valid time zone",
                this->getName());
        return result;
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

    /// The result depends on the session time zone when the argument has none.
    void updateHash(SipHash & hash) const override
    {
        hash.update(enable_extended_results_for_datetime_functions);
        hash.update(DateLUT::instance().getTimeZone());
    }

#if USE_EMBEDDED_COMPILER
    /// `toStartOfDay`, `toStartOfHour` and the minute intervals of one `DateTime` (`toStartOfDay` also of a `Date`), with
    /// the time zone of the `DateTime` result, as `DateTimeTransformImpl::execute`: see `compileStartOfTransform`.
    bool isCompilableImpl(const DataTypes & types, const DataTypePtr & result_type) const override
    {
        if constexpr (jitStartOfDivisor<Transform>() < 0)
            return false;
        else
        {
            if (types.size() != 1 || !isDateTime(removeNullable(result_type)))
                return false;
            const WhichDataType from(types[0]);
            return from.isDateTime() || (jitStartOfDivisor<Transform>() == 0 && from.isDate());
        }
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        if constexpr (jitStartOfDivisor<Transform>() < 0)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} is not compilable", this->getName());
        else
        {
            const auto & time_zone = assert_cast<const DataTypeDateTime &>(*removeNullable(result_type)).getTimeZone();
            if (WhichDataType(arguments[0].type).isDateTime())
                return compileStartOfTransform<Transform, UInt32>(builder, arguments[0].value, time_zone);
            if constexpr (jitStartOfDivisor<Transform>() == 0)
                return compileStartOfTransform<Transform, UInt16>(builder, arguments[0].value, time_zone);
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} cannot be compiled for {}", this->getName(), arguments[0].type->getName());
        }
    }
#endif
};

}
