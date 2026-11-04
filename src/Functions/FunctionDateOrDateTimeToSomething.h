#pragma once
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunctionDateOrDateTime.h>
#include <Interpreters/castColumn.h>
#include <Common/SipHash.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <Functions/compileDateTransform.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
}

/// See DateTimeTransforms.h
template <typename ToDataType, typename Transform>
class FunctionDateOrDateTimeToSomething final : public IFunctionDateOrDateTime<Transform>
{
public:
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateOrDateTimeToSomething>(); }

    String getSignatureString() const override
    {
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            /// Timezone is optional. When the 2nd argument is present it is attached to the
            /// return type; otherwise the source argument's own time zone is propagated
            /// (e.g. `toStartOfHour(x::DateTime('UTC'))` stays `DateTime('UTC')`).
            return "(T : DateOrDateTime) -> DateTime(timezoneOf(T))"
                   " OR (T : DateOrDateTime, const tz String) -> DateTime(tz)";
        }
        else if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            /// Scale is the source scale (default 3 for non-DateTime64 sources), possibly
            /// bumped up by transforms that operate at finer-than-source precision.
            String min_scale;
            if constexpr (std::is_same_v<ToStartOfMillisecondImpl, Transform>)
                min_scale = "3";
            else if constexpr (std::is_same_v<ToStartOfMicrosecondImpl, Transform>)
                min_scale = "6";
            else if constexpr (std::is_same_v<ToStartOfNanosecondImpl, Transform>)
                min_scale = "9";

            const String scale_expr = min_scale.empty()
                ? "scaleOf(T)"
                : "max(scaleOf(T), " + min_scale + ")";

            return "(T : DateOrDateTime) -> DateTime64(" + scale_expr + ", timezoneOf(T))"
                   " OR (T : DateOrDateTime, const tz String) -> DateTime64(" + scale_expr + ", tz)";
        }
        else if constexpr (std::is_same_v<ToDataType, DataTypeTime64>)
        {
            String min_scale;
            if constexpr (std::is_same_v<ToStartOfMillisecondImpl, Transform>)
                min_scale = "3";
            else if constexpr (std::is_same_v<ToStartOfMicrosecondImpl, Transform>)
                min_scale = "6";
            else if constexpr (std::is_same_v<ToStartOfNanosecondImpl, Transform>)
                min_scale = "9";

            const String scale_expr = min_scale.empty()
                ? "scaleOf(T)"
                : "max(scaleOf(T), " + min_scale + ")";

            /// DataTypeTime64 doesn't carry a timezone of its own, but the 2nd arg is still
            /// accepted. Since both branches return the same shape, an optional group works.
            return "(T : DateOrDateTime, [const tz String]) -> Time64(" + scale_expr + ")";
        }
        else
        {
            return "(DateOrDateTime, [String]) -> " + ToDataType{}.getName();
        }
    }

    /// The declarative signature above is documentation-only: the exact result type is not
    /// expressible in the DSL, so the `ColumnsWithTypeAndName` override below is authoritative.
    /// Route the types-only path to it as well, so the base
    /// `IFunction::getReturnTypeImpl(DataTypes)` never applies the documentation string (which
    /// would reject valid calls or evaluate an uncaptured return-type name).
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        ColumnsWithTypeAndName columns;
        columns.reserve(arguments.size());
        for (const auto & type : arguments)
            columns.emplace_back(nullptr, type, String{});
        return getReturnTypeImpl(columns);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        /// For Interval operands, validate the kind match here (at type
        /// analysis) rather than only in `executeImpl`, so an invalid
        /// expression like `toHour(<IntervalDay>)` is rejected before it can
        /// be captured in a view's column type. Return the raw stored value
        /// as `Int64`: the function's natural return type (e.g. `UInt8` for
        /// `toDayOfMonth`) is sized for a calendar-field range (1..31), not
        /// for arbitrary interval magnitudes or negative values, so widening
        /// to `Int64` avoids silent overflow/wrap. The DSL signature describes
        /// only the date/time argument shapes, so Interval is handled here.
        if (!arguments.empty() && isInterval(arguments[0].type))
        {
            /// The signature is bypassed in this branch, so enforce the argument count and
            /// reject a timezone argument here, as for the date/time shapes.
            constexpr bool result_is_date_or_date32 = (std::is_same_v<ToDataType, DataTypeDate> || std::is_same_v<ToDataType, DataTypeDate32>);
            this->checkArguments(arguments, result_is_date_or_date32);

            IntervalKind required;
            if (!IntervalKind::tryParseFromNameOfFunctionExtractTimePart(this->getName(), required))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} does not support Interval arguments",
                    this->getName());

            const auto & interval_type = static_cast<const DataTypeInterval &>(*arguments[0].type);
            if (interval_type.getKind() != required)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot extract {} from {}: the interval's unit does not match the requested unit",
                    required.toLowercasedKeyword(),
                    interval_type.getName());

            return std::make_shared<DataTypeInt64>();
        }

        /// Preserve the legacy rejection of an explicitly provided empty timezone for the
        /// `DateTime`-returning transforms; the DSL `DateTime(tz)` type function silently
        /// falls back to the server timezone when tz == ''. The `DateTime64` / `Time64`
        /// transforms accepted an empty 2nd argument on master, so they are left untouched.
        /// The check runs after the signature has accepted the arguments, so a 2nd argument of a
        /// wrong type is reported by the signature, not by the timezone extractor.
        auto result = IFunction::getReturnTypeImpl(arguments);
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            if (arguments.size() == 2 && extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, false).empty())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} supports a 2nd argument (optional) that must be a valid time zone", this->getName());
        }
        return result;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        /// If result type is DateTime or DateTime64 we don't know the timezone and scale without argument types.
        /// Keep this branch as the `if constexpr` so `std::make_shared<ToDataType>()` is not instantiated
        /// for types without a default constructor (e.g. DataTypeDateTime64).
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime> || std::is_same_v<ToDataType, DataTypeTime> || std::is_same_v<ToDataType, DataTypeDateTime64> || std::is_same_v<ToDataType, DataTypeTime64>)
            return nullptr;
        else
        {
            /// Extract-capable functions widen to Int64 for Interval; the narrow `ToDataType`
            /// would wrap an out-of-range interval. Int64 fits every supported result.
            if (this->acceptsIntervalArgument())
                return std::make_shared<DataTypeInt64>();
            return std::make_shared<ToDataType>();
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();

        if (isInterval(from_type))
            return executeOnInterval(arguments, result_type, input_rows_count);
        if (isDate(from_type))
            return DateTimeTransformImpl<DataTypeDate, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        if (isDate32(from_type))
            return DateTimeTransformImpl<DataTypeDate32, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        if (isTime(from_type))
            return DateTimeTransformImpl<DataTypeTime, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        if (isTime64(from_type))
        {
            const auto scale = static_cast<const DataTypeTime64 *>(from_type)->getScale();
            const TransformTime64<Transform> transformer(scale);
            return DateTimeTransformImpl<DataTypeTime64, ToDataType, decltype(transformer)>::execute(
                arguments, result_type, input_rows_count, transformer);
        }
        if (isDateTime(from_type))
            return DateTimeTransformImpl<DataTypeDateTime, ToDataType, Transform>::execute(arguments, result_type, input_rows_count);
        if (isDateTime64(from_type))
        {
            const auto scale = static_cast<const DataTypeDateTime64 *>(from_type)->getScale();
            const TransformDateTime64<Transform> transformer(scale);
            return DateTimeTransformImpl<DataTypeDateTime64, ToDataType, decltype(transformer)>::execute(
                arguments, result_type, input_rows_count, transformer);
        }
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument of function {}",
            arguments[0].type->getName(),
            this->getName());
    }

private:
    /// PostgreSQL-style `EXTRACT(<unit> FROM INTERVAL ...)`: kind matching is
    /// validated in `getReturnTypeImpl`, so here we just return the underlying
    /// Int64 values cast to the function's result type.
    ColumnPtr executeOnInterval(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
    {
        return castColumn(arguments[0], result_type);
    }

public:
    /// The result depends on the session time zone when the argument has none (`DateLUT::instance`).
    void updateHash(SipHash & hash) const override { hash.update(DateLUT::instance().getTimeZone()); }

#if USE_EMBEDDED_COMPILER
    /// The transforms whose `execute` throws for a `Date` or a `Date32`: the parts of the time of day.
    static constexpr bool throwsOnDate()
    {
        constexpr std::string_view transform_name = Transform::name;
        return transform_name == "toHour" || transform_name == "toMinute" || transform_name == "toSecond"
            || transform_name == "toMillisecond" || transform_name == "toMicrosecond" || transform_name == "toNanosecond"
            || transform_name == "timezoneOffset" || transform_name == "toTimeWithFixedDate";
    }

    /// The time zone `DateTimeTransformImpl::execute` uses with one argument: the one of a `DateTime` result, else the one
    /// of a `DateTime` argument, else the session's (`extractTimeZoneFromFunctionArguments`).
    static const DateLUTImpl & timeZoneOf(const DataTypePtr & argument_type, const DataTypePtr & result_type)
    {
        if (const auto * result_date_time = checkAndGetDataType<DataTypeDateTime>(result_type.get()))
            return result_date_time->getTimeZone();
        if (const auto * argument_date_time = checkAndGetDataType<DataTypeDateTime>(argument_type.get()))
            return argument_date_time->getTimeZone();
        return DateLUT::instance();
    }

    /// One `Date`, `Date32` or `DateTime` argument (no time zone argument); a number or a `DateTime` result. Only the
    /// transforms that read the lookup table directly (`jitDateValuesField`): a call of the transform per row is slower than
    /// the interpreted loop.
    bool isCompilableImpl(const DataTypes & types, const DataTypePtr & result_type) const override
    {
        if constexpr ((!std::is_same_v<ToDataType, DataTypeDateTime> && !IsDataTypeNumber<ToDataType>)
            || jitDateValuesField<Transform>() == JITDateValuesField::None)
            return false;
        else
        {
            const auto result_nested = removeNullable(result_type);
            if (types.size() != 1 || !canBeNativeType(*result_nested) || (!WhichDataType(result_nested).isNativeInteger() && !isDateTime(result_nested)))
                return false;
            const WhichDataType from(types[0]);
            if (from.isDateTime())
                return true;
            return (from.isDate() || from.isDate32()) && !throwsOnDate();
        }
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        if constexpr ((!std::is_same_v<ToDataType, DataTypeDateTime> && !IsDataTypeNumber<ToDataType>)
            || jitDateValuesField<Transform>() == JITDateValuesField::None)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not compilable", this->getName());
        else
        {
            using To = typename ToDataType::FieldType;
            const auto & time_zone = timeZoneOf(arguments[0].type, removeNullable(result_type));
            const WhichDataType from(arguments[0].type);
            if (from.isDateTime())
                return compileDateValuesTransform<Transform, UInt32, To>(builder, arguments[0].value, time_zone);
            if constexpr (!throwsOnDate())
            {
                if (from.isDate())
                    return compileDateValuesTransform<Transform, UInt16, To>(builder, arguments[0].value, time_zone);
                if (from.isDate32())
                    return compileDateValuesTransform<Transform, Int32, To>(builder, arguments[0].value, time_zone);
            }
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} cannot be compiled for {}", this->getName(), arguments[0].type->getName());
        }
    }
#endif

    bool hasInformationAboutPreimage() const override { return Transform::hasPreimage(); }

    FieldIntervalPtr getPreimage(const IDataType & type, const Field & point) const override
    {
        if constexpr (Transform::hasPreimage())
            return Transform::getPreimage(type, point);
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Function {} has no information about its preimage",
                Transform::name);
    }

};

}
