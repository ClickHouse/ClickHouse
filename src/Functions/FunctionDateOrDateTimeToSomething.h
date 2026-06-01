#pragma once
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunctionDateOrDateTime.h>
#include <Interpreters/castColumn.h>

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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        constexpr bool result_is_date_or_date32 = (std::is_same_v<ToDataType, DataTypeDate> || std::is_same_v<ToDataType, DataTypeDate32>);
        this->checkArguments(arguments, result_is_date_or_date32);

        /// For Interval operands, validate the kind match here (at type
        /// analysis) rather than only in `executeImpl`, so an invalid
        /// expression like `toHour(<IntervalDay>)` is rejected before it can
        /// be captured in a view's column type. Return the raw stored value
        /// as `Int64`: the function's natural return type (e.g. `UInt8` for
        /// `toDayOfMonth`) is sized for a calendar-field range (1..31), not
        /// for arbitrary interval magnitudes or negative values, so widening
        /// to `Int64` avoids silent overflow/wrap.
        if (isInterval(arguments[0].type))
        {
            IntervalKind::Kind required = IntervalKind::Kind::Second;
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
                    IntervalKind(required).toLowercasedKeyword(),
                    interval_type.getName());

            return std::make_shared<DataTypeInt64>();
        }

        /// For DateTime results, if time zone is specified, attach it to type.
        /// If the time zone is specified but empty, throw an exception.
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            std::string time_zone = extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, false);
            /// only validate the time_zone part if the number of arguments is 2. This is mainly
            /// to accommodate functions like toStartOfDay(today()), toStartOfDay(yesterday()) etc.
            if (arguments.size() == 2 && time_zone.empty())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} supports a 2nd argument (optional) that must be a valid time zone",
                    this->getName());
            return std::make_shared<ToDataType>(time_zone);
        }

        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime64>)
        {
            Int64 scale = DataTypeDateTime64::default_scale;
            if (const auto * dt64 =  checkAndGetDataType<DataTypeDateTime64>(arguments[0].type.get()))
                scale = dt64->getScale();
            auto source_scale = scale;

            if constexpr (std::is_same_v<ToStartOfMillisecondImpl, Transform>)
                scale = std::max(source_scale, static_cast<Int64>(3));
            else if constexpr (std::is_same_v<ToStartOfMicrosecondImpl, Transform>)
                scale = std::max(source_scale, static_cast<Int64>(6));
            else if constexpr (std::is_same_v<ToStartOfNanosecondImpl, Transform>)
                scale = std::max(source_scale, static_cast<Int64>(9));

            return std::make_shared<ToDataType>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, false));
        }
        else if constexpr (std::is_same_v<ToDataType, DataTypeTime64>)
        {
            Int64 scale = DataTypeTime64::default_scale;
            if (const auto * dt64 =  checkAndGetDataType<DataTypeTime64>(arguments[0].type.get()))
                scale = dt64->getScale();
            auto source_scale = scale;

            if constexpr (std::is_same_v<ToStartOfMillisecondImpl, Transform>)
                scale = std::max(source_scale, static_cast<Int64>(3));
            else if constexpr (std::is_same_v<ToStartOfMicrosecondImpl, Transform>)
                scale = std::max(source_scale, static_cast<Int64>(6));
            else if constexpr (std::is_same_v<ToStartOfNanosecondImpl, Transform>)
                scale = std::max(source_scale, static_cast<Int64>(9));

            return std::make_shared<ToDataType>(scale, extractTimeZoneNameFromFunctionArguments(arguments, 1, 0, false));
        }
        else
            return std::make_shared<ToDataType>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        /// If result type is DateTime or DateTime64 we don't know the timezone and scale without argument types.
        if constexpr (!std::is_same_v<ToDataType, DataTypeDateTime> && !std::is_same_v<ToDataType, DataTypeTime> && !std::is_same_v<ToDataType, DataTypeDateTime64> && !std::is_same_v<ToDataType, DataTypeTime64>)
            return std::make_shared<ToDataType>();
        return nullptr;
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
