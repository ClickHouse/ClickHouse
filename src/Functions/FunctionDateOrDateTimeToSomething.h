#pragma once
#include <Functions/IFunctionDateOrDateTime.h>

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
            /// Timezone is optional and, when present, attached to the return type.
            return "(DateOrDateTime) -> DateTime"
                   " OR (DateOrDateTime, const tz String) -> DateTime(tz)";
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

            return "(T : DateOrDateTime) -> DateTime64(" + scale_expr + ")"
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
