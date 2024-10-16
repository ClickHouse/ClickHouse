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
class FunctionDateOrDateTimeToSomething : public IFunctionDateOrDateTime<Transform>
{
public:
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateOrDateTimeToSomething>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        constexpr bool result_is_date_or_date32 = (std::is_same_v<ToDataType, DataTypeDate> || std::is_same_v<ToDataType, DataTypeDate32>);
        this->checkArguments(arguments, result_is_date_or_date32);

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
        else
            return std::make_shared<ToDataType>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        /// If result type is DateTime or DateTime64 we don't know the timezone and scale without argument types.
        if constexpr (!std::is_same_v<ToDataType, DataTypeDateTime> && !std::is_same_v<ToDataType, DataTypeDateTime64>)
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

    OptionalFieldInterval getPreimage(const IDataType & type, const Field & point) const override
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
