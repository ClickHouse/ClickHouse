#pragma once
#include <Functions/IFunctionCustomWeek.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// See CustomWeekTransforms.h
template <typename ToDataType, typename Transform>
class FunctionCustomWeekToSomething final : public IFunctionCustomWeek<Transform>
{
public:
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCustomWeekToSomething>(); }

    String getSignatureString() const override
    {
        const String to = ToDataType{}.getName();
        const String input_matcher = Transform::value_may_be_string
            ? "Date | Date32 | DateTime | DateTime64 | String"
            : "Date | Date32 | DateTime | DateTime64";
        return "(" + input_matcher + ", [UInt8], [String]) -> " + to;
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<ToDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return CustomWeekTransformImpl<DataTypeDate, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        if (which.isDate32())
            return CustomWeekTransformImpl<DataTypeDate32, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        if (which.isDateTime())
            return CustomWeekTransformImpl<DataTypeDateTime, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        if (which.isDateTime64())
            return CustomWeekTransformImpl<DataTypeDateTime64, ToDataType>::execute(
                arguments,
                result_type,
                input_rows_count,
                TransformDateTime64<Transform>{assert_cast<const DataTypeDateTime64 *>(from_type)->getScale()});
        if (Transform::value_may_be_string && which.isString())
            return CustomWeekTransformImpl<DataTypeString, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument of function {}",
            arguments[0].type->getName(),
            this->getName());
    }

};

}
