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
class FunctionCustomWeekToSomething : public IFunctionCustomWeek<Transform>
{
private:
    std::string force_timezone;
public:
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionCustomWeekToSomething>(context->getSettingsRef().force_timezone); }

    explicit FunctionCustomWeekToSomething(const std::string & force_timezone_) : force_timezone(force_timezone_) {}

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        this->checkArguments(arguments);

        return std::make_shared<ToDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return CustomWeekTransformImpl<DataTypeDate, ToDataType>::execute(
                arguments, result_type, input_rows_count, Transform{}, force_timezone);
        else if (which.isDate32())
            return CustomWeekTransformImpl<DataTypeDate32, ToDataType>::execute(
                arguments, result_type, input_rows_count, Transform{}, force_timezone);
        else if (which.isDateTime())
            return CustomWeekTransformImpl<DataTypeDateTime, ToDataType>::execute(
                arguments, result_type, input_rows_count, Transform{}, force_timezone);
        else if (which.isDateTime64())
        {
            return CustomWeekTransformImpl<DataTypeDateTime64, ToDataType>::execute(
                arguments, result_type, input_rows_count,
                TransformDateTime64<Transform>{assert_cast<const DataTypeDateTime64 *>(from_type)->getScale()}, force_timezone);
        }
        else
            throw Exception(
                "Illegal type " + arguments[0].type->getName() + " of argument of function " + this->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

};

}
