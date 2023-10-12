#pragma once
#include <DataTypes/DataTypeString.h>
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
public:
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionCustomWeekToSomething>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        // Adjusted from IFunctionCustomWeek::checkArguments to support String arguments,
        // but only for the functions that use FunctionCustomWeekToSomething at the moment (see toDayOfWeek.cpp).
        // If we decide to support String arguments for all the functions that use IFunctionCustomWeek,
        // makes sense to just add !isString check to the FunctionCustomWeekToSomething::checkArguments and use it instead.
        if (arguments.size() == 1)
        {
            auto t0 = arguments[0].type;
            if (!isDate(t0) && !isDate32(t0) && !isDateTime(t0) && !isDateTime64(t0) && !isString(t0))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. Must be Date, Date32, DateTime or DateTime64.",
                    arguments[0].type->getName(),
                    this->getName());
        }
        else if (arguments.size() == 2)
        {
            auto t0 = arguments[0].type;
            if (!isDate(t0) && !isDate32(t0) && !isDateTime(t0) && !isDateTime64(t0) && !isString(t0))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 1st argument of function {}. Must be Date, Date32, DateTime or DateTime64.",
                    arguments[0].type->getName(),
                    this->getName());
            if (!isUInt8(arguments[1].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 2nd (optional) argument of function {}. Must be constant UInt8 (week mode).",
                    arguments[1].type->getName(),
                    this->getName());
        }
        else if (arguments.size() == 3)
        {
            auto t0 = arguments[0].type;
            if (!isDate(t0) && !isDate32(t0) && !isDateTime(t0) && !isDateTime64(t0) && !isString(t0))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of argument of function {}. Must be Date, Date32, DateTime or DateTime64",
                    arguments[0].type->getName(),
                    this->getName());
            if (!isUInt8(arguments[1].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 2nd (optional) argument of function {}. Must be constant UInt8 (week mode).",
                    arguments[1].type->getName(),
                    this->getName());
            if (!isString(arguments[2].type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of 3rd (optional) argument of function {}. Must be constant string (timezone name).",
                    arguments[2].type->getName(),
                    this->getName());
        }
        else
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, expected 1, 2 or 3.",
                this->getName(),
                arguments.size());
        return std::make_shared<ToDataType>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return CustomWeekTransformImpl<DataTypeDate, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        else if (which.isDate32())
            return CustomWeekTransformImpl<DataTypeDate32, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        else if (which.isDateTime())
            return CustomWeekTransformImpl<DataTypeDateTime, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        else if (which.isDateTime64())
        {
            return CustomWeekTransformImpl<DataTypeDateTime64, ToDataType>::execute(
                arguments,
                result_type,
                input_rows_count,
                TransformDateTime64<Transform>{assert_cast<const DataTypeDateTime64 *>(from_type)->getScale()});
        }
        else if (which.isString())
        {
            return CustomWeekTransformImpl<DataTypeString, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[0].type->getName(), this->getName());
    }

};

}
