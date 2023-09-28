#pragma once
#include <Functions/IFunctionCustomWeek.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Transform>
class FunctionCustomWeekToDateOrDate32 : public IFunctionCustomWeek<Transform>
{
private:
    const bool enable_extended_results_for_datetime_functions = false;

public:
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionCustomWeekToDateOrDate32>(context_);
    }

    explicit FunctionCustomWeekToDateOrDate32(ContextPtr context_)
        : enable_extended_results_for_datetime_functions(context_->getSettingsRef().enable_extended_results_for_datetime_functions)
    {
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        this->checkArguments(arguments, /*is_result_type_date_or_date32*/ true);

        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);
        if ((which.isDate32() || which.isDateTime64()) && enable_extended_results_for_datetime_functions)
            return std::make_shared<DataTypeDate32>();
        else
            return std::make_shared<DataTypeDate>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isDate())
            return CustomWeekTransformImpl<DataTypeDate, DataTypeDate>::execute(
                arguments, result_type, input_rows_count, Transform{});
        else if (which.isDate32())
        {
            if (enable_extended_results_for_datetime_functions)
                return CustomWeekTransformImpl<DataTypeDate32, DataTypeDate32, /*is_extended_result*/ true>::execute(
                    arguments, result_type, input_rows_count, Transform{});
            else
                return CustomWeekTransformImpl<DataTypeDate32, DataTypeDate>::execute(
                    arguments, result_type, input_rows_count, Transform{});
        }
        else if (which.isDateTime())
            return CustomWeekTransformImpl<DataTypeDateTime, DataTypeDate>::execute(
                arguments, result_type, input_rows_count, Transform{});
        else if (which.isDateTime64())
        {
            if (enable_extended_results_for_datetime_functions)
                return CustomWeekTransformImpl<DataTypeDateTime64, DataTypeDate32, /*is_extended_result*/ true>::execute(
                    arguments, result_type, input_rows_count,
                    TransformDateTime64<Transform>{assert_cast<const DataTypeDateTime64 *>(from_type)->getScale()});
            else
                return CustomWeekTransformImpl<DataTypeDateTime64, DataTypeDate>::execute(
                    arguments, result_type, input_rows_count,
                    TransformDateTime64<Transform>{assert_cast<const DataTypeDateTime64 *>(from_type)->getScale()});
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[0].type->getName(), this->getName());
    }

};

}
