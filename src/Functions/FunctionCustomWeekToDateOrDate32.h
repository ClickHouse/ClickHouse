#pragma once
#include <Functions/IFunctionCustomWeek.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Transform>
class FunctionCustomWeekToDateOrDate32 : public IFunctionCustomWeek<Transform>, WithContext
{
public:
    bool enable_date32_results = false;

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionCustomWeekToDateOrDate32>(context_);
    }

    explicit FunctionCustomWeekToDateOrDate32(ContextPtr context_) : WithContext(context_)
    {
        enable_date32_results = context_->getSettingsRef().enable_date32_results;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        this->checkArguments(arguments, true);

        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);
        if ((which.isDate32() || which.isDateTime64()) && enable_date32_results)
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
            if (enable_date32_results)
                return CustomWeekTransformImpl<DataTypeDate32, DataTypeDate32, true>::execute(
                    arguments, result_type, input_rows_count, Transform{});
            else
                return CustomWeekTransformImpl<DataTypeDate32, DataTypeDate>::execute(
                    arguments, result_type, input_rows_count, Transform{});
        else if (which.isDateTime())
            return CustomWeekTransformImpl<DataTypeDateTime, DataTypeDate>::execute(
                arguments, result_type, input_rows_count, Transform{});
        else if (which.isDateTime64())
        {
            if (enable_date32_results)
                return CustomWeekTransformImpl<DataTypeDateTime64, DataTypeDate32, true>::execute(
                    arguments, result_type, input_rows_count,
                    TransformDateTime64<Transform>{assert_cast<const DataTypeDateTime64 *>(from_type)->getScale()});
            else
                return CustomWeekTransformImpl<DataTypeDateTime64, DataTypeDate>::execute(
                    arguments, result_type, input_rows_count,
                    TransformDateTime64<Transform>{assert_cast<const DataTypeDateTime64 *>(from_type)->getScale()});
        }
        else
            throw Exception(
                "Illegal type " + arguments[0].type->getName() + " of argument of function " + this->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

};

}
