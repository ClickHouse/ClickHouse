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
    ContextPtr context;

public:
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionCustomWeekToSomething>(context_); }

    explicit FunctionCustomWeekToSomething(ContextPtr & context_) : context(context_) {}

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        this->checkArguments(arguments, /*is_result_type_date_or_date32*/ false, Transform::value_may_be_string);
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
            return CustomWeekTransformImpl<DataTypeDateTime64, ToDataType>::execute(arguments, result_type, input_rows_count,
                TransformDateTime64<Transform>{assert_cast<const DataTypeDateTime64 *>(from_type)->getScale()});
        else if (Transform::value_may_be_string && which.isString())
            return CustomWeekTransformImpl<DataTypeString, ToDataType>::execute(arguments, result_type, input_rows_count, Transform{});
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}",
                arguments[0].type->getName(), this->getName());
    }

    std::pair<Field, Field> convertStringToUnixTimestamp(const Field & left, const Field & right) const override
    {
        ColumnsWithTypeAndName temp_block;
        auto src_type = std::make_shared<DataTypeString>();

        auto column = src_type->createColumn();
        column->insert(left);
        column->insert(right);
        temp_block.emplace_back(ColumnWithTypeAndName{std::move(column), src_type,"tmp"});

        auto to_datetime = FunctionFactory::instance().get("toUnixTimestamp", context);
        auto res_column = to_datetime->build(temp_block)->execute(
            temp_block, std::make_shared<DataTypeUInt32>(), 2U);

        return { (*res_column)[0U], (*res_column)[1U] };
    }

};

}
