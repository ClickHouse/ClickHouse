#include <Functions/dateName.h>

namespace DB
{
class FunctionMonthNameImpl : public FunctionDateNameImpl
{
public:
    static constexpr auto name = "monthName";

    static constexpr auto month_str = "month";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMonthNameImpl>(); }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}",
                getName(),
                toString(arguments.size()));

        WhichDataType first_argument_type(arguments[0].type);

        if (!(first_argument_type.isDate() || first_argument_type.isDateTime() || first_argument_type.isDateTime64()))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 2 argument of function {}. Must be a date or a date with time",
                arguments[1].type->getName(),
                getName());

        return std::make_shared<DataTypeString>();
    }
    
    template <typename DataType>
    ColumnPtr transformAndExecuteType(const ColumnsWithTypeAndName &arguments, const DataTypePtr & result_type) const
    {
        auto * times = checkAndGetColumn<typename DataType::ColumnType>(arguments[0].column.get());
        if (!times)
            return nullptr;

        auto month_column = DataTypeString().createColumnConst(times->getData().size(), month_str);

        ColumnsWithTypeAndName temporary_columns
        {
            ColumnWithTypeAndName(month_column, std::make_shared<DataTypeString>(), ""),
            arguments[0]
        };

        return this->executeType<DataType>(temporary_columns, result_type);
    }

    ColumnPtr executeImpl(
            const ColumnsWithTypeAndName & arguments,
            const DataTypePtr & result_type,
            [[maybe_unused]] size_t input_rows_count) const override
    {
        ColumnPtr res;

        if (!((res = transformAndExecuteType<DataTypeDate>(arguments, result_type))
            || (res = transformAndExecuteType<DataTypeDateTime>(arguments, result_type))
            || (res = transformAndExecuteType<DataTypeDateTime64>(arguments, result_type))))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of function {], must be Date or DateTime.",
                arguments[1].column->getName(),
                getName());

        return res;
    }
};

void registerFunctionMonthName(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMonthNameImpl>(FunctionFactory::CaseInsensitive);
}

}
