#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionDateTime64Diff : public IFunction
{
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;

public:
    static constexpr auto name = "dateTime64Diff";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDateTime64Diff>(std::move(context)); }

    explicit FunctionDateTime64Diff(ContextPtr context_) : context(std::move(context_)) { }
    ~FunctionDateTime64Diff() override = default;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

private:
    ContextPtr context;
};

ColumnPtr FunctionDateTime64Diff::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const size_t input_rows_count) const
{
    const auto & lhs_arg = arguments.front();
    const auto & rhs_arg = arguments.back();
    const auto * lhs_type = checkAndGetDataType<DataTypeDateTime64>(lhs_arg.type.get());
    const auto * rhs_type = checkAndGetDataType<DataTypeDateTime64>(rhs_arg.type.get());
    if (!lhs_type || !rhs_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected arguments of function {}", getName());

    const auto common_scale = std::max(lhs_type->getScale(), rhs_type->getScale());
    const auto scale_arg = createConstColumnWithTypeAndName<DataTypeUInt32>(common_scale, "scale");
    const auto convert_to_decimal = [this, &input_rows_count, &scale_arg](const ColumnWithTypeAndName & argument)
    {
        const ColumnsWithTypeAndName cast_args{argument, scale_arg};
        return executeFunctionCall(context, "toDecimal64", cast_args, input_rows_count);
    };

    const auto lhs_arg_as_decimal = convert_to_decimal(lhs_arg);
    const auto rhs_arg_as_decimal = convert_to_decimal(rhs_arg);
    const ColumnsWithTypeAndName subtraction_args{asArgument(lhs_arg_as_decimal, "lhs"), asArgument(rhs_arg_as_decimal, "rhs")};
    const auto difference = executeFunctionCall(context, "minus", subtraction_args, input_rows_count);

    const ColumnsWithTypeAndName to_decimal128_args{asArgument(difference, "difference"), scale_arg};
    const auto as_decimal128 = executeFunctionCall(context, "toDecimal128", to_decimal128_args, input_rows_count);

    const ColumnsWithTypeAndName scale_args{
        asArgument(as_decimal128, "difference"), createConstColumnWithTypeAndName<DataTypeUInt32>(1'000'000'000, "multiplier")};
    const auto scaled = executeFunctionCall(context, "multiply", scale_args, input_rows_count);

    const ColumnsWithTypeAndName to_int64_args{asArgument(scaled, "scaled")};
    const auto as_int64 = executeFunctionCall(context, "toInt64", to_int64_args, input_rows_count);

    const ColumnsWithTypeAndName cast_args{
        asArgument(as_int64, "as_int64"), createConstColumnWithTypeAndName<DataTypeString>(result_type->getName(), "target_type")};
    return executeFunctionCall(context, "cast", cast_args, input_rows_count).first;
}

DataTypePtr FunctionDateTime64Diff::getReturnTypeImpl(const DataTypes & arguments) const
{
    const auto & lhs = arguments.front();
    const auto & rhs = arguments.back();
    if (!WhichDataType(*lhs).isDateTime64())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of first argument of function {}, expected DateTime64",
            lhs->getName(),
            getName());

    if (!WhichDataType(*rhs).isDateTime64())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of second argument of function {}, expected DateTime64",
            rhs->getName(),
            getName());

    return std::make_shared<DataTypeInterval>(IntervalKind::Nanosecond);
}

REGISTER_FUNCTION(DateTime64Diff)
{
    factory.registerFunction<FunctionDateTime64Diff>();
}
}
