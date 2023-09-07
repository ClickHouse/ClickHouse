#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionAddDate : public IFunction
{
public:
    static constexpr auto name = "addDate";

    explicit FunctionAddDate(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAddDate>(context); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be a date or a date with time",
                arguments[0].type->getName(),
                getName());

        if (!isInterval(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be an interval",
                arguments[0].type->getName(),
                getName());

        auto plus = FunctionFactory::instance().get("plus", context);
        auto plus_build = plus->build(arguments);

        return plus_build->getResultType();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be a date or a date with time",
                arguments[0].type->getName(),
                getName());

        if (!isInterval(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be an interval",
                arguments[0].type->getName(),
                getName());

        auto plus = FunctionFactory::instance().get("plus", context);
        auto plus_build = plus->build(arguments);

        auto res_type = plus_build->getResultType();
        return plus_build->execute(arguments, res_type, input_rows_count);
    }

private:
    ContextPtr context;
};

}


REGISTER_FUNCTION(AddInterval)
{
    factory.registerFunction<FunctionAddDate>();
}

}
