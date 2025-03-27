#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
template <typename Op>
class FunctionOpDate : public IFunction
{
public:
    static constexpr auto name = Op::name;

    explicit FunctionOpDate(ContextPtr context_) : context(context_) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionOpDate<Op>>(context); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateOrDate32OrDateTimeOrDateTime64(arguments[0].type) && !isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be a date, a date with time or a string",
                arguments[0].type->getName(),
                getName());

        if (!isInterval(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 2nd argument of function {}. Should be an interval",
                arguments[1].type->getName(),
                getName());

        auto op = FunctionFactory::instance().get(Op::internal_name, context);
        auto op_build = op->build(arguments);

        return op_build->getResultType();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!isDateOrDate32OrDateTimeOrDateTime64(arguments[0].type) && !isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 1st argument of function {}. Should be a date or a date with time",
                arguments[0].type->getName(),
                getName());

        if (!isInterval(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of 2nd argument of function {}. Should be an interval",
                arguments[1].type->getName(),
                getName());

        auto op = FunctionFactory::instance().get(Op::internal_name, context);
        auto op_build = op->build(arguments);

        auto res_type = op_build->getResultType();
        return op_build->execute(arguments, res_type, input_rows_count);
    }

private:
    ContextPtr context;
};

}

struct AddDate
{
    static constexpr auto name = "addDate";
    static constexpr auto internal_name = "plus";
};

struct SubDate
{
    static constexpr auto name = "subDate";
    static constexpr auto internal_name = "minus";
};

using FunctionAddDate = FunctionOpDate<AddDate>;
using FunctionSubDate = FunctionOpDate<SubDate>;

REGISTER_FUNCTION(AddInterval)
{
    factory.registerFunction<FunctionAddDate>({}, FunctionFactory::Case::Insensitive);
    factory.registerFunction<FunctionSubDate>({}, FunctionFactory::Case::Insensitive);
}

}
