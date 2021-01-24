#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class FunctionSign : public IFunction
{
private:
    const Context & context;

public:
    static constexpr auto name = "sign";

    explicit FunctionSign(const Context & context_) : context(context_) { }
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionSign>(context); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isNumber(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} should be a number, got {}",
                getName(),
                arguments[0].type->getName());

        ColumnWithTypeAndName compare_elem{arguments[0].column, arguments[0].type, {}};

        auto greater = FunctionFactory::instance().get("greater", context);
        auto greater_compare = greater->build(ColumnsWithTypeAndName{compare_elem, compare_elem});

        if (isUnsignedInteger(arguments[0].type.get()))
        {
            return greater_compare->getResultType();
        }

        auto compare_type = greater_compare->getResultType();

        ColumnWithTypeAndName minus_elem = {compare_type, {}};

        auto minus = FunctionFactory::instance().get("minus", context);
        auto elem_minus = minus->build(ColumnsWithTypeAndName{minus_elem, minus_elem});

        return elem_minus->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto zero_column = arguments[0].type->createColumnConstWithDefaultValue(input_rows_count);

        ColumnWithTypeAndName left{arguments[0].column, arguments[0].type, {}};
        ColumnWithTypeAndName right{zero_column, arguments[0].type, {}};

        auto func_arg = ColumnsWithTypeAndName{left, right};

        auto greater = FunctionFactory::instance().get("greater", context);
        auto greater_compare = greater->build(func_arg);

        /// Unsigned number: sign(n) = greater(n, 0)
        if (isUnsignedInteger(arguments[0].type.get()))
        {
            return greater_compare->execute(func_arg, greater_compare->getResultType(), input_rows_count);
        }

        /// Signed number: sign(n) = minus(greater(n, 0), less(n, 0))
        auto less = FunctionFactory::instance().get("less", context);
        auto less_compare = less->build(func_arg);

        ColumnsWithTypeAndName columns(2);

        columns[0].type = greater_compare->getResultType();
        columns[0].column = greater_compare->execute(func_arg, greater_compare->getResultType(), input_rows_count);
        columns[1].type = less_compare->getResultType();
        columns[1].column = less_compare->execute(func_arg, less_compare->getResultType(), input_rows_count);

        auto minus = FunctionFactory::instance().get("minus", context);
        auto elem_minus = minus->build(columns);

        return elem_minus->execute(columns, elem_minus->getResultType(), input_rows_count);
    }
};

void registerFunctionSign(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSign>();
}
}
