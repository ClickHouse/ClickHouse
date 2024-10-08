#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionGetSubcolumn : public IFunction
{
public:
    static constexpr auto name = "getSubcolumn";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionGetSubcolumn>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForDynamic() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto subcolumn_name = getSubcolumnName(arguments);
        return arguments[0].type->getSubcolumnType(subcolumn_name);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto subcolumn_name = getSubcolumnName(arguments);
        return arguments[0].type->getSubcolumn(subcolumn_name, arguments[0].column);
    }

private:
    static std::string_view getSubcolumnName(const ColumnsWithTypeAndName & arguments)
    {
        const auto * column = arguments[1].column.get();
        if (!isString(arguments[1].type) || !column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The second argument of function {} should be a constant string with the name of a subcolumn", name);

        return column->getDataAt(0).toView();
    }
};

}

REGISTER_FUNCTION(GetSubcolumn)
{
    factory.registerFunction<FunctionGetSubcolumn>(FunctionDocumentation{
        .description=R"(
Receives the expression or identifier and constant string with the name of subcolumn.

Returns requested subcolumn extracted from the expression.
)",
        .examples{{"getSubcolumn", "SELECT getSubcolumn(array_col, 'size0'), getSubcolumn(tuple_col, 'elem_name')", ""}},
        .categories{"OtherFunctions"}
    });
}

}
