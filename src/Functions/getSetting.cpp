#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/FieldToDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/Context.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

/// Get the value of a setting.
class FunctionGetSetting : public IFunction
{
public:
    static constexpr auto name = "getSetting";

    static FunctionPtr create(const Context & context_) { return std::make_shared<FunctionGetSetting>(context_); }
    explicit FunctionGetSetting(const Context & context_) : context(context_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    size_t getNumberOfArguments() const override { return 1; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isString(arguments[0].type))
            throw Exception{"The argument of function " + String{name} + " should be a constant string with the name of a setting",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        const auto * column = arguments[0].column.get();
        if (!column || !checkAndGetColumnConstStringOrFixedString(column))
            throw Exception{"The argument of function " + String{name} + " should be a constant string with the name of a setting",
                            ErrorCodes::ILLEGAL_COLUMN};

        std::string_view setting_name{column->getDataAt(0)};
        value = context.getSettingsRef().get(setting_name);

        DataTypePtr type = applyVisitor(FieldToDataType{}, value);
        value = convertFieldToType(value, *type);
        return type;
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t result, size_t input_rows_count) const override
    {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(input_rows_count, value);
    }

private:
    mutable Field value;
    const Context & context;
};


void registerFunctionGetSetting(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGetSetting>();
}

}
