#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>

#include <unordered_map>
#include <Poco/String.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** globalVariable('name') - takes constant string argument and returns the value of global variable with that name.
  * It is intended for compatibility with MySQL.
  *
  * Currently it's a stub, no variables are implemented. Feel free to add more variables.
  */
class FunctionGlobalVariable : public IFunction
{
public:
    static constexpr auto name = "globalVariable";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGlobalVariable>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!checkColumnConst<ColumnString>(arguments[0].column.get()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument of function {} must be constant string", getName());

        String variable_name = assert_cast<const ColumnConst &>(*arguments[0].column).getValue<String>();
        auto variable = global_variable_map.find(Poco::toLower(variable_name));
        if (variable == global_variable_map.end())
            return std::make_shared<DataTypeInt32>();
        return variable->second.type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col = arguments[0];
        String variable_name = assert_cast<const ColumnConst &>(*col.column).getValue<String>();
        auto variable = global_variable_map.find(Poco::toLower(variable_name));

        if (variable != global_variable_map.end())
            return result_type->createColumnConst(input_rows_count, variable->second.value);
        return result_type->createColumnConstWithDefaultValue(input_rows_count);
    }

private:
    struct TypeAndValue
    {
        DataTypePtr type;
        Field value;
    };
    std::unordered_map<String, TypeAndValue> global_variable_map =
    {
        {"max_allowed_packet", {std::make_shared<DataTypeInt32>(), 67108864}},
        {"version", {std::make_shared<DataTypeString>(), "5.7.30"}},
        {"version_comment", {std::make_shared<DataTypeString>(), ""}},
        {"transaction_isolation", {std::make_shared<DataTypeString>(), "READ-UNCOMMITTED"}},
        {"session_track_system_variables", {std::make_shared<DataTypeString>(), ""}},
        {"sql_mode", {std::make_shared<DataTypeString>(), "ALLOW_INVALID_DATES,ANSI_QUOTES,IGNORE_SPACE,NO_AUTO_VALUE_ON_ZERO,NO_DIR_IN_CREATE,ONLY_FULL_GROUP_BY,PAD_CHAR_TO_FULL_LENGTH,PIPES_AS_CONCAT,REAL_AS_FLOAT"}},
    };
};

}

REGISTER_FUNCTION(GlobalVariable)
{
    FunctionDocumentation::Description description = R"(
Takes a constant string argument and returns the value of the global variable with that name. This function is intended for compatibility with MySQL and not needed or useful for normal operation of ClickHouse. Only few dummy global variables are defined.
    )";
    FunctionDocumentation::Syntax syntax = "globalVariable(name)";
    FunctionDocumentation::Arguments arguments = {
        {"name", "Global variable name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the value of variable `name`.", {"Any"}};
    FunctionDocumentation::Examples examples = {
        {"globalVariable", "SELECT globalVariable('max_allowed_packet')", "67108864"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGlobalVariable>(documentation);
}

}
