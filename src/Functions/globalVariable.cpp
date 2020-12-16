#include <Functions/IFunctionImpl.h>
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


/** globalVariable('name') - takes constant string argument and returns the value of global variable with that name.
  * It is intended for compatibility with MySQL.
  *
  * Currently it's a stub, no variables are implemented. Feel free to add more variables.
  */
class FunctionGlobalVariable : public IFunction
{
public:
    static constexpr auto name = "globalVariable";
    static FunctionPtr create(const Context &)
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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!checkColumnConst<ColumnString>(arguments[0].column.get()))
            throw Exception("Argument of function " + getName() + " must be constant string", ErrorCodes::BAD_ARGUMENTS);

        String variable_name = assert_cast<const ColumnConst &>(*arguments[0].column).getValue<String>();
        auto variable = global_variable_map.find(Poco::toLower(variable_name));
        if (variable == global_variable_map.end())
            return std::make_shared<DataTypeInt32>();
        else
            return variable->second.type;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const ColumnWithTypeAndName & col = block.getByPosition(arguments[0]);
        String variable_name = assert_cast<const ColumnConst &>(*col.column).getValue<String>();
        auto variable = global_variable_map.find(Poco::toLower(variable_name));

        Field val = 0;
        if (variable != global_variable_map.end())
            val = variable->second.value;

        auto & result_col = block.getByPosition(result);
        result_col.column = result_col.type->createColumnConst(input_rows_count, val);
    }

private:
    struct TypeAndValue
    {
        DataTypePtr type;
        Field value;
    };
    std::unordered_map<String, TypeAndValue> global_variable_map = {
        {"max_allowed_packet", {std::make_shared<DataTypeInt32>(), 67108864}}, {"version", {std::make_shared<DataTypeString>(), "5.7.30"}}};
};


void registerFunctionGlobalVariable(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGlobalVariable>();
}

}

