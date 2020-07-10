#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>


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
            throw Exception("Agrument of function " + getName() + " must be constant string", ErrorCodes::BAD_ARGUMENTS);

        String variable_name = assert_cast<const ColumnConst &>(*arguments[0].column).getValue<String>();

        throw Exception("There is no global variable with name " + variable_name, ErrorCodes::BAD_ARGUMENTS);
    }

    void executeImpl(Block & block, const ColumnNumbers &, size_t /*result*/, size_t /*input_rows_count*/) override
    {
        String variable_name = assert_cast<const ColumnConst &>(*block.getByPosition(0).column).getValue<String>();

        throw Exception("There is no global variable with name " + variable_name, ErrorCodes::BAD_ARGUMENTS);
    }
};


void registerFunctionGlobalVariable(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGlobalVariable>();
}

}

