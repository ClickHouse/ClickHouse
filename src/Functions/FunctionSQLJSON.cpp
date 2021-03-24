#include <Functions/FunctionSQLJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


void registerFunctionsSQLJSON(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSQLJSON<NameSQLJSONTest, SQLJSONTestImpl>>();
    factory.registerFunction<FunctionSQLJSON<NameSQLJSONMemberAccess, SQLJSONMemberAccessImpl>>();
}

}
