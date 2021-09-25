#include <Functions/FunctionSQLJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionsSQLJSON(FunctionFactory & factory)
{
    factory.registerFunction<FunctionSQLJSON<NameJSONExists, JSONExistsImpl>>();
    factory.registerFunction<FunctionSQLJSON<NameJSONQuery, JSONQueryImpl>>();
    factory.registerFunction<FunctionSQLJSON<NameJSONValue, JSONValueImpl>>();
}

}
