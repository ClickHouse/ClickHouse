#include <Functions/FunctionSQLJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(SQLJSON)
{
    factory.registerFunction<FunctionSQLJSON<NameJSONExists, JSONExistsImpl>>();
    factory.registerFunction<FunctionSQLJSON<NameJSONQuery, JSONQueryImpl>>();
    factory.registerFunction<FunctionSQLJSON<NameJSONValue, JSONValueImpl>>();
}

}
