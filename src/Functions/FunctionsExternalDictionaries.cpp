#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>


namespace DB
{

void registerFunctionsExternalDictionaries(FunctionFactory & factory)
{
    factory.registerFunction<FunctionDictHas>();
    factory.registerFunction<FunctionDictGetUInt8>();
    factory.registerFunction<FunctionDictGetUInt16>();
    factory.registerFunction<FunctionDictGetUInt32>();
    factory.registerFunction<FunctionDictGetUInt64>();
    factory.registerFunction<FunctionDictGetInt8>();
    factory.registerFunction<FunctionDictGetInt16>();
    factory.registerFunction<FunctionDictGetInt32>();
    factory.registerFunction<FunctionDictGetInt64>();
    factory.registerFunction<FunctionDictGetFloat32>();
    factory.registerFunction<FunctionDictGetFloat64>();
    factory.registerFunction<FunctionDictGetDate>();
    factory.registerFunction<FunctionDictGetDateTime>();
    factory.registerFunction<FunctionDictGetUUID>();
    factory.registerFunction<FunctionDictGetString>();
    factory.registerFunction<FunctionDictGetHierarchy>();
    factory.registerFunction<FunctionDictIsIn>();
    factory.registerFunction<FunctionDictGetUInt8OrDefault>();
    factory.registerFunction<FunctionDictGetUInt16OrDefault>();
    factory.registerFunction<FunctionDictGetUInt32OrDefault>();
    factory.registerFunction<FunctionDictGetUInt64OrDefault>();
    factory.registerFunction<FunctionDictGetInt8OrDefault>();
    factory.registerFunction<FunctionDictGetInt16OrDefault>();
    factory.registerFunction<FunctionDictGetInt32OrDefault>();
    factory.registerFunction<FunctionDictGetInt64OrDefault>();
    factory.registerFunction<FunctionDictGetFloat32OrDefault>();
    factory.registerFunction<FunctionDictGetFloat64OrDefault>();
    factory.registerFunction<FunctionDictGetDateOrDefault>();
    factory.registerFunction<FunctionDictGetDateTimeOrDefault>();
    factory.registerFunction<FunctionDictGetUUIDOrDefault>();
    factory.registerFunction<FunctionDictGetStringOrDefault>();
    factory.registerFunction<FunctionDictGetNoType>();
    factory.registerFunction<FunctionDictGetNoTypeOrDefault>();
}

}
