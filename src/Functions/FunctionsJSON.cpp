#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

void registerFunctionsJSON(FunctionFactory & factory)
{
    factory.registerFunction<FunctionJSON<NameJSONHas, JSONHasImpl>>();
    factory.registerFunction<FunctionJSON<NameIsValidJSON, IsValidJSONImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONLength, JSONLengthImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONKey, JSONKeyImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONType, JSONTypeImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractInt, JSONExtractInt64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractUInt, JSONExtractUInt64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractFloat, JSONExtractFloat64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractBool, JSONExtractBoolImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractString, JSONExtractStringImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtract, JSONExtractImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValues, JSONExtractKeysAndValuesImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractRaw, JSONExtractRawImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractArrayRaw, JSONExtractArrayRawImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValuesRaw, JSONExtractKeysAndValuesRawImpl>>();
}

}
