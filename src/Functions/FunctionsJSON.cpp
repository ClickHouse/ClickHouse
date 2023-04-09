#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

REGISTER_FUNCTION(JSON)
{
    factory.registerFunction<JSONOverloadResolver<NameJSONHas, JSONHasImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameIsValidJSON, IsValidJSONImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONLength, JSONLengthImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONKey, JSONKeyImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONType, JSONTypeImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractInt, JSONExtractInt64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractUInt, JSONExtractUInt64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractFloat, JSONExtractFloat64Impl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractBool, JSONExtractBoolImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractString, JSONExtractStringImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtract, JSONExtractImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeysAndValues, JSONExtractKeysAndValuesImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractRaw, JSONExtractRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractArrayRaw, JSONExtractArrayRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeysAndValuesRaw, JSONExtractKeysAndValuesRawImpl>>();
    factory.registerFunction<JSONOverloadResolver<NameJSONExtractKeys, JSONExtractKeysImpl>>();
}

}
