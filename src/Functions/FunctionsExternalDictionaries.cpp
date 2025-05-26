#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>


namespace DB
{

REGISTER_FUNCTION(ExternalDictionaries)
{
    constexpr auto dict_get_description { R"(
Retrieves values from a dictionary.

Accepts 3 parameters:
-- name of the dictionary;
-- name of the column of the dictionary or tuple of column names;
-- key value - expression returning dictionary key-type value or tuple-type value - depending on the dictionary configuration;

Returned value: value of the dictionary attribute parsed in the {} if key is found, otherwise <null_value> element specified in the dictionary configuration.

Throws an exception if cannot parse the value of the attribute or the value does not match the attribute data type.
)" };

    constexpr auto dict_get_or_default_description { R"(
Retrieves values from a dictionary.

Accepts 4 parameters:
-- name of the dictionary;
-- name of the column of the dictionary or tuple of column names;
-- key value - expression returning dictionary key-type value or tuple-type value - depending on the dictionary configuration;
-- default values returned if the dictionary does not contain a row with the key value;

Returned value: value of the dictionary attribute parsed in the {} if key is found, otherwise default value.

Throws an exception if cannot parse the value of the attribute or the value does not match the attribute data type.
)" };

    constexpr auto dict_get_or_null_description { R"(
Retrieves values from a dictionary.

Accepts 3 parameters:
-- name of the dictionary;
-- name of the column of the dictionary or tuple of column names;
-- key value - expression returning dictionary key-type value or tuple-type value - depending on the dictionary configuration;

Returned value: value of the dictionary attribute parsed in the attribute's data type if key is found, otherwise NULL.

Throws an exception if cannot parse the value of the attribute or the value does not match the attribute data type.
)" };

    constexpr auto dict_get_all_description { R"(
Retrieves all values from a dictionary corresponding to the given key values.

Accepts 3 or 4 parameters:
-- name of the dictionary;
-- name of the column of the dictionary or tuple of column names;
-- key value - expression returning dictionary key-type value or tuple-type value - depending on the dictionary configuration;
-- [optional] maximum number of values to return for each attribute;

Returned value: array of dictionary attribute values parsed in the attribute's data type if key is found, otherwise empty array.

Throws an exception if cannot parse the value of the attribute, the value does not match the attribute data type, or the dictionary doesn't support this function.
)" };

    factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::get>>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "attribute's data type") });
    factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::getOrDefault>>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "attribute's data type") });
    factory.registerFunction<FunctionDictGetOrNull>(FunctionDocumentation{ .description=dict_get_or_null_description });
    factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::getAll>>(FunctionDocumentation{ .description=dict_get_all_description });

    factory.registerFunction<FunctionDictGetUInt8>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "UInt8") });
    factory.registerFunction<FunctionDictGetUInt16>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "UInt16") });
    factory.registerFunction<FunctionDictGetUInt32>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "UInt32") });
    factory.registerFunction<FunctionDictGetUInt64>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "UInt64") });
    factory.registerFunction<FunctionDictGetInt8>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "Int8") });
    factory.registerFunction<FunctionDictGetInt16>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "Int16") });
    factory.registerFunction<FunctionDictGetInt32>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "Int32") });
    factory.registerFunction<FunctionDictGetInt64>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "Int64") });
    factory.registerFunction<FunctionDictGetFloat32>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "Float32") });
    factory.registerFunction<FunctionDictGetFloat64>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "Float64") });
    factory.registerFunction<FunctionDictGetDate>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "Date") });
    factory.registerFunction<FunctionDictGetDateTime>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "DateTime") });
    factory.registerFunction<FunctionDictGetUUID>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "UUID") });
    factory.registerFunction<FunctionDictGetIPv4>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "IPv4") });
    factory.registerFunction<FunctionDictGetIPv6>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "IPv6") });
    factory.registerFunction<FunctionDictGetString>(FunctionDocumentation{ .description=fmt::format(dict_get_description, "String") });

    factory.registerFunction<FunctionDictGetUInt8OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "UInt8") });
    factory.registerFunction<FunctionDictGetUInt16OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "UInt16") });
    factory.registerFunction<FunctionDictGetUInt32OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "UInt32") });
    factory.registerFunction<FunctionDictGetUInt64OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "UInt64") });
    factory.registerFunction<FunctionDictGetInt8OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "Int8") });
    factory.registerFunction<FunctionDictGetInt16OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "Int16") });
    factory.registerFunction<FunctionDictGetInt32OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "Int32") });
    factory.registerFunction<FunctionDictGetInt64OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "Int64") });
    factory.registerFunction<FunctionDictGetFloat32OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "Float32") });
    factory.registerFunction<FunctionDictGetFloat64OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "Float64") });
    factory.registerFunction<FunctionDictGetDateOrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "Date") });
    factory.registerFunction<FunctionDictGetDateTimeOrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "DateTime") });
    factory.registerFunction<FunctionDictGetUUIDOrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "UUID") });
    factory.registerFunction<FunctionDictGetIPv4OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "IPv4") });
    factory.registerFunction<FunctionDictGetIPv6OrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "IPv6") });
    factory.registerFunction<FunctionDictGetStringOrDefault>(FunctionDocumentation{ .description=fmt::format(dict_get_or_default_description, "String") });

    factory.registerFunction<FunctionDictHas>(FunctionDocumentation{ .description=R"(
Checks whether a key is present in a dictionary.
Accepts 2 parameters: name of the dictionary, key value - expression returning dictionary key-type value or tuple-type value - depending on the dictionary configuration.
Returned value: 0 if there is no key, 1 if there is a key, type of UInt8
)"});

    factory.registerFunction<FunctionDictGetHierarchy>(FunctionDocumentation{ .description=R"(
Creates an array, containing all the parents of a key in the hierarchical dictionary.
Accepts 2 parameters: name of the dictionary, key value - expression returning a UInt64-type value.
Returned value: parents for the key, type of Array(UInt64)
)"});

    factory.registerFunction<FunctionDictIsIn>(FunctionDocumentation{ .description=R"(
Checks the ancestor of a key through the whole hierarchical chain in the dictionary.
Accepts 3 parameters: name of the dictionary, key to be checked - expression returning a UInt64-type value, alleged ancestor of the key - expression returning a UInt64-type.
Returned value: 0 if key is not a child of the ancestor, 1 if key is a child of the ancestor or if key is the ancestor, type of UInt8
)"});

    factory.registerFunction<FunctionDictGetChildrenOverloadResolver>(FunctionDocumentation{ .description=R"(
Returns first-level children as an array of indexes. It is the inverse transformation for dictGetHierarchy.
Accepts 2 parameters: name of the dictionary, key value - expression returning a UInt64-type value.
Returned value: first-level descendants for the key, type of Array(UInt64)
)"});

    factory.registerFunction<FunctionDictGetDescendantsOverloadResolver>(FunctionDocumentation{ .description=R"(
Returns all descendants as if dictGetChildren function was applied level times recursively.
Accepts 3 parameters: name of the dictionary, key value - expression returning a UInt64-type value, level — hierarchy level - If level = 0 returns all descendants to the end - UInt8
Returned value: descendants for the key, type of Array(UInt64)
)"});
}

}
