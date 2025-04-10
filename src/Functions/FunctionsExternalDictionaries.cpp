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

    factory.registerFunction<FunctionDictHas>(FunctionDocumentation{ 
        .description="Checks whether a key is present in a dictionary.",
        .syntax="dictHas('dict_name', id_expr)",
        .arguments={
            {"dict_name", "Name of the dictionary. [String literal](/sql-reference/syntax#string)."},
            {"id_expr", "Key value. [Expression](/sql-reference/syntax#expressions) returning dictionary key-type value or [Tuple](../data-types/tuple.md)-type value depending on the dictionary configuration."}
        },
        .returned_value=R"(
- 0, if there is no key. [UInt8](../data-types/int-uint.md).
- 1, if there is a key. [UInt8](../data-types/int-uint.md).        
        )",
        .examples={{"", "", ""}},
        .category=FunctionDocumentation::Category::Dictionary
    });

    factory.registerFunction<FunctionDictGetHierarchy>(FunctionDocumentation{ 
        .description="Creates an array, containing all the parents of a key in the hierarchical dictionary.",
        .syntax="dictGetHierarchy('dict_name', key)",
        .arguments={
            {"dict_name", "Name of the dictionary. String literal."},
            {"key", "Key value. Expression returning a UInt64-type value."}
        },
        .returned_value="Parents for the key. Array(UInt64).",
        .examples={{"","",""}},
        .category=FunctionDocumentation::Category::Dictionary
    });

    factory.registerFunction<FunctionDictIsIn>(FunctionDocumentation{ 
        .description="Checks the ancestor of a key through the whole hierarchical chain in the dictionary.",
        .syntax="dictIsIn('dict_name', child_id_expr, ancestor_id_expr)",
        .arguments={
            {"dict_name", "Name of the dictionary. [String literal](/sql-reference/syntax#string)."},
            {"child_id_expr", "Key to be checked. [Expression](/sql-reference/syntax#expressions) returning a [UInt64](../data-types/int-uint.md)-type value."},
            {"ancestor_id_expr", "Alleged ancestor of the `child_id_expr` key. [Expression](/sql-reference/syntax#expressions) returning a [UInt64](../data-types/int-uint.md)-type value."}
        },
        .returned_value=R"(
- `0`, if `child_id_expr` is not a child of `ancestor_id_expr`. [UInt8](../data-types/int-uint.md).
- `1`, if `child_id_expr` is a child of `ancestor_id_expr` or if `child_id_expr` is an `ancestor_id_expr`. [UInt8](../data-types/int-uint.md).        
        )",
        .examples={{"","",""}},
        .category=FunctionDocumentation::Category::Dictionary
    });

    factory.registerFunction<FunctionDictGetChildrenOverloadResolver>(FunctionDocumentation{ 
        .description="Returns first-level children as an array of indexes. It is the inverse transformation for dictGetHierarchy.",
        .syntax="dictGetChildren(dict_name, key)",
        .arguments={
            {"dict_name", "Name of the dictionary. [String literal](/sql-reference/syntax#string)."},
            {"key", "Key value. [Expression](/sql-reference/syntax#expressions) returning a [UInt64](../data-types/int-uint.md)-type value."}
        },
        .returned_value="First-level descendants for the key. [Array](../data-types/array.md)([UInt64](../data-types/int-uint.md)).",
        .examples={
            {
                "Basic example",
                R"(
Consider the hierarchic dictionary:

```text
┌─id─┬─parent_id─┐
│  1 │         0 │
│  2 │         1 │
│  3 │         1 │
│  4 │         2 │
└────┴───────────┘
```

First-level children:

```sql
SELECT dictGetChildren('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
```                
                )",
                R"(
```text
┌─dictGetChildren('hierarchy_flat_dictionary', number)─┐
│ [1]                                                  │
│ [2,3]                                                │
│ [4]                                                  │
│ []                                                   │
└──────────────────────────────────────────────────────┘
```                
                )"
            }
        }
    });

    factory.registerFunction<FunctionDictGetDescendantsOverloadResolver>(FunctionDocumentation{ 
        .description=R"(
Returns all descendants as if dictGetChildren function was applied level times recursively.
Accepts 3 parameters: name of the dictionary, key value - expression returning a UInt64-type value, level — hierarchy level - If level = 0 returns all descendants to the end - UInt8
Returned value: descendants for the key, type of Array(UInt64)
        )",
        .syntax="dictGetDescendants(dict_name, key, level)",
        .arguments={
            {"dict_name", "Name of the dictionary. [String literal](/sql-reference/syntax#string)."},
            {"key", "Key value. [Expression](/sql-reference/syntax#expressions) returning a [UInt64](../data-types/int-uint.md)-type value."},
            {"level", "Hierarchy level. If `level = 0` returns all descendants to the end. [UInt8](../data-types/int-uint.md)."}
        },
        .returned_value="- Descendants for the key. [Array](../data-types/array.md)([UInt64](../data-types/int-uint.md)).",
        .examples={
            {
                "Get all descendants",
                R"(
Consider the hierarchic dictionary:

```text
┌─id─┬─parent_id─┐
│  1 │         0 │
│  2 │         1 │
│  3 │         1 │
│  4 │         2 │
└────┴───────────┘
```
All descendants:

```sql
SELECT dictGetDescendants('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 4;
```              
                )",
                R"(
```text
┌─dictGetDescendants('hierarchy_flat_dictionary', number)─┐
│ [1,2,3,4]                                               │
│ [2,3,4]                                                 │
│ [4]                                                     │
│ []                                                      │
└─────────────────────────────────────────────────────────┘
```                  
                )"
            },
            {
                "Get first-level descendants",
                R"(
First-level descendants:

```sql
SELECT dictGetDescendants('hierarchy_flat_dictionary', number, 1) FROM system.numbers LIMIT 4;
```                
                )",
                R"(
```text
┌─dictGetDescendants('hierarchy_flat_dictionary', number, 1)─┐
│ [1]                                                        │
│ [2,3]                                                      │
│ [4]                                                        │
│ []                                                         │
└────────────────────────────────────────────────────────────┘
```                
                )"
            }
        },
        .category=FunctionDocumentation::Category::Dictionary
    });
}

}
