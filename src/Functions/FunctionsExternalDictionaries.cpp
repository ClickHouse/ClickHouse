#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>


namespace DB
{

    /// dictGet<T> FunctionDocumentation parameters
    constexpr auto dict_get_description { R"(
Retrieves values from a dictionary.

Throws an exception if unable to parse the value of the attribute or the value does not match {}.
    )" };
    constexpr auto dict_get_syntax = "dictGet(dict_name, attr_names, id_expr)";
    const FunctionDocumentation::Arguments dict_get_arguments = {
        {"dict_name", "Name of the dictionary. String literal."},
        {"attr_names", "Name of the column of the dictionary, String literal, or tuple of column names, Tuple(String literal."},
        {"id_expr", "Key value. Expression returning dictionary key-type value or Tuple-type value depending on the dictionary configuration."}
    };
    constexpr auto dict_get_return_value = "Value of the dictionary attribute parsed in the {} if key is found, otherwise <null_value> element specified in the dictionary configuration.";

    /// dictGet<T>OrDefault FunctionDocumentation parameters
    constexpr auto dict_get_or_default_description { R"(
Retrieves values from a dictionary.

Throws an exception if unable to parse the value of the attribute or the value does not match {}.
    )" };
    constexpr auto dict_get_or_default_syntax = "dictGetOrDefault(dict_name, attr_names, id_expr, default_value_expr)";
    const FunctionDocumentation::Arguments dict_get_or_default_arguments = {
        {"dict_name", "Name of the dictionary. String literal."},
        {"attr_names", "Name of the column of the dictionary, String literal, or tuple of column names, Tuple(String literal."},
        {"id_expr", "Key value. Expression returning dictionary key-type value or Tuple-type value depending on the dictionary configuration."},
        {"default_value_expr", "Values returned if the dictionary does not contain a row with the `id_expr` key. Expression or Tuple(Expression), returning the value (or values) in the data types configured for the `attr_names` attribute."}
    };
    constexpr auto dict_get_or_default_return_value = "Value of the dictionary attribute parsed in the {} if key is found, otherwise default value.";

    /// dictGet<T>OrNull FunctionDocumentation parameters
    constexpr auto dict_get_or_null_description { R"(
Retrieves values from a dictionary.

Throws an exception if unable to parse the value of the attribute or the value does not match the attribute data type.
    )" };
    constexpr auto dict_get_or_null_syntax = "dictGetOrNull('dict_name', attr_name, id_expr)";
    constexpr auto dict_get_or_null_return_value = "Value of the dictionary attribute parsed in the {} if key is found, otherwise NULL.";

    /// dictGetAll FunctionDocumentation parameters
    constexpr auto dict_get_all_description { R"(
Retrieves all values from a dictionary corresponding to the given key values.

Throws an exception if unable to parse the value of the attribute, the value does not match the attribute data type, or the dictionary doesn't support this function.
)" };
    constexpr auto dict_get_all_syntax = "dictGetAll('dict_name', attr_names, id_expr[, limit])";
    const FunctionDocumentation::Arguments dict_get_all_arguments = {
        {"dict_name", "Name of the dictionary. String literal."},
        {"attr_names", "Name of the column of the dictionary, String literal, or tuple of column names, Tuple(String literal."},
        {"id_expr", "Key value. Expression returning dictionary key-type value or Tuple-type value depending on the dictionary configuration."},
        {"limit", "Maximum length for each value array returned. When truncating, child nodes are given precedence over parent nodes, and otherwise the defined list order for the regexp tree dictionary is respected. If unspecified, array length is unlimited."}
    };
    constexpr auto dict_get_all_return_value = "array of dictionary attribute values parsed in the attribute's data type if key is found, otherwise empty array.";

    FunctionDocumentation createDictGetDocumentation(const dictGetType& type, const std::string& type_name )
    {

        switch (type) {
            case dictGetType::dictGet:
                return FunctionDocumentation{
                    .description = fmt::format(dict_get_description, type_name),
                    .syntax = dict_get_syntax,
                    .arguments = dict_get_arguments,
                    .returned_value = fmt::format(dict_get_return_value, type_name),
                    .examples = {{"", "", ""}},
                    .category = FunctionDocumentation::Category::Dictionary
                };

            case dictGetType::dictGetOrDefault:
                return FunctionDocumentation{
                    .description = fmt::format(dict_get_or_default_description, type_name),
                    .syntax = dict_get_or_default_syntax,
                    .arguments = dict_get_or_default_arguments,
                    .returned_value = fmt::format(dict_get_or_default_return_value, type_name),
                    .examples = {{"", "", ""}},
                    .category = FunctionDocumentation::Category::Dictionary
                };

            case dictGetType::dictGetOrNull:
                return FunctionDocumentation{
                    .description = dict_get_or_null_description,
                    .syntax = dict_get_or_null_syntax,
                    .arguments = dict_get_arguments,
                    .returned_value = fmt::format(dict_get_or_null_return_value, type_name),
                    .examples = {{"", "", ""}},
                    .category = FunctionDocumentation::Category::Dictionary
                };

            case dictGetType::dictGetAll:
                return FunctionDocumentation{
                    .description = dict_get_all_description,
                    .syntax = dict_get_all_syntax,
                    .arguments = dict_get_arguments,
                    .returned_value = dict_get_all_return_value,
                    .examples = {{"", "", ""}},
                    .category = FunctionDocumentation::Category::Dictionary
                };
        }
    }

REGISTER_FUNCTION(ExternalDictionaries)
{
    factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::get>>(createDictGetDocumentation(dictGetType::dictGet, "the attribute's data type"));
    factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::getOrDefault>>(createDictGetDocumentation(dictGetType::dictGetOrDefault, "the attribute's data type"));
    factory.registerFunction<FunctionDictGetOrNull>(createDictGetDocumentation(dictGetType::dictGetOrNull, ""));
    factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::getAll>>(createDictGetDocumentation(dictGetType::dictGetAll, ""));

    factory.registerFunction<FunctionDictGetUInt8>(createDictGetDocumentation(dictGetType::dictGet, "UInt8"));
    factory.registerFunction<FunctionDictGetUInt16>(createDictGetDocumentation(dictGetType::dictGet, "UInt16"));
    factory.registerFunction<FunctionDictGetUInt32>(createDictGetDocumentation(dictGetType::dictGet, "UInt32"));
    factory.registerFunction<FunctionDictGetUInt64>(createDictGetDocumentation(dictGetType::dictGet, "UInt64"));
    factory.registerFunction<FunctionDictGetInt8>(createDictGetDocumentation(dictGetType::dictGet, "Int8"));
    factory.registerFunction<FunctionDictGetInt16>(createDictGetDocumentation(dictGetType::dictGet, "Int16"));
    factory.registerFunction<FunctionDictGetInt32>(createDictGetDocumentation(dictGetType::dictGet, "Int32"));
    factory.registerFunction<FunctionDictGetInt64>(createDictGetDocumentation(dictGetType::dictGet, "Int64"));
    factory.registerFunction<FunctionDictGetFloat32>(createDictGetDocumentation(dictGetType::dictGet, "Float32"));
    factory.registerFunction<FunctionDictGetFloat64>(createDictGetDocumentation(dictGetType::dictGet, "Float64"));
    factory.registerFunction<FunctionDictGetDate>(createDictGetDocumentation(dictGetType::dictGet, "Date"));
    factory.registerFunction<FunctionDictGetDateTime>(createDictGetDocumentation(dictGetType::dictGet, "DateTime"));
    factory.registerFunction<FunctionDictGetUUID>(createDictGetDocumentation(dictGetType::dictGet, "UUID"));
    factory.registerFunction<FunctionDictGetIPv4>(createDictGetDocumentation(dictGetType::dictGet, "IPv4"));
    factory.registerFunction<FunctionDictGetIPv6>(createDictGetDocumentation(dictGetType::dictGet, "IPv6"));
    factory.registerFunction<FunctionDictGetString>(createDictGetDocumentation(dictGetType::dictGet, "String"));

    factory.registerFunction<FunctionDictGetUInt8OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "UInt8"));
    factory.registerFunction<FunctionDictGetUInt16OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "UInt16"));
    factory.registerFunction<FunctionDictGetUInt32OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "UInt32"));
    factory.registerFunction<FunctionDictGetUInt64OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "UInt64"));
    factory.registerFunction<FunctionDictGetInt8OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "Int8"));
    factory.registerFunction<FunctionDictGetInt16OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "Int16"));
    factory.registerFunction<FunctionDictGetInt32OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "Int32"));
    factory.registerFunction<FunctionDictGetInt64OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "Int64"));
    factory.registerFunction<FunctionDictGetFloat32OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "Float32"));
    factory.registerFunction<FunctionDictGetFloat64OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "Float64"));
    factory.registerFunction<FunctionDictGetDateOrDefault>(createDictGetDocumentation(dictGetType::dictGet, "Date"));
    factory.registerFunction<FunctionDictGetDateTimeOrDefault>(createDictGetDocumentation(dictGetType::dictGet, "DateTime"));
    factory.registerFunction<FunctionDictGetUUIDOrDefault>(createDictGetDocumentation(dictGetType::dictGet, "UUID"));
    factory.registerFunction<FunctionDictGetIPv4OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "IPv4"));
    factory.registerFunction<FunctionDictGetIPv6OrDefault>(createDictGetDocumentation(dictGetType::dictGet, "IPv6"));
    factory.registerFunction<FunctionDictGetStringOrDefault>(createDictGetDocumentation(dictGetType::dictGetOrDefault, "String"));

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
