#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>

namespace DB
{

/// Type information for dictionary function variants
struct TypeInfo
{
    String name;        /// Display name of the type e.g. Tuple
    String doc_link;    /// Docs site link for the type e.g. /sql-reference/data-types/tuple
};

namespace TypeTags
{
    /// Unsigned integer types
    struct UInt8 { static constexpr const char* name = "UInt8"; };
    struct UInt16 { static constexpr const char* name = "UInt16"; };
    struct UInt32 { static constexpr const char* name = "UInt32"; };
    struct UInt64 { static constexpr const char* name = "UInt64"; };
    struct UInt128 { static constexpr const char* name = "UInt128"; };
    struct UInt256 { static constexpr const char* name = "UInt256"; };
    /// Signed integer types
    struct Int8 { static constexpr const char* name = "Int8"; };
    struct Int16 { static constexpr const char* name = "Int16"; };
    struct Int32 { static constexpr const char* name = "Int32"; };
    struct Int64 { static constexpr const char* name = "Int64"; };
    struct Int128 { static constexpr const char* name = "Int128"; };
    struct Int256 { static constexpr const char* name = "Int256"; };
    /// Floating-point types
    struct Float32 { static constexpr const char* name = "Float32"; };
    struct Float64 { static constexpr const char* name = "Float64"; };
}

/// Helper to create a TypeInfo entry
constexpr TypeInfo makeTypeInfo(const char* name, const char* link)
{
    return TypeInfo{name, link};
}

/// Helper to add multiple types with the same documentation link (Int and UInt, Float32 and Float64)
template <typename... Types>
void addTypesWithLink(std::map<String, TypeInfo>& map, const char* link)
{
    (map.try_emplace(Types::name, makeTypeInfo(Types::name, link)), ...);
}

/// Initialize type information map
std::map<String, TypeInfo> initializeTypeInfos()
{
    std::map<String, TypeInfo> type_infos;

    /// Add integer types
    addTypesWithLink<
        TypeTags::UInt8, TypeTags::UInt16, TypeTags::UInt32, TypeTags::UInt64,
        TypeTags::UInt128, TypeTags::UInt256, TypeTags::Int8, TypeTags::Int16,
        TypeTags::Int32, TypeTags::Int64, TypeTags::Int128, TypeTags::Int256
    >(type_infos, "/sql-reference/data-types/int-uint");

    /// Add floating point types
    addTypesWithLink<TypeTags::Float32, TypeTags::Float64>(type_infos, "/sql-reference/data-types/float");

    /// Add other types for which the documentation links differ for each type
    const std::initializer_list<std::pair<String, TypeInfo>> other_types =
    {
        /// Date/Time
        {"Date",      {"Date",      "/sql-reference/data-types/date"}},
        {"Date32",    {"Date32",    "/sql-reference/data-types/date32"}},
        {"DateTime",  {"DateTime",  "/sql-reference/data-types/datetime"}},
        {"DateTime64",{"DateTime64","/sql-reference/data-types/datetime64"}},
        /// Special types
        {"UUID",      {"UUID",      "/sql-reference/data-types/uuid"}},
        {"IPv4",      {"IPv4",      "/sql-reference/data-types/ipv4"}},
        {"IPv6",      {"IPv6",      "/sql-reference/data-types/ipv6"}},
        {"String",    {"String",    "/sql-reference/data-types/string"}},
        {"FixedString", {"FixedString", "/sql-reference/data-types/fixedstring"}},
        {"Expression", {"Expression", "/sql-reference/syntax#expressions"}},
        {"Tuple", {"Expression", "/sql-reference/data-types/tuple"}}
    };

    for (const auto & type : other_types)
    {
        type_infos.insert(type);
    }
    return type_infos;
}

/// Global type information map
const auto type_infos = initializeTypeInfos();

/// Helper to get documentation link for a type
String getTypeDocLink(const String & type_name)
{
    auto it = type_infos.find(type_name);
    if (it != type_infos.end())
        return it->second.doc_link;
    return "";
}

namespace
{

/// Helper to get the description for dictGet<type> functions
String getDictGetDescription(const String & type_name)
{
    return fmt::format("Converts a dictionary attribute value to `{}` data type regardless of the dictionary configuration.", type_name);
}

String getDictGetOrDefaultDescription(const String & type_name)
{
    return fmt::format("Converts a dictionary attribute value to `{}` data type regardless of the dictionary configuration, or returns the provided default value if the key is not found.", type_name);
}

/// Helper to get the syntax for dictGet<type> functions
String getDictGetSyntax(const String & type_name)
{
    return fmt::format("dictGet{}(dict_name, attr_name, id_expr)", type_name);
}

/// Helper to get the syntax for dictGet<type>OrDefault functions
String getDictGetOrDefaultSyntax(const String & type_name)
{
    return fmt::format("dictGet{}OrDefault(dict_name, attr_name, id_expr, default_value_expr)", type_name);
}

/// Helper to get the arguments for dictGet<type> functions
FunctionDocumentation::Arguments getDictGetArguments()
{
    return {
        {"dict_name", fmt::format("Name of the dictionary. [`String`]({}).", getTypeDocLink("String"))},
        {"attr_name", fmt::format("Name of the column of the dictionary. [`String`]({})/[`Tuple(String)`]({}).", getTypeDocLink("String"), getTypeDocLink("String"))},
                {"id_expr", fmt::format(
            "Key value. [Expression]({}) returning dictionary key-type value or [`Tuple(T)`]({}) value (dictionary configuration dependent).",
            getTypeDocLink("Expression"),
            getTypeDocLink("Tuple")
        )}
    };
}

/// Helper to get the arguments for dictGet<type>OrDefault functions
FunctionDocumentation::Arguments getDictGetOrDefaultArguments()
{
    return
    {
        {"dict_name", fmt::format("Name of the dictionary. [`String`]({}).", getTypeDocLink("String"))},
        {"attr_name", fmt::format("Name of the column of the dictionary. [`String`]({})/[`Tuple(String)`]({}).", getTypeDocLink("String"), getTypeDocLink("String"))},
        {"id_expr", fmt::format(
            "Key value. [Expression]({}) returning dictionary key-type value or [`Tuple(T)`]({}) value (dictionary configuration dependent).",
            getTypeDocLink("Expression"),
            getTypeDocLink("Tuple")
        )},
        {"default_value_expr", fmt::format("Value(s) returned if the dictionary does not contain a row with the `id_expr` key. [`Expression`]({})/[`Tuple(T)`]({}) of `attr_names` type(s).", getTypeDocLink("Expression"), getTypeDocLink("tuple"))},
    };
}

/// Helper to get the returned value documentation for dictGet<type> functions
String getDictGetReturnedValue()
{
    return R"(
Returns the value of the dictionary attribute that corresponds to `id_expr`,
otherwise returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.

:::note
ClickHouse throws an exception if it cannot parse the value of the attribute or the value does not match the attribute data type.
:::
    )";
}

/// Helper to get the returned value documentation for dictGet<type>OrDefault functions
String getDictGetOrDefaultReturnedValue()
{
    return R"(
Returns the value of the dictionary attribute that corresponds to `id_expr`,
otherwise returns the value passed as the `default_value_expr` parameter.

:::note
ClickHouse throws an exception if it cannot parse the value of the attribute or the value does not match the attribute data type.
:::
    )";
}

}

REGISTER_FUNCTION(ExternalDictionaries)
{
    constexpr auto category_dictionary = FunctionDocumentation::Category::Dictionary;

    /// dictGet
    {
        FunctionDocumentation::Description description_dictGet = "Retrieves values from a dictionary.";
        FunctionDocumentation::Syntax syntax_dictGet = "dictGet('dict_name', attr_names, id_expr)";
        FunctionDocumentation::Arguments arguments_dictGet =
        {
            {"dict_name", fmt::format("Name of the dictionary. [`String`]({})", getTypeDocLink("String"))},
            {"attr_names", fmt::format("Name of the column of the dictionary, or tuple of column names. [`String`]({})/[`Tuple(String)`]({}).", getTypeDocLink("String"), getTypeDocLink("tuple"))},
            {"id_expr", fmt::format("Key value. [Expression]({}) returning a [`UInt64`]({})/[`Tuple(T)`]({}).", getTypeDocLink("Expression"), getTypeDocLink("UInt64"), getTypeDocLink("tuple")) }
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGet =
R"(
Returns the value of the dictionary attribute that corresponds to id_expr if the key is found.
If the key is not found, returns the content of the <null_value> element specified for the attribute in the dictionary configuration.
)";
        FunctionDocumentation::Examples examples_dictGet =
        {
            {
                "Retrieve a single attribute",
                "SELECT dictGet('ext_dict_test', 'c1', toUInt64(1)) AS val",
                "1"
            },
            {
                "Multiple attributes",
R"(
SELECT
    dictGet('ext_dict_mult', ('c1','c2'), number + 1) AS val,
    toTypeName(val) AS type
FROM system.numbers
LIMIT 3;
)",
R"(
┌─val─────┬─type───────────┐
│ (1,'1') │ Tuple(        ↴│
│         │↳    c1 UInt32,↴│
│         │↳    c2 String) │
│ (2,'2') │ Tuple(        ↴│
│         │↳    c1 UInt32,↴│
│         │↳    c2 String) │
│ (3,'3') │ Tuple(        ↴│
│         │↳    c1 UInt32,↴│
│         │↳    c2 String) │
└─────────┴────────────────┘
)"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictGet = {18, 16};

        FunctionDocumentation documentation_dictGet =
        {
            description_dictGet,
            syntax_dictGet,
            arguments_dictGet,
            returned_value_dictGet,
            examples_dictGet,
            introduced_in_dictGet,
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::get>>(documentation_dictGet);
    }

    /// dictGetOrDefault
    {
        FunctionDocumentation::Description description_dictGetOrDefault = "Retrieves values from a dictionary, with a default value if the key is not found.";
        FunctionDocumentation::Syntax syntax_dictGetOrDefault = "dictGetOrDefault('dict_name', attr_names, id_expr, default_value)";
        FunctionDocumentation::Arguments arguments_dictGetOrDefault =
        {
            {"dict_name", fmt::format("Name of the dictionary. [`{}`]({}).", "String", getTypeDocLink("String"))},
            {"attr_names", fmt::format("Name of the column of the dictionary, or tuple of column names. [`String`]({})/[`Tuple(String)`]({}).", getTypeDocLink("String"), getTypeDocLink("tuple"))},
            {"id_expr", fmt::format("Key value. [Expression]({}) returning a [`UInt64`]({})/[`Tuple(T)`]({}).", getTypeDocLink("Expression"), getTypeDocLink("UInt64"), getTypeDocLink("tuple"))},
            {"default_value", "Default value to return if the key is not found. Type must match the attribute's data type."}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGetOrDefault =
R"(
Returns the value of the dictionary attribute that corresponds to id_expr if the key is found.
If the key is not found, returns the default_value provided.
)";
        FunctionDocumentation::Examples examples_dictGetOrDefault =
        {
            {
                "Get value with default",
                "SELECT dictGetOrDefault('ext_dict_mult', 'c1', toUInt64(999), 0) AS val",
                "0"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictGetOrDefault = {18, 16};
        FunctionDocumentation documentation_dictGetOrDefault =
        {
            description_dictGetOrDefault,
            syntax_dictGetOrDefault,
            arguments_dictGetOrDefault,
            returned_value_dictGetOrDefault,
            examples_dictGetOrDefault,
            introduced_in_dictGetOrDefault,
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::getOrDefault>>(documentation_dictGetOrDefault);
    }

    /// dictGetOrNull
    {
        FunctionDocumentation::Description description_dictGetOrNull = "Retrieves values from a dictionary, returning NULL if the key is not found.";
        FunctionDocumentation::Syntax syntax_dictGetOrNull = "dictGetOrNull('dict_name', 'attr_name', id_expr)";
        FunctionDocumentation::Arguments arguments_dictGetOrNull =
        {
            {"dict_name", "Name of the dictionary. String literal."},
            {"attr_name", "Name of the column to retrieve. String literal."},
            {"id_expr", "Key value. Expression returning dictionary key-type value."}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGetOrNull =
R"(
Returns the value of the dictionary attribute that corresponds to `id_expr` if the key is found.
If the key is not found, returns `NULL`.
)";
        FunctionDocumentation::Examples examples_dictGetOrNull =
        {
            {
                "Example using the range key dictionary",
R"(
SELECT
    (number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value', number, toDate('2019-05-20')),
FROM system.numbers LIMIT 5 FORMAT TabSeparated;
)",
R"(
(0,'2019-05-20')  \N
(1,'2019-05-20')  First
(2,'2019-05-20')  Second
(3,'2019-05-20')  Third
(4,'2019-05-20')  \N
)"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictGetOrNull = {21, 4};
        FunctionDocumentation documentation_dictGetOrNull =
        {
            description_dictGetOrNull,
            syntax_dictGetOrNull,
            arguments_dictGetOrNull,
            returned_value_dictGetOrNull,
            examples_dictGetOrNull,
            introduced_in_dictGetOrNull,
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetOrNull>(documentation_dictGetOrNull);
    }

    /// dictGetUInt8
    {
        const String type_name = "UInt8";

        FunctionDocumentation documentation_dictGetUInt8 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {
                    "Usage example",
                    "SELECT dictGetUInt8('all_types_dict', 'UInt8_value', 1)",
R"(
┌─dictGetUInt8⋯_value', 1)─┐
│                      100 │
└──────────────────────────┘
)"
                }
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUInt8>(documentation_dictGetUInt8);
    }

    /// dictGetUInt8OrDefault
    {
        const String type_name = "UInt8";

        FunctionDocumentation documentation_dictGetUInt8OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetUInt8('all_types_dict', 'UInt8_value', 1);

-- for key which does not exist, returns the provided default value (0)
SELECT dictGetUInt8OrDefault('all_types_dict', 'UInt8_value', 999, 0);
)",
R"(
┌─dictGetUInt8⋯_value', 1)─┐
│                      100 │
└──────────────────────────┘
┌─dictGetUInt8⋯e', 999, 0)─┐
│                        0 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUInt8OrDefault>(documentation_dictGetUInt8OrDefault);
    }

    /// dictGetUInt16
    {
        const String type_name = "UInt16";

        FunctionDocumentation documentation_dictGetUInt16 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetUInt16('all_types_dict', 'UInt16_value', 1)",
R"(
┌─dictGetUInt1⋯_value', 1)─┐
│                     5000 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };


        factory.registerFunction<FunctionDictGetUInt16>(documentation_dictGetUInt16);
    }

    /// dictGetUInt16OrDefault
    {
        const String type_name = "UInt16";

        FunctionDocumentation documentation_dictGetUInt16OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetUInt16('all_types_dict', 'UInt16_value', 1);

-- for key which does not exist, returns the provided default value (0)
SELECT dictGetUInt16OrDefault('all_types_dict', 'UInt16_value', 999, 0);
)",
R"(
┌─dictGetUInt1⋯_value', 1)─┐
│                     5000 │
└──────────────────────────┘
┌─dictGetUInt1⋯e', 999, 0)─┐
│                        0 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUInt16OrDefault>(documentation_dictGetUInt16OrDefault);
    }

    /// dictGetUInt32
    {
        const String type_name = "UInt32";

        FunctionDocumentation documentation_dictGetUInt32 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetUInt32('all_types_dict', 'UInt32_value', 1)",
R"(
┌─dictGetUInt3⋯_value', 1)─┐
│                  1000000 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUInt32>(documentation_dictGetUInt32);
    }

    /// dictGetUInt32OrDefault
    {
        const String type_name = "UInt32";

        FunctionDocumentation documentation_dictGetUInt32OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetUInt32('all_types_dict', 'UInt32_value', 1);

-- for key which does not exist, returns the provided default value (0)
SELECT dictGetUInt32OrDefault('all_types_dict', 'UInt32_value', 999, 0);
)",
R"(
┌─dictGetUInt3⋯_value', 1)─┐
│                  1000000 │
└──────────────────────────┘
┌─dictGetUInt3⋯e', 999, 0)─┐
│                        0 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUInt32OrDefault>(documentation_dictGetUInt32OrDefault);
    }

    /// dictGetUInt64
    {
        const String type_name = "UInt64";

        FunctionDocumentation documentation_dictGetUInt64 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetUInt64('all_types_dict', 'UInt64_value', 1)",
R"(
┌─dictGetUInt6⋯_value', 1)─┐
│      9223372036854775807 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUInt64>(documentation_dictGetUInt64);
    }

    /// dictGetUInt64OrDefault
    {
        const String type_name = "UInt64";

        FunctionDocumentation documentation_dictGetUInt64OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetUInt64('all_types_dict', 'UInt64_value', 1);

-- for key which does not exist, returns the provideddefault value (0)
SELECT dictGetUInt64OrDefault('all_types_dict', 'UInt64_value', 999, 0);
)",
R"(
┌─dictGetUInt6⋯_value', 1)─┐
│      9223372036854775807 │
└──────────────────────────┘
┌─dictGetUInt6⋯e', 999, 0)─┐
│                        0 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUInt64OrDefault>(documentation_dictGetUInt64OrDefault);
    }

    /// dictGetInt8
    {
        const String type_name = "Int8";

        FunctionDocumentation documentation_dictGetInt8 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetInt8('all_types_dict', 'Int8_value', 1)",
R"(
┌─dictGetInt8(⋯_value', 1)─┐
│                     -100 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetInt8>(documentation_dictGetInt8);
    }

    /// dictGetInt8OrDefault
    {
        const String type_name = "Int8";

        FunctionDocumentation documentation_dictGetInt8OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetInt8('all_types_dict', 'Int8_value', 1);

-- for key which does not exist, returns the provided default value (-1)
SELECT dictGetInt8OrDefault('all_types_dict', 'Int8_value', 999, -1);
)",
R"(
┌─dictGetInt8(⋯_value', 1)─┐
│                     -100 │
└──────────────────────────┘
┌─dictGetInt8O⋯', 999, -1)─┐
│                       -1 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetInt8OrDefault>(documentation_dictGetInt8OrDefault);
    }
    /// dictGetInt16
    {
        const String type_name = "Int16";

        FunctionDocumentation documentation_dictGetInt16 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetInt16('all_types_dict', 'Int16_value', 1)",
R"(
┌─dictGetInt16⋯_value', 1)─┐
│                    -5000 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetInt16>(documentation_dictGetInt16);
    }

    /// dictGetInt16OrDefault
    {
        const String type_name = "Int16";

        FunctionDocumentation documentation_dictGetInt16OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetInt16('all_types_dict', 'Int16_value', 1);

-- for key which does not exist, returns the provided default value (-1)
SELECT dictGetInt16OrDefault('all_types_dict', 'Int16_value', 999, -1);
)",
R"(
┌─dictGetInt16⋯_value', 1)─┐
│                    -5000 │
└──────────────────────────┘
┌─dictGetInt16⋯', 999, -1)─┐
│                       -1 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetInt16OrDefault>(documentation_dictGetInt16OrDefault);
    }
    /// dictGetInt32
    {
        const String type_name = "Int32";

        FunctionDocumentation documentation_dictGetInt32 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetInt32('all_types_dict', 'Int32_value', 1)",
R"(
┌─dictGetInt32⋯_value', 1)─┐
│                 -1000000 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetInt32>(documentation_dictGetInt32);
    }

    /// dictGetInt32OrDefault
    {
        const String type_name = "Int32";

        FunctionDocumentation documentation_dictGetInt32OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetInt32('all_types_dict', 'Int32_value', 1);

-- for key which does not exist, returns the provided default value (-1)
SELECT dictGetInt32OrDefault('all_types_dict', 'Int32_value', 999, -1);
)",
R"(
┌─dictGetInt32⋯_value', 1)─┐
│                 -1000000 │
└──────────────────────────┘
┌─dictGetInt32⋯', 999, -1)─┐
│                       -1 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetInt32OrDefault>(documentation_dictGetInt32OrDefault);
    }

    /// dictGetInt64
    {
        const String type_name = "Int64";

        FunctionDocumentation documentation_dictGetInt64 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetInt64('all_types_dict', 'Int64_value', 1)",
R"(
┌─dictGetInt64⋯_value', 1)─┐
│     -9223372036854775808 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetInt64>(documentation_dictGetInt64);
    }

    /// dictGetInt64OrDefault
    {
        const String type_name = "Int64";

        FunctionDocumentation documentation_dictGetInt64OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetInt64('all_types_dict', 'Int64_value', 1);

-- for key which does not exist, returns the provided default value (-1)
SELECT dictGetInt64OrDefault('all_types_dict', 'Int64_value', 999, -1);
)",
R"(
┌─dictGetInt64⋯_value', 1)─┐
│     -9223372036854775808 │
└──────────────────────────┘
┌─dictGetInt64⋯', 999, -1)─┐
│                       -1 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetInt64OrDefault>(documentation_dictGetInt64OrDefault);
    }
    /// dictGetFloat32
    {
        const String type_name = "Float32";

        FunctionDocumentation documentation_dictGetFloat32 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetFloat32('all_types_dict', 'Float32_value', 1)",
R"(
┌─dictGetFloat⋯_value', 1)─┐
│                   123.45 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetFloat32>(documentation_dictGetFloat32);
    }

    /// dictGetFloat32OrDefault
    {
        const String type_name = "Float32";

        FunctionDocumentation documentation_dictGetFloat32OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetFloat32('all_types_dict', 'Float32_value', 1);

-- for key which does not exist, returns the provided default value (nan)
SELECT dictGetFloat32OrDefault('all_types_dict', 'Float32_value', 999, nan);
)",
R"(
┌─dictGetFloat⋯_value', 1)─┐
│                   123.45 │
└──────────────────────────┘
┌─dictGetFloat⋯, 999, nan)─┐
│                      nan │
└──────────────────────────┘
 )"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetFloat32OrDefault>(documentation_dictGetFloat32OrDefault);
    }
    /// dictGetFloat64
    {
        const String type_name = "Float64";

        FunctionDocumentation documentation_dictGetFloat64 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetFloat64('all_types_dict', 'Float64_value', 1)",
R"(
┌─dictGetFloat⋯_value', 1)─┐
│            987654.123456 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetFloat64>(documentation_dictGetFloat64);
    }

    /// dictGetFloat64OrDefault
    {
        const String type_name = "Float64";

        FunctionDocumentation documentation_dictGetFloat64OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetFloat64('all_types_dict', 'Float64_value', 1);

-- for key which does not exist, returns the provided default value (nan)
SELECT dictGetFloat64OrDefault('all_types_dict', 'Float64_value', 999, nan);
)",
R"(
┌─dictGetFloat⋯_value', 1)─┐
│            987654.123456 │
└──────────────────────────┘
┌─dictGetFloat⋯, 999, nan)─┐
│                      nan │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetFloat64OrDefault>(documentation_dictGetFloat64OrDefault);
    }

    /// dictGetDate
    {
        const String type_name = "Date";

        FunctionDocumentation documentation_dictGetDate =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetDate('all_types_dict', 'Date_value', 1)",
R"(
┌─dictGetDate(⋯_value', 1)─┐
│               2024-01-15 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetDate>(documentation_dictGetDate);
    }

    /// dictGetDateOrDefault
    {
        const String type_name = "Date";

        FunctionDocumentation documentation_dictGetDateOrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetDate('all_types_dict', 'Date_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetDateOrDefault('all_types_dict', 'Date_value', 999, toDate('1970-01-01'));
)",
R"(
┌─dictGetDate(⋯_value', 1)─┐
│               2024-01-15 │
└──────────────────────────┘
┌─dictGetDateO⋯70-01-01'))─┐
│               1970-01-01 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetDateOrDefault>(documentation_dictGetDateOrDefault);
    }

    /// dictGetDateTime
    {
        const String type_name = "DateTime";

        FunctionDocumentation documentation_dictGetDateTime =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetDateTime('all_types_dict', 'DateTime_value', 1)",
R"(
┌─dictGetDateT⋯_value', 1)─┐
│      2024-01-15 10:30:00 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetDateTime>(documentation_dictGetDateTime);
    }

    /// dictGetDateTimeOrDefault
    {
        const String type_name = "DateTime";

        FunctionDocumentation documentation_dictGetDateTimeOrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetDateTime('all_types_dict', 'DateTime_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetDateTimeOrDefault('all_types_dict', 'DateTime_value', 999, toDateTime('1970-01-01 00:00:00'));
)",
R"(
┌─dictGetDateT⋯_value', 1)─┐
│      2024-01-15 10:30:00 │
└──────────────────────────┘
┌─dictGetDateT⋯00:00:00'))─┐
│      1970-01-01 01:00:00 │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetDateTimeOrDefault>(documentation_dictGetDateTimeOrDefault);
    }

    /// dictGetUUID
    {
        const String type_name = "UUID";

        FunctionDocumentation documentation_dictGetUUID =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetUUID('all_types_dict', 'UUID_value', 1)",
R"(
┌─dictGetUUID('all_t⋯ 'UUID_value', 1)─┐
│ 550e8400-e29b-41d4-a716-446655440000 │
└──────────────────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUUID>(documentation_dictGetUUID);
    }

    /// dictGetUUIDOrDefault
    {
        const String type_name = "UUID";

        FunctionDocumentation documentation_dictGetUUIDOrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetUUID('all_types_dict', 'UUID_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetUUIDOrDefault('all_types_dict', 'UUID_value', 999, '00000000-0000-0000-0000-000000000000'::UUID);
)",
R"(
┌─dictGetUUID('all_t⋯ 'UUID_value', 1)─┐
│ 550e8400-e29b-41d4-a716-446655440000 │
└──────────────────────────────────────┘
┌─dictGetUUIDOrDefau⋯000000', 'UUID'))─┐
│ 00000000-0000-0000-0000-000000000000 │
└──────────────────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetUUIDOrDefault>(documentation_dictGetUUIDOrDefault);
    }

    /// dictGetIPv4
    {
        const String type_name = "IPv4";

        FunctionDocumentation documentation_dictGetIPv4 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetIPv4('all_types_dict', 'IPv4_value', 1)",
R"(
┌─dictGetIPv4(⋯_value', 1)─┐
│ 192.168.1.1              │
└──────────────────────────┘
)"},
            },
            {23, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetIPv4>(documentation_dictGetIPv4);
    }

    /// dictGetIPv4OrDefault
    {
        const String type_name = "IPv4";

        FunctionDocumentation documentation_dictGetIPv4OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetIPv4('all_types_dict', 'IPv4_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetIPv4OrDefault('all_types_dict', 'IPv4_value', 999, '0.0.0.0'::IPv4);
)",
R"(
┌─dictGetIPv4(⋯_value', 1)─┐
│ 192.168.1.1              │
└──────────────────────────┘
┌─dictGetIPv4O⋯', 'IPv4'))─┐
│ 0.0.0.0                  │
└──────────────────────────┘
)"},
            },
            {23, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetIPv4OrDefault>(documentation_dictGetIPv4OrDefault);
    }

    /// dictGetIPv6
    {
        const String type_name = "IPv6";

        FunctionDocumentation documentation_dictGetIPv6 =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example", "SELECT dictGetIPv6('all_types_dict', 'IPv6_value', 1)",
R"(
┌─dictGetIPv6(⋯_value', 1)─┐
│ 2001:db8::1              │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetIPv6>(documentation_dictGetIPv6);
    }

    /// dictGetIPv6OrDefault
    {
        const String type_name = "IPv6";

        FunctionDocumentation documentation_dictGetIPv6OrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetIPv6('all_types_dict', 'IPv6_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetIPv6OrDefault('all_types_dict', 'IPv6_value', 999, '::'::IPv6);
)",
R"(
┌─dictGetIPv6(⋯_value', 1)─┐
│ 2001:db8::1              │
└──────────────────────────┘
┌─dictGetIPv6O⋯', 'IPv6'))─┐
│ ::                       │
└──────────────────────────┘
)"},
            },
            {23, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetIPv6OrDefault>(documentation_dictGetIPv6OrDefault);
    }

    /// dictGetString
    {
        const String type_name = "String";

        FunctionDocumentation documentation_dictGetString =
        {
            getDictGetDescription(type_name),
            getDictGetSyntax(type_name),
            getDictGetArguments(),
            getDictGetReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetString('all_types_dict', 'String_value', 1);

-- for key which does not exist, returns empty string
SELECT dictGetString('all_types_dict', 'String_value', 999);
)",
R"(
┌─dictGetStrin⋯_value', 1)─┐
│ ClickHouse               │
└──────────────────────────┘
┌─dictGetStrin⋯alue', 999)─┐
│                          │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetString>(documentation_dictGetString);
    }

    /// dictGetStringOrDefault
    {
        const String type_name = "String";

        FunctionDocumentation documentation_dictGetStringOrDefault =
        {
            getDictGetOrDefaultDescription(type_name),
            getDictGetOrDefaultSyntax(type_name),
            getDictGetOrDefaultArguments(),
            getDictGetOrDefaultReturnedValue(),
            {
                {"Usage example",
R"(
-- for key which exists
SELECT dictGetString('all_types_dict', 'String_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetStringOrDefault('all_types_dict', 'String_value', 999, 'Not found');
)",
R"(
┌─dictGetStrin⋯_value', 1)─┐
│ ClickHouse               │
└──────────────────────────┘
┌─dictGetStrin⋯Not found')─┐
│ Not found                │
└──────────────────────────┘
)"},
            },
            {1, 1},  /// Version introduced
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetStringOrDefault>(documentation_dictGetStringOrDefault);
    }

    /// dictGetAll
    {
        FunctionDocumentation::Description description_dictGetAll =
R"(
Retrieves the attribute values of all nodes that matched each key in a [regular expression tree dictionary](../../sql-reference/dictionaries/index.md#regexp-tree-dictionary).

Apart from returning values of type `Array(T)` instead of `T`, this function behaves similarly to [`dictGet`](#dictGet).
)";

        FunctionDocumentation::Syntax syntax_dictGetAll = "dictGetAll('dict_name', attr_names, id_expr[, limit])";
        FunctionDocumentation::Arguments arguments_dictGetAll =
        {
            {"dict_name", fmt::format("Name of the regexp treedictionary.[`String`]({})", getTypeDocLink("String"))},
            {"attr_name", fmt::format("Name of the column to retrieve.[`String`]({})", getTypeDocLink("String"))},
            {"id_expr", fmt::format("Key value. [Expression]({}) returning an [Array(T)]({}) or [`Tuple(T)`]({}).", getTypeDocLink("Expression"), getTypeDocLink("array"), getTypeDocLink("tuple"))},
            {"limit", "Optional. The maximum length for each value array returned. When truncating, child nodes are given precedence over parent nodes, otherwise the defined list order for the regexp tree dictionary is respected. If unspecified, array length is unlimited."}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGetAll =
R"(
Returns an array of all values from the dictionary that match the given key.
If no matches are found, returns an empty array.
)";
        FunctionDocumentation::Examples examples_dictGetAll =
        {
            {
                "User agent parsing with dictGetAll",
R"(
SELECT
    'Mozilla/5.0 (Linux; Android 12; SM-G998B) Mobile Safari/537.36' AS user_agent,

    -- This will match ALL applicable patterns
    dictGetAll('regexp_tree', 'os_replacement', 'Mozilla/5.0 (Linux; Android 12; SM-G998B) Mobile Safari/537.36') AS all_matches,

    -- This returns only the first match
    dictGet('regexp_tree', 'os_replacement', 'Mozilla/5.0 (Linux; Android 12; SM-G998B) Mobile Safari/537.36') AS first_match;
)",
R"(
┌─user_agent─────────────────────────────────────────────────────┬─all_matches─────────────────────────────┬─first_match─┐
│ Mozilla/5.0 (Linux; Android 12; SM-G998B) Mobile Safari/537.36 │ ['Android','Android','Android','Linux'] │ Android     │
└────────────────────────────────────────────────────────────────┴─────────────────────────────────────────┴─────────────┘
)"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictGetAll = {23, 5};
        FunctionDocumentation documentation_dictGetAll =
        {
            description_dictGetAll,
            syntax_dictGetAll,
            arguments_dictGetAll,
            returned_value_dictGetAll,
            examples_dictGetAll,
            introduced_in_dictGetAll,
            category_dictionary
        };

        factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::getAll>>(documentation_dictGetAll);
    }

    /// dictGetHierarchy
    {
        FunctionDocumentation::Description description_dictGetHierarchy =
R"(
Creates an array, containing all the parents of a key in the [hierarchical dictionary](../../sql-reference/dictionaries/index.md#hierarchical-dictionaries).
)";
        FunctionDocumentation::Syntax syntax_dictGetHierarchy = "dictGetHierarchy(dict_name, key)";
        FunctionDocumentation::Arguments arguments_dictGetHierarchy =
        {
            {"dict_name", fmt::format("Name of the dictionary. [`String`]({}).", getTypeDocLink("String"))},
            {"key", fmt::format("Key value. [Expression]({}) returning a [`UInt64`]({})-type value.", getTypeDocLink("Expression"), getTypeDocLink("UInt64"))}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGetHierarchy = fmt::format("Returns parents for the key. [`Array(UInt64)`]({}).", getTypeDocLink("array"));
        FunctionDocumentation::Examples examples_dictGetHierarchy =
        {
            {"Get hierarchy for a key",
R"(
SELECT dictGetHierarchy('hierarchical_dictionary', 5)
)",
R"(
┌─dictGetHiera⋯ionary', 5)─┐
│ [5,2,1]                  │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictGetHierarchy = {1, 1};
        FunctionDocumentation::Category category_dictGetHierarchy = FunctionDocumentation::Category::Dictionary;
        FunctionDocumentation documentation_dictGetHierarchy =
        {
            description_dictGetHierarchy,
            syntax_dictGetHierarchy,
            arguments_dictGetHierarchy,
            returned_value_dictGetHierarchy,
            examples_dictGetHierarchy,
            introduced_in_dictGetHierarchy,
            category_dictGetHierarchy
        };

        factory.registerFunction<FunctionDictGetHierarchy>(documentation_dictGetHierarchy);
    }

    /// dictIsIn
    {
        FunctionDocumentation::Description description_dictIsIn =
R"(
Checks the ancestor of a key through the whole hierarchical chain in the dictionary.
)";
        FunctionDocumentation::Syntax syntax_dictIsIn = "dictIsIn(dict_name, child_id_expr, ancestor_id_expr)";
        FunctionDocumentation::Arguments arguments_dictIsIn =
        {
            {"dict_name", fmt::format("Name of the dictionary. [`String`]({}).", getTypeDocLink("String"))},
            {"child_id_expr", fmt::format("Key to be checked. [Expression]({}) returning a [`UInt64`]({})-type value.", getTypeDocLink("Expression"), getTypeDocLink("UInt64"))},
            {"ancestor_id_expr", fmt::format("Alleged ancestor of the `child_id_expr` key. [Expression]({}) returning a [`UInt64`]({})-type value.", getTypeDocLink("Expression"), getTypeDocLink("UInt64"))}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictIsIn = fmt::format("Returns `0` if `child_id_expr` is not a child of `ancestor_id_expr`, `1` if `child_id_expr` is a child of `ancestor_id_expr` or if `child_id_expr` is an `ancestor_id_expr`. [`UInt8`]({}).", getTypeDocLink("UInt8"));
        FunctionDocumentation::Examples examples_dictIsIn =
        {
            {"Check hierarchical relationship",
R"(
-- valid hierarchy
SELECT dictIsIn('hierarchical_dictionary', 6, 3)

-- invalid hierarchy
SELECT dictIsIn('hierarchical_dictionary', 3, 5)
)",
R"(
┌─dictIsIn('hi⋯ary', 6, 3)─┐
│                        1 │
└──────────────────────────┘
┌─dictIsIn('hi⋯ary', 3, 5)─┐
│                        0 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictIsIn = {1, 1};
        FunctionDocumentation::Category category_dictIsIn = FunctionDocumentation::Category::Dictionary;
        FunctionDocumentation documentation_dictIsIn =
        {
            description_dictIsIn,
            syntax_dictIsIn,
            arguments_dictIsIn,
            returned_value_dictIsIn,
            examples_dictIsIn,
            introduced_in_dictIsIn,
            category_dictIsIn
        };

        factory.registerFunction<FunctionDictIsIn>(documentation_dictIsIn);
    }

    /// dictGetChildren
    {
        FunctionDocumentation::Description description_dictGetChildren =
R"(
Returns first-level children as an array of indexes. It is the inverse transformation for [dictGetHierarchy](#dictgethierarchy).
)";
        FunctionDocumentation::Syntax syntax_dictGetChildren = "dictGetChildren(dict_name, key)";
        FunctionDocumentation::Arguments arguments_dictGetChildren =
        {
            {"dict_name", fmt::format("Name of the dictionary. [`String`]({}).", getTypeDocLink("String"))},
            {"key", fmt::format("Key to be checked. [Expression]({}) returning a [`UInt64`]({})-type value.", getTypeDocLink("Expression"), getTypeDocLink("UInt64"))}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGetChildren = fmt::format("Returns the first-level descendants for the key. [`Array(UInt64)`]({})", getTypeDocLink("array"));
        FunctionDocumentation::Examples examples_dictGetChildren =
        {
            {"Get the first-level children of a dictionary",
R"(
SELECT dictGetChildren('hierarchical_dictionary', 2);
)",
R"(
┌─dictGetChild⋯ionary', 2)─┐
│ [4,5]                    │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictGetChildren = {21, 4};
        FunctionDocumentation::Category category_dictGetChildren = FunctionDocumentation::Category::Dictionary;
        FunctionDocumentation documentation_dictGetChildren =
        {
            description_dictGetChildren,
            syntax_dictGetChildren,
            arguments_dictGetChildren,
            returned_value_dictGetChildren,
            examples_dictGetChildren,
            introduced_in_dictGetChildren,
            category_dictGetChildren
        };
        factory.registerFunction<FunctionDictGetChildrenOverloadResolver>(documentation_dictGetChildren);
    }

    /// dictGetDescendants
    {
        FunctionDocumentation::Description description_dictGetDescendants =
R"(
Returns all descendants as if the [`dictGetChildren`](#dictGetChildren) function were applied `level` times recursively.
)";
        FunctionDocumentation::Syntax syntax_dictGetDescendants = "dictGetDescendants(dict_name, key, level)";
        FunctionDocumentation::Arguments arguments_dictGetDescendants =
        {
            {"dict_name", fmt::format("Name of the dictionary. [`String`]({}).", getTypeDocLink("String"))},
            {"key", fmt::format("Key to be checked. [Expression]({}) returning a [`UInt64`]({})-type value.", getTypeDocLink("Expression"), getTypeDocLink("UInt64"))},
            {"level", fmt::format("Key to be checked. Hierarchy level. If `level = 0` returns all descendants to the end. [`UInt8`]({}).", getTypeDocLink("UInt64"))}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGetDescendants = fmt::format("Returns the descendants for the key. [`Array(UInt64)`]({})", getTypeDocLink("array"));
        FunctionDocumentation::Examples examples_dictGetDescendants =
        {
            {
                "Get the first-level children of a dictionary",
R"(
-- consider the following hierarchical dictionary:
-- 0 (Root)
-- └── 1 (Level 1 - Node 1)
--     ├── 2 (Level 2 - Node 2)
--     │   ├── 4 (Level 3 - Node 4)
--     │   └── 5 (Level 3 - Node 5)
--     └── 3 (Level 2 - Node 3)
--         └── 6 (Level 3 - Node 6)

SELECT dictGetDescendants('hierarchical_dictionary', 0, 2)
)",
R"(
┌─dictGetDesce⋯ary', 0, 2)─┐
│ [3,2]                    │
└──────────────────────────┘
)"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictGetDescendants = {21, 4};
        FunctionDocumentation::Category category_dictGetDescendants = FunctionDocumentation::Category::Dictionary;
        FunctionDocumentation documentation_dictGetDescendants =
        {
            description_dictGetDescendants,
            syntax_dictGetDescendants,
            arguments_dictGetDescendants,
            returned_value_dictGetDescendants,
            examples_dictGetDescendants,
            introduced_in_dictGetDescendants,
            category_dictGetDescendants
        };

        factory.registerFunction<FunctionDictGetDescendantsOverloadResolver>(documentation_dictGetDescendants);
    }

    /// dictHas
    {
        FunctionDocumentation::Description description_dictHas = "Checks whether a key is present in a dictionary.";
        FunctionDocumentation::Syntax syntax_dictHas = "dictHas('dict_name', id_expr)";
        FunctionDocumentation::Arguments arguments_dictHas =
        {
            {"dict_name", fmt::format("Name of the dictionary. [`String`]({}).", getTypeDocLink("String"))},
            {"id_expr", fmt::format("Key value. [Expression]({}) returning an [Array(T)]({}) or [`Tuple(T)`]({}).", getTypeDocLink("Expression"), getTypeDocLink("array"), getTypeDocLink("tuple"))}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictHas = fmt::format("Returns `1` if the key exists, otherwise `0`. [`UInt8`]({})", getTypeDocLink("UInt8"));
        FunctionDocumentation::Examples examples_dictHas =
        {
            {
                "Check for the existence of a key in a dictionary",
R"(
-- consider the following hierarchical dictionary:
-- 0 (Root)
-- └── 1 (Level 1 - Node 1)
--     ├── 2 (Level 2 - Node 2)
--     │   ├── 4 (Level 3 - Node 4)
--     │   └── 5 (Level 3 - Node 5)
--     └── 3 (Level 2 - Node 3)
--         └── 6 (Level 3 - Node 6)

SELECT dictHas('hierarchical_dictionary', 2);
SELECT dictHas('hierarchical_dictionary', 7);
)",
R"(
┌─dictHas('hie⋯ionary', 2)─┐
│                        1 │
└──────────────────────────┘
┌─dictHas('hie⋯ionary', 7)─┐
│                        0 │
└──────────────────────────┘
)"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in_dictHas = {1, 1};
        FunctionDocumentation::Category category_dictHas = FunctionDocumentation::Category::Dictionary;
        FunctionDocumentation documentation_dictHas =
        {
            description_dictHas,
            syntax_dictHas,
            arguments_dictHas,
            returned_value_dictHas,
            examples_dictHas,
            introduced_in_dictHas,
            category_dictHas
        };

        factory.registerFunction<FunctionDictHas>(documentation_dictHas);
    }
    }
}
