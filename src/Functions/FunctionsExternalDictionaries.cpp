#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsExternalDictionaries.h>

namespace DB
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
    FunctionDocumentation::Arguments args;
    args.emplace_back(FunctionDocumentation::Argument{"dict_name", "Name of the dictionary.", {"String"}});
    args.emplace_back(FunctionDocumentation::Argument{"attr_name", "Name of the column of the dictionary.", {"String", "Tuple(String)"}});
    args.emplace_back(FunctionDocumentation::Argument{"id_expr", "Key value. An expression returning a dictionary key-type value or tuple value (dictionary configuration dependent).", {"Expression", "Tuple(T)"}});
    return args;
}

/// Helper to get the arguments for dictGet<type>OrDefault functions
FunctionDocumentation::Arguments getDictGetOrDefaultArguments()
{
    FunctionDocumentation::Arguments args;
    args.emplace_back(FunctionDocumentation::Argument{"dict_name", "Name of the dictionary.", {"String"}});
    args.emplace_back(FunctionDocumentation::Argument{"attr_name", "Name of the column of the dictionary.", {"String", "Tuple(String)"}});
    args.emplace_back(FunctionDocumentation::Argument{"id_expr", "Key value. Expression returning dictionary key-type value or tuple value (dictionary configuration dependent).", {"Expression", "Tuple(T)"}});
    args.emplace_back(FunctionDocumentation::Argument{"default_value_expr", "Value(s) returned if the dictionary does not contain a row with the `id_expr` key.", {"Expression", "Tuple(T)"}});
    return args;
}

/// Helper to get the returned value documentation for dictGet<type> functions
FunctionDocumentation::ReturnedValue getDictGetReturnedValue()
{
    return {R"(
Returns the value of the dictionary attribute that corresponds to `id_expr`,
otherwise returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.

:::note
ClickHouse throws an exception if it cannot parse the value of the attribute or the value does not match the attribute data type.
:::
    )"};
}

/// Helper to get the returned value documentation for dictGet<type>OrDefault functions
FunctionDocumentation::ReturnedValue getDictGetOrDefaultReturnedValue()
{
    return {R"(
Returns the value of the dictionary attribute that corresponds to `id_expr`,
otherwise returns the value passed as the `default_value_expr` parameter.

:::note
ClickHouse throws an exception if it cannot parse the value of the attribute or the value does not match the attribute data type.
:::
    )"};
}

REGISTER_FUNCTION(ExternalDictionaries)
{
    constexpr auto category_dictionary = FunctionDocumentation::Category::Dictionary;

    /// dictGet
    {
        FunctionDocumentation::Description description = "Retrieves values from a dictionary.";
        FunctionDocumentation::Syntax syntax = "dictGet('dict_name', attr_names, id_expr)";
        FunctionDocumentation::Arguments arguments = {
            {"dict_name", "Name of the dictionary.", {"String"}},
            {"attr_names", "Name of the column of the dictionary, or tuple of column names.", {"String", "Tuple(String)"}},
            {"id_expr", "Key value. An expression returning UInt64/Tuple(T).", {"UInt64", "Tuple(T)"}}
        };
        FunctionDocumentation::ReturnedValue returned_value =
{R"(
Returns the value of the dictionary attribute that corresponds to id_expr if the key is found.
If the key is not found, returns the content of the `<null_value>` element specified for the attribute in the dictionary configuration.
)"};
        FunctionDocumentation::Examples examples = {
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
        FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::get>>(documentation);
    }

    /// dictGetOrDefault
    {
        FunctionDocumentation::Description description = "Retrieves values from a dictionary, with a default value if the key is not found.";
        FunctionDocumentation::Syntax syntax = "dictGetOrDefault('dict_name', attr_names, id_expr, default_value)";
        FunctionDocumentation::Arguments arguments = {
            {"dict_name", "Name of the dictionary.", {"String"}},
            {"attr_names", "Name of the column of the dictionary, or tuple of column names.", {"String", "Tuple(String)"}},
            {"id_expr", "Key value. An expression returning UInt64/Tuple(T).", {"UInt64", "Tuple(T)"}},
            {"default_value", "Default value to return if the key is not found. Type must match the attribute's data type.", {}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the value of the dictionary attribute that corresponds to `id_expr` if the key is found.
If the key is not found, returns the `default_value` provided.
)"};
        FunctionDocumentation::Examples examples = {{"Get value with default", "SELECT dictGetOrDefault('ext_dict_mult', 'c1', toUInt64(999), 0) AS val", "0"}};
        FunctionDocumentation::IntroducedIn introduced_in = {18, 16};
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::getOrDefault>>(documentation);
    }

    /// dictGetOrNull
    {
        FunctionDocumentation::Description description = "Retrieves values from a dictionary, returning NULL if the key is not found.";
        FunctionDocumentation::Syntax syntax = "dictGetOrNull('dict_name', 'attr_name', id_expr)";
        FunctionDocumentation::Arguments arguments = {
            {"dict_name", "Name of the dictionary. String literal."},
            {"attr_name", "Name of the column to retrieve. String literal."},
            {"id_expr", "Key value. Expression returning dictionary key-type value."}
        };
        FunctionDocumentation::ReturnedValue returned_value = {R"(
Returns the value of the dictionary attribute that corresponds to `id_expr` if the key is found.
If the key is not found, returns `NULL`.
)"};
        FunctionDocumentation::Examples examples = {{"Example using the range key dictionary", R"(
SELECT
    (number, toDate('2019-05-20')),
    dictGetOrNull('range_key_dictionary', 'value', number, toDate('2019-05-20')),
FROM system.numbers LIMIT 5 FORMAT TabSeparated;
)", R"(
(0,'2019-05-20')  \N
(1,'2019-05-20')  First
(2,'2019-05-20')  Second
(3,'2019-05-20')  Third
(4,'2019-05-20')  \N
)"}};
        FunctionDocumentation::IntroducedIn introduced_in = {21, 4};
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetOrNull>(documentation);
    }

    /// dictGetUInt8
    {
        const String type_name = "UInt8";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                "SELECT dictGetUInt8('all_types_dict', 'UInt8_value', 1)",
R"(
┌─dictGetUInt8⋯_value', 1)─┐
│                      100 │
└──────────────────────────┘
)"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation_dictGetUInt8 = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUInt8>(documentation_dictGetUInt8);
    }

    /// dictGetUInt8OrDefault
    {
        const String type_name = "UInt8";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUInt8OrDefault>(documentation);
    }

    /// dictGetUInt16
    {
        const String type_name = "UInt16";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetUInt16('all_types_dict', 'UInt16_value', 1)",
R"(
┌─dictGetUInt1⋯_value', 1)─┐
│                     5000 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUInt16>(documentation);
    }

    /// dictGetUInt16OrDefault
    {
        const String type_name = "UInt16";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUInt16OrDefault>(documentation);
    }

    /// dictGetUInt32
    {
        const String type_name = "UInt32";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetUInt32('all_types_dict', 'UInt32_value', 1)",
R"(
┌─dictGetUInt3⋯_value', 1)─┐
│                  1000000 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUInt32>(documentation);
    }

    /// dictGetUInt32OrDefault
    {
        const String type_name = "UInt32";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUInt32OrDefault>(documentation);
    }

    /// dictGetUInt64
    {
        const String type_name = "UInt64";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetUInt64('all_types_dict', 'UInt64_value', 1)",
R"(
┌─dictGetUInt6⋯_value', 1)─┐
│      9223372036854775807 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUInt64>(documentation);
    }

    /// dictGetUInt64OrDefault
    {
        const String type_name = "UInt64";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUInt64OrDefault>(documentation);
    }

    /// dictGetInt8
    {
        const String type_name = "Int8";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetInt8('all_types_dict', 'Int8_value', 1)",
R"(
┌─dictGetInt8(⋯_value', 1)─┐
│                     -100 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetInt8>(documentation);
    }

    /// dictGetInt8OrDefault
    {
        const String type_name = "Int8";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetInt8OrDefault>(documentation);
    }

    /// dictGetInt16
    {
        const String type_name = "Int16";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetInt16('all_types_dict', 'Int16_value', 1)",
R"(
┌─dictGetInt16⋯_value', 1)─┐
│                    -5000 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetInt16>(documentation);
    }

    /// dictGetInt16OrDefault
    {
        const String type_name = "Int16";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetInt16OrDefault>(documentation);
    }
    /// dictGetInt32
    {
        const String type_name = "Int32";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetInt32('all_types_dict', 'Int32_value', 1)",
R"(
┌─dictGetInt32⋯_value', 1)─┐
│                -1000000  │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetInt32>(documentation);
    }

    /// dictGetInt32OrDefault
    {
        const String type_name = "Int32";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example",
R"(
-- for key which exists
SELECT dictGetInt32('all_types_dict', 'Int32_value', 1);

-- for key which does not exist, returns the provided default value (-1)
SELECT dictGetInt32OrDefault('all_types_dict', 'Int32_value', 999, -1);
)",
R"(
┌─dictGetInt32⋯_value', 1)─┐
│                -1000000  │
└──────────────────────────┘
┌─dictGetInt32⋯', 999, -1)─┐
│                       -1 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetInt32OrDefault>(documentation);
    }

    /// dictGetInt64
    {
        const String type_name = "Int64";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetInt64('all_types_dict', 'Int64_value', 1)",
R"(
┌─dictGetInt64⋯_value', 1)───┐
│       -9223372036854775807 │
└────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetInt64>(documentation);
    }

    /// dictGetInt64OrDefault
    {
        const String type_name = "Int64";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetInt64OrDefault>(documentation);
    }
    /// dictGetFloat32
    {
        const String type_name = "Float32";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetFloat32('all_types_dict', 'Float32_value', 1)",
R"(
┌─dictGetFloat⋯_value', 1)─┐
│               -123.123   │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetFloat32>(documentation);
    }

    /// dictGetFloat32OrDefault
    {
        const String type_name = "Float32";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example",
R"(
-- for key which exists
SELECT dictGetFloat32('all_types_dict', 'Float32_value', 1);

-- for key which does not exist, returns the provided default value (-1.0)
SELECT dictGetFloat32OrDefault('all_types_dict', 'Float32_value', 999, -1.0);
)",
R"(
┌─dictGetFloat⋯_value', 1)─┐
│                   123.45 │
└──────────────────────────┘
┌─dictGetFloat⋯e', 999, -1)─┐
│                       -1  │
└───────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetFloat32OrDefault>(documentation);
    }
    /// dictGetFloat64
    {
        const String type_name = "Float64";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetFloat64('all_types_dict', 'Float64_value', 1)",
R"(
┌─dictGetFloat⋯_value', 1)─┐
│                 -123.123 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetFloat64>(documentation);
    }

    /// dictGetFloat64OrDefault
    {
        const String type_name = "Float64";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
 )"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetFloat64OrDefault>(documentation);
    }

/// dictGetDate
{
    const String type_name = "Date";

    FunctionDocumentation::Description description = getDictGetDescription(type_name);
    FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
    FunctionDocumentation::Arguments arguments = getDictGetArguments();
    FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT dictGetDate('all_types_dict', 'Date_value', 1)",
R"(
┌─dictGetDate(⋯_value', 1)─┐
│               2020-01-01 │
└──────────────────────────┘
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

    factory.registerFunction<FunctionDictGetDate>(documentation);
}

/// dictGetDateOrDefault
{
    const String type_name = "Date";

    FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
    FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
    FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
    FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
    FunctionDocumentation::Examples examples = {
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
)"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

    factory.registerFunction<FunctionDictGetDateOrDefault>(documentation);
}

    /// dictGetDateTime
    {
        const String type_name = "DateTime";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetDateTime('all_types_dict', 'DateTime_value', 1)",
R"(
┌─dictGetDateT⋯_value', 1)─┐
│      2024-01-15 10:30:00 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetDateTime>(documentation);
    }

    /// dictGetDateTimeOrDefault
    {
        const String type_name = "DateTime";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
┌─dictGetDateT⋯0:00:00'))──┐
│      1970-01-01 00:00:00 │
└──────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetDateTimeOrDefault>(documentation);
    }

    /// dictGetUUID
    {
        const String type_name = "UUID";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetUUID('all_types_dict', 'UUID_value', 1)",
R"(
┌─dictGetUUID(⋯_value', 1)─────────────┐
│ 123e4567-e89b-12d3-a456-426614174000 │
└──────────────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUUID>(documentation);
    }

    /// dictGetUUIDOrDefault
    {
        const String type_name = "UUID";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
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
┌─dictGetUUIDOrDefa⋯000000000000'::UUID)─┐
│ 00000000-0000-0000-0000-000000000000   │
└────────────────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetUUIDOrDefault>(documentation);
    }

    /// dictGetIPv4
    {
        const String type_name = "IPv4";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetIPv4('all_types_dict', 'IPv4_value', 1)",
R"(
┌─dictGetIPv4('all_⋯ 'IPv4_value', 1)─┐
│ 192.168.0.1                         │
└─────────────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetIPv4>(documentation);
    }

    /// dictGetIPv4OrDefault
    {
        const String type_name = "IPv4";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example",
R"(
-- for key which exists
SELECT dictGetIPv4('all_types_dict', 'IPv4_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetIPv4OrDefault('all_types_dict', 'IPv4_value', 999, toIPv4('0.0.0.0'));
)",
R"(
┌─dictGetIPv4('all_⋯ 'IPv4_value', 1)─┐
│ 192.168.0.1                         │
└─────────────────────────────────────┘
┌─dictGetIPv4OrDefa⋯0.0.0.0'))─┐
│ 0.0.0.0                      │
└──────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {23, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetIPv4OrDefault>(documentation);
    }

    /// dictGetIPv6
    {
        const String type_name = "IPv6";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetIPv6('all_types_dict', 'IPv6_value', 1)",
R"(
┌─dictGetIPv6('all_⋯ 'IPv6_value', 1)─┐
│ 2001:db8:85a3::8a2e:370:7334        │
└─────────────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {23, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetIPv6>(documentation);
    }

    /// dictGetIPv6OrDefault
    {
        const String type_name = "IPv6";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example",
R"(
-- for key which exists
SELECT dictGetIPv6('all_types_dict', 'IPv6_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetIPv6OrDefault('all_types_dict', 'IPv6_value', 999, '::1'::IPv6);
)",
R"(
┌─dictGetIPv6('all_⋯ 'IPv6_value', 1)─┐
│ 2001:db8:85a3::8a2e:370:7334        │
└─────────────────────────────────────┘
┌─dictGetIPv6OrDefa⋯:1'::IPv6)─┐
│ ::1                          │
└──────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {23, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetIPv6OrDefault>(documentation);
    }

    /// dictGetString
    {
        const String type_name = "String";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example", "SELECT dictGetString('all_types_dict', 'String_value', 1)",
R"(
┌─dictGetString(⋯_value', 1)─┐
│ test string                │
└────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetString>(documentation);
    }

    /// dictGetStringOrDefault
    {
        const String type_name = "String";

        FunctionDocumentation::Description description = getDictGetOrDefaultDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetOrDefaultSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetOrDefaultArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetOrDefaultReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example",
R"(
-- for key which exists
SELECT dictGetString('all_types_dict', 'String_value', 1);

-- for key which does not exist, returns the provided default value
SELECT dictGetStringOrDefault('all_types_dict', 'String_value', 999, 'default');
)",
R"(
┌─dictGetString(⋯_value', 1)─┐
│ test string                │
└────────────────────────────┘
┌─dictGetStringO⋯ 999, 'default')─┐
│ default                         │
└─────────────────────────────────┘
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};  /// Version introduced
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetStringOrDefault>(documentation);
    }

    /// dictGetAll
    {
        const String type_name = "All";

        FunctionDocumentation::Description description = getDictGetDescription(type_name);
        FunctionDocumentation::Syntax syntax = getDictGetSyntax(type_name);
        FunctionDocumentation::Arguments arguments = getDictGetArguments();
        FunctionDocumentation::ReturnedValue returned_value = getDictGetReturnedValue();
        FunctionDocumentation::Examples examples = {
            {"Usage example",
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
)"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {23, 5};
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetNoType<DictionaryGetFunctionType::getAll>>(documentation);
    }

    /// dictGetHierarchy
    {
        FunctionDocumentation::Description description =
R"(
Creates an array, containing all the parents of a key in the [hierarchical dictionary](/docs/sql-reference/statements/create/dictionary/layouts/hierarchical#hierarchical-dictionaries).
)";
        FunctionDocumentation::Syntax syntax = "dictGetHierarchy(dict_name, key)";
        FunctionDocumentation::Arguments arguments = {
            {"dict_name", "Name of the dictionary.", {"String"}},
            {"key", "Key value.", {"const String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns parents for the key.", {"Array(UInt64)"}};
        FunctionDocumentation::Examples examples = {
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
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation documentation{description, syntax, arguments, {}, returned_value, examples, introduced_in, category_dictionary};

        factory.registerFunction<FunctionDictGetHierarchy>(documentation);
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
            {"dict_name", "Name of the dictionary.", {"String"}},
            {"child_id_expr", "Key to be checked.", {"String"}},
            {"ancestor_id_expr", "Alleged ancestor of the `child_id_expr` key.", {"const String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictIsIn = {"Returns `0` if `child_id_expr` is not a child of `ancestor_id_expr`, `1` if `child_id_expr` is a child of `ancestor_id_expr` or if `child_id_expr` is an `ancestor_id_expr`.", {"UInt8"}};
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
            {},
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
Returns first-level children as an array of indexes. It is the inverse transformation for [dictGetHierarchy](#dictGetHierarchy).
)";
        FunctionDocumentation::Syntax syntax_dictGetChildren = "dictGetChildren(dict_name, key)";
        FunctionDocumentation::Arguments arguments_dictGetChildren =
        {
            {"dict_name", "Name of the dictionary.", {"String"}},
            {"key", "Key to be checked.", {"const String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGetChildren = {"Returns the first-level descendants for the key.", {"Array(UInt64)"}};
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
            {},
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
            {"dict_name", "Name of the dictionary.", {"String"}},
            {"key", "Key to be checked.", {"const String"}},
            {"level", "Key to be checked. Hierarchy level. If `level = 0` returns all descendants to the end.", {"UInt8"}}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictGetDescendants = {"Returns the descendants for the key.", {"Array(UInt64)"}};
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
            {},
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
            {"dict_name", "Name of the dictionary.", {"String"}},
            {"id_expr", "Key value", {"const String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value_dictHas = {"Returns `1` if the key exists, otherwise `0`.", {"UInt8"}};
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
            {},
            returned_value_dictHas,
            examples_dictHas,
            introduced_in_dictHas,
            category_dictHas
        };

        factory.registerFunction<FunctionDictHas>(documentation_dictHas);
    }
    }
}
