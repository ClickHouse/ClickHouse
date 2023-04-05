#include <Functions/keyvaluepair/extractKeyValuePairs.h>

#include <Functions/keyvaluepair/impl/KeyValuePairExtractor.h>
#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>
#include <Functions/keyvaluepair/ArgumentExtractor.h>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>


namespace
{

using namespace DB;

auto getExtractor(const ArgumentExtractor::ParsedArguments & parsed_arguments)
{
    auto builder = KeyValuePairExtractorBuilder();

    if (parsed_arguments.with_escaping.value_or(true))
    {
        builder.withEscaping();
    }

    if (parsed_arguments.key_value_pair_delimiter)
    {
        builder.withKeyValuePairDelimiter(parsed_arguments.key_value_pair_delimiter.value());
    }

    if (!parsed_arguments.pair_delimiters.empty())
    {
        builder.withItemDelimiters(parsed_arguments.pair_delimiters);
    }

    if (parsed_arguments.quoting_character)
    {
        builder.withQuotingCharacter(parsed_arguments.quoting_character.value());
    }

    return builder.build();
}

ColumnPtr extract(ColumnPtr data_column, std::shared_ptr<KeyValuePairExtractor> extractor)
{
    auto offsets = ColumnUInt64::create();

    auto keys = ColumnString::create();
    auto values = ColumnString::create();

    uint64_t offset = 0u;

    for (auto i = 0u; i < data_column->size(); i++)
    {
        auto row = data_column->getDataAt(i).toView();

        auto pairs_count = extractor->extract(row, keys, values);

        offset += pairs_count;

        offsets->insert(offset);
    }

    keys->validate();
    values->validate();

    ColumnPtr keys_ptr = std::move(keys);

    return ColumnMap::create(keys_ptr, std::move(values), std::move(offsets));
}


}

namespace DB
{

ExtractKeyValuePairs::ExtractKeyValuePairs()
{
}

String ExtractKeyValuePairs::getName() const
{
    return name;
}

FunctionPtr ExtractKeyValuePairs::create(ContextPtr)
{
    return std::make_shared<ExtractKeyValuePairs>();
}

DataTypePtr ExtractKeyValuePairs::getReturnTypeImpl(const DataTypes &) const
{
    return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
}

bool ExtractKeyValuePairs::isVariadic() const
{
    return true;
}

bool ExtractKeyValuePairs::isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const
{
    return false;
}

std::size_t ExtractKeyValuePairs::getNumberOfArguments() const
{
    return 0u;
}

ColumnNumbers ExtractKeyValuePairs::getArgumentsThatAreAlwaysConstant() const
{
    return {1, 2, 3, 4, 5};
}

ColumnPtr ExtractKeyValuePairs::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    auto parsed_arguments = ArgumentExtractor::extract(arguments);

    auto extractor_without_escaping = getExtractor(parsed_arguments);

    return extract(parsed_arguments.data_column, extractor_without_escaping);
}

REGISTER_FUNCTION(ExtractKeyValuePairs)
{
    factory.registerFunction<ExtractKeyValuePairs>(
        Documentation(
            R"(Extracts key-value pairs from any string. The string does not need to be 100% structured in a key value pair format;

            It can contain noise (e.g. log files). The key-value pair format to be interpreted should be specified via function arguments.

            A key-value pair consists of a key followed by a `key_value_delimiter` and a value. Quoted keys and values are also supported. Key value pairs must be separated by pair delimiters.
            Escaping support can be turned on and off.

            Escape sequences supported: `\x`, `\N`, `\a`, `\b`, `\e`, `\f`, `\n`, `\r`, `\t`, `\v` and `\0`.
            Non standard escape sequences are returned as it is (including the backslash) unless they are one of the following:
            `\\`, `'`, `"`, `backtick`, `/`, `=` or ASCII control characters (c <= 31).

            **Syntax**
            ``` sql
            extractKeyValuePairs(data, [key_value_delimiter], [pair_delimiter], [quoting_character], [escaping_support])
            ```

            **Arguments**
            - `data` - String to extract key-value pairs from. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - `key_value_delimiter` - Character to be used as delimiter between the key and the value. Defaults to `:`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - `pair_delimiters` - Set of character to be used as delimiters between pairs. Defaults to `\space`, `,` and `;`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - `quoting_character` - Character to be used as quoting character. Defaults to `"`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - `escaping_support` - Turns escaping support on or off. Defaults to off.

            **Returned values**
            - The extracted key-value pairs in a Map(String, String).

            **Examples**

            Query:

            **Simple case**
            ``` sql
            arthur :) select extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv

            SELECT extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv

            Query id: f9e0ca6f-3178-4ee2-aa2c-a5517abb9cee

            ┌─kv──────────────────────────────────────────────────────────────────────┐
            │ {'name':'neymar','age':'31','team':'psg','nationality':'brazil'}        │
            └─────────────────────────────────────────────────────────────────────────┘
            ```

            **Single quote as quoting character**
            ``` sql
            arthur :) select extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'', '0') as kv

            SELECT extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'', '0') as kv

            Query id: 0e22bf6b-9844-414a-99dc-32bf647abd5e

            ┌─kv───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
            │ {'name':'neymar','age':'31','team':'psg','nationality':'brazil','last_key':'last_value'}                                 │
            └──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            ```

            **Escape sequences without escape sequences support**
            ``` sql
            arthur :) select extractKeyValuePairs('age:\\x0A', ':', ',', '"', '0') as kv

            SELECT extractKeyValuePairs('age:\\x0A', ':', ',', '"', '0') as kv

            Query id: 4aa4a519-d130-4b09-b555-9214f9416c01

            ┌─kv────────────────────────────────────────────────────┐
            │ {'age':'\\x0A'}                                       │
            └───────────────────────────────────────────────────────┘
            ```

            **Escape sequences with escape sequence support turned on**
            ``` sql
            arthur :) select extractKeyValuePairs('age:\\x0A', ':', ',', '"', '1') as kv

            SELECT extractKeyValuePairs('age:\\x0A', ':', ',', '"', '1') as kv

            Query id: 2c2044c6-3ca7-4300-a582-33b3192ad88d

            ┌─kv────────────────────────────────────────────────────┐
            │ {'age':'\n'}                                          │
            └───────────────────────────────────────────────────────┘
            ```)")
        );
}

}
