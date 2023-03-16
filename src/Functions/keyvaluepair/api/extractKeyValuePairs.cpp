#include "extractKeyValuePairs.h"

#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>

#include <Common/assert_cast.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>

#include <Functions/keyvaluepair/src/KeyValuePairExtractorBuilder.h>

namespace DB
{

ExtractKeyValuePairs::ExtractKeyValuePairs()
    : return_type(std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()))
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

static ColumnPtr extract(ColumnPtr data_column, std::shared_ptr<KeyValuePairExtractor> extractor)
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

    ColumnPtr keys_ptr = std::move(keys);

    return ColumnMap::create(keys_ptr, std::move(values), std::move(offsets));
}

auto ExtractKeyValuePairs::getExtractor(const ArgumentExtractor::ParsedArguments & parsed_arguments)
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

ColumnPtr ExtractKeyValuePairs::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    auto parsed_arguments = ArgumentExtractor::extract(arguments);

    auto extractor_without_escaping = getExtractor(parsed_arguments);

    return extract(parsed_arguments.data_column, extractor_without_escaping);
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

DataTypePtr ExtractKeyValuePairs::getReturnTypeImpl(const DataTypes &) const
{
    return return_type;
}

ColumnNumbers ExtractKeyValuePairs::getArgumentsThatAreAlwaysConstant() const
{
    return {1, 2, 3, 4, 5};
}

REGISTER_FUNCTION(ExtractKeyValuePairs)
{
    factory.registerFunction<ExtractKeyValuePairs>(
        Documentation(
            R"(Extracts key value pairs from any string. The string does not need to be 100% structured in a key value pair format,
            it might contain noise (e.g. log files). The key value pair format to be interpreted should be specified via function arguments.
            A key value pair consists of a key followed by a key_value_pair_delimiter and a value.
            Special characters (e.g. $!@#¨) must be escaped or added to the value_special_character_allow_list.
            Enclosed/ quoted keys and values are accepted.

            If escaping is not needed, it is advised not to set the escape character as a more optimized version of the algorithm will be used.

            The below grammar is a simplified representation of what is expected/ supported (does not include escaping and character allow_listing):

            * line = (reserved_char* key_value_pair)*  reserved_char*
            * key_value_pair = key kv_separator value
            * key = <quoted_string> |  asciichar asciialphanumeric*
            * kv_separator = ':'
            * value = <quoted_string> | asciialphanum*
            * item_delimiter = ','

            Both key and values accepts underscores as well.

            **Syntax**
            ``` sql
            extractKeyValuePairs(data, [escape_character], [key_value_pair_delimiter], [item_delimiter], [enclosing_character], [value_special_characters_allow_list])
            ```

            **Arguments**
            - data - string to extract key value pairs from. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - escape_character - character to be used as escape. Defaults to '\\'. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - key_value_pair_delimiter - character to be used as delimiter between the key and the value. Defaults to ':'. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - item_delimiter - character to be used as delimiter between pairs. Defaults to ','. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - enclosing_character - character to be used as enclosing/quoting character. Defaults to '\"'. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
            - value_special_characters_allow_list - special (e.g. $!@#¨) value characters to ignore during value parsing and include without the need to escape. Should be specified without separators. Defaults to empty. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

            **Returned values**
            - The extracted key value pairs in a Map(String, String).

            **Example**

            Query:

            ``` sql
            select extractKeyValuePairs('9 ads =nm, no*:me: neymar, age: 30, daojmskdpoa and a height: 1.75, school: lupe* picasso, team: psg,', '*', ':', ',', '"', '.');
            ```

            Result:

            ``` text
            ┌─extractKeyValuePairs('9 ads =nm, no*:me: neymar, age: 30, daojmskdpoa and a height: 1.75, school: lupe* picasso, team: psg,', '*', ':', ',', '"', '.')─┐
            │ {'no:me':'neymar','age':'30','height':'1.75','school':'lupe picasso','team':'psg'}                                                                     │
            └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            ```)")
        );
}

}
