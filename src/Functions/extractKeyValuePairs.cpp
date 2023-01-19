#include "extractKeyValuePairs.h"

#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/ReplaceStringImpl.h>
#include <Functions/keyvaluepair/src/KeyValuePairExtractorBuilder.h>
#include <Functions/keyvaluepair/src/impl/CHKeyValuePairExtractor.h>
#include <Common/assert_cast.h>

#include <chrono>

/* Only needed for the sake of this example. */
#include <iostream>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

ExtractKeyValuePairs::ExtractKeyValuePairs()
    : return_type(std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()))
{
}

String ExtractKeyValuePairs::getName() const
{
    return name;
}

ColumnPtr ExtractKeyValuePairs::chInline(
    ColumnPtr data_column,
    CharArgument escape_character,
    CharArgument key_value_pair_delimiter,
    CharArgument item_delimiter,
    CharArgument enclosing_character,
    SetArgument)
{
    InlineEscapingKeyStateHandler key_state_handler(*key_value_pair_delimiter, *escape_character, *enclosing_character);
    InlineEscapingValueStateHandler value_state_handler(*escape_character, *item_delimiter, *enclosing_character);
    CHKeyValuePairExtractor ch_extractor(key_state_handler, value_state_handler);

    auto offsets = ColumnUInt64::create();

    auto keys = ColumnString::create();
    auto values = ColumnString::create();

    uint64_t offset = 0u;

    for (auto i = 0u; i < data_column->size(); i++)
    {
        auto row = data_column->getDataAt(i).toView();

        auto inserted_rows = ch_extractor.extract(row, keys, values);

        offset += inserted_rows;

        offsets->insert(offset);
    }

    ColumnPtr keys_ptr = std::move(keys);

    return ColumnMap::create(keys_ptr, std::move(values), std::move(offsets));
}


ColumnPtr ExtractKeyValuePairs::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
//    auto t1 = high_resolution_clock::now();

    auto [data_column, escape_character, key_value_pair_delimiter, item_delimiter,
          enclosing_character, value_special_characters_allow_list, ch_inline]
        = parseArguments(arguments);

    if (ch_inline)
    {
        return chInline(data_column, escape_character, key_value_pair_delimiter, item_delimiter, enclosing_character, value_special_characters_allow_list);
    }

    auto extractor = getExtractor(
        escape_character, key_value_pair_delimiter, item_delimiter, enclosing_character);

//    auto t2 = high_resolution_clock::now();
    auto raw_columns = extract(extractor, data_column);

    ColumnPtr keys_ptr = std::move(raw_columns.keys);
//    auto t3 = high_resolution_clock::now();


    auto map = ColumnMap::create(keys_ptr, std::move(raw_columns.values), std::move(raw_columns.offsets));
//    auto t4 = high_resolution_clock::now();

//    std::cout<<"Time taken for building extractor is: "<<duration_cast<microseconds>(t2 - t1).count()<<"u\n";
//    std::cout<<"Time taken for building extracting & creating output is: "<<duration_cast<microseconds>(t3 - t2).count()<<"u\n";
//    std::cout<<"Time taken for whole process is: "<<duration_cast<microseconds>(t4 - t1).count()<<"u\n";
    return map;

//    return raw_columns;

//
//    // improve escape character..
//    auto escaped_out = escape(raw_columns, escape_character ? escape_character.value() : '\\');
//    auto t3 = high_resolution_clock::now();
//
//
//    std::cout<<"Time taken for extraction is: "<<duration_cast<microseconds>(t2 - t1).count()<<"u\n";
//    std::cout<<"Time taken for escaping is: "<<duration_cast<microseconds>(t3 - t2).count()<<"u\n";
//
//    return escaped_out;
}

bool ExtractKeyValuePairs::isVariadic() const
{
    return true;
}

bool ExtractKeyValuePairs::isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const
{
    return false;
}

std::size_t ExtractKeyValuePairs::getNumberOfArguments() const
{
    return 0u;
}

DataTypePtr ExtractKeyValuePairs::getReturnTypeImpl(const DataTypes & /*arguments*/) const
{
    return return_type;
}

ExtractKeyValuePairs::ParsedArguments ExtractKeyValuePairs::parseArguments(const ColumnsWithTypeAndName & arguments)
{
    if (arguments.empty())
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function " + String(name) + "requires at least one argument");
    }

    std::unordered_set<char> value_special_characters_allow_list;

    auto data_column = arguments[0].column;

    if (arguments.size() == 1u)
    {
        return ParsedArguments{data_column, {}, {}, {}, {}, value_special_characters_allow_list};
    }

    auto escape_character = extractControlCharacter(arguments[1].column);

    if (arguments.size() == 2u)
    {
        return ParsedArguments{data_column, escape_character, {}, {}, {}, value_special_characters_allow_list};
    }

    auto key_value_pair_delimiter = extractControlCharacter(arguments[2].column);

    if (arguments.size() == 3u)
    {
        return ParsedArguments{data_column, escape_character, key_value_pair_delimiter, {}, {}, value_special_characters_allow_list};
    }

    auto item_delimiter = extractControlCharacter(arguments[3].column);

    if (arguments.size() == 4u)
    {
        return ParsedArguments{
            data_column, escape_character, key_value_pair_delimiter, item_delimiter, {}, value_special_characters_allow_list};
    }

    auto enclosing_character = extractControlCharacter(arguments[4].column);

    if (arguments.size() == 5u)
    {
        return ParsedArguments{
            data_column,
            escape_character,
            key_value_pair_delimiter,
            item_delimiter,
            enclosing_character,
            value_special_characters_allow_list};
    }

    bool ch_inline = false;

    if (arguments.size() == 7u)
    {
        ch_inline = true;
    }

    auto value_special_characters_allow_list_characters = arguments[5].column->getDataAt(0).toView();

    value_special_characters_allow_list.insert(
        value_special_characters_allow_list_characters.begin(), value_special_characters_allow_list_characters.end());

    return ParsedArguments{
        data_column, escape_character, key_value_pair_delimiter, item_delimiter, enclosing_character, value_special_characters_allow_list,
        ch_inline
    };
}

char ExtractKeyValuePairs::extractControlCharacter(ColumnPtr column)
{
    auto view = column->getDataAt(0).toView();

    if (view.size() != 1u)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Control character argument must contain exactly 1 character");
    }

    return view.front();
}

std::shared_ptr<KeyValuePairExtractor<ExtractKeyValuePairs::EscapingProcessorOutput>> ExtractKeyValuePairs::getExtractor(
    CharArgument escape_character,
    CharArgument key_value_pair_delimiter,
    CharArgument item_delimiter,
    CharArgument enclosing_character)
{
    auto builder = KeyValuePairExtractorBuilder();

    if (escape_character)
    {
        builder.withEscapeCharacter(escape_character.value());
    }

    if (key_value_pair_delimiter)
    {
        builder.withKeyValuePairDelimiter(key_value_pair_delimiter.value());
    }

    if (item_delimiter)
    {
        builder.withItemDelimiter(item_delimiter.value());
    }

    if (enclosing_character)
    {
        builder.withEnclosingCharacter(enclosing_character.value());
    }

    return builder.build();
}

std::shared_ptr<KeyValuePairExtractor<std::unordered_map<std::string, std::string>>> ExtractKeyValuePairs::getExtractor2(
    CharArgument escape_character,
    CharArgument key_value_pair_delimiter,
    CharArgument item_delimiter,
    CharArgument enclosing_character,
    SetArgument)
{
    InlineEscapingKeyStateHandler key_state_handler(*key_value_pair_delimiter, *escape_character, enclosing_character);
    InlineEscapingValueStateHandler value_state_handler(*escape_character, *item_delimiter, enclosing_character);

    return std::make_shared<InlineKeyValuePairExtractor>(key_state_handler, value_state_handler);
//    auto builder = KeyValuePairExtractorBuilder<ExtractKeyValuePairs::EscapingProcessorOutput>();
//
//    if (escape_character)
//    {
//        builder.withEscapeCharacter(escape_character.value());
//    }
//
//    if (key_value_pair_delimiter)
//    {
//        builder.withKeyValuePairDelimiter(key_value_pair_delimiter.value());
//    }
//
//    if (item_delimiter)
//    {
//        builder.withItemDelimiter(item_delimiter.value());
//    }
//
//    if (enclosing_character)
//    {
//        builder.withEnclosingCharacter(enclosing_character.value());
//    }
//
//    builder.withEscapingProcessor<NoOpEscapingProcessor>();
//
//    builder.withValueSpecialCharacterAllowList(value_special_characters_allow_list);
//
//    return builder.build();
}

ExtractKeyValuePairs::RawColumns ExtractKeyValuePairs::extract(std::shared_ptr<KeyValuePairExtractor<EscapingProcessorOutput>> extractor, ColumnPtr data_column)
{
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::microseconds;

    auto offsets = ColumnUInt64::create();

    auto keys = ColumnString::create();
    auto values = ColumnString::create();

//    keys->reserve(data_column->byteSize());
//    values->reserve(data_column->byteSize());

    auto row_offset = 0u;

//    long totalExtractionTime = 0;
//    long shortestExtractionTime = std::numeric_limits<long>::max();
//    long longestExtractionTime = std::numeric_limits<long>::min();
//
//    long totalInsertionTime = 0;
//    long shortestInsertionTime = std::numeric_limits<long>::max();
//    long longestInsertionTime = std::numeric_limits<long>::min();


    for (auto i = 0u; i < data_column->size(); i++)
    {
        auto row = data_column->getDataAt(i).toView();
        //        auto t2 = high_resolution_clock::now();

        auto response = extractor->extract(row);
        //        auto t3 = high_resolution_clock::now();

        for (auto & [key, value] : response)
        {
            keys->insert(key);
            values->insert(value);

            row_offset++;
        }

        offsets->insert(row_offset);
    }

    return {std::move(keys), std::move(values), std::move(offsets)};
}

ColumnPtr ExtractKeyValuePairs::escape(RawColumns & raw_columns, char escape_character)
{
    auto & [raw_keys, raw_values, offsets] = raw_columns;

    auto escaped_keys = ColumnString::create();
    auto escaped_values = ColumnString::create();

    escaped_keys->reserve(raw_keys->size());
    escaped_values->reserve(raw_values->size());

    auto escape_character_string = std::string(1, escape_character);

    using ReplaceString = ReplaceStringImpl<ReplaceStringTraits::Replace::All>;

    ReplaceString::vector(raw_keys->getChars(), raw_keys->getOffsets(), escape_character_string, "", escaped_keys->getChars(), escaped_keys->getOffsets());
    ReplaceString::vector(raw_values->getChars(), raw_values->getOffsets(), escape_character_string, "", escaped_values->getChars(), escaped_values->getOffsets());

    ColumnPtr keys_ptr = std::move(escaped_keys);

    return ColumnMap::create(keys_ptr, std::move(escaped_values), std::move(offsets));
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
            Special characters (e.g. $!@#¨) must be escaped. Enclosed/ quoted keys and values are accepted.

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
            select extractKeyValuePairs('9 ads =nm, no\:me: neymar, age: 30, daojmskdpoa and a height: 1.75, school: lupe\ picasso, team: psg,', '\\', ':', ',', '"', '.');
            ```

            Result:

            ``` text
            ┌─extractKeyValuePairs('9 ads =nm, no\\:me: neymar, age: 30, daojmskdpoa and a height: 1.75, school: lupe\\ picasso, team: psg,', '\\', ':', ',', '"', '.')─┐
            │ {'no:me':'neymar','age':'30','height':'1.75','school':'lupe picasso','team':'psg'}                                                                            │
            └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            ```)")
        );
}

}
