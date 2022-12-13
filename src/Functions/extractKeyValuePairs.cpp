#include "extractKeyValuePairs.h"

#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Common/assert_cast.h>
#include <Functions/keyvaluepair/src/KeyValuePairExtractorBuilder.h>
#include <Functions/ReplaceStringImpl.h>

namespace DB
{

/*
 * In order to leverage DB::ReplaceStringImpl for a better performance, the default escaping processor needs
 * to be overriden by a no-op escaping processor. DB::ReplaceStringImpl does in-place replacing and leverages the
 * Volnitsky searcher.
 * */
struct NoOpEscapingProcessor : KeyValuePairEscapingProcessor<ExtractKeyValuePairs::EscapingProcessorOutput>
{
    explicit NoOpEscapingProcessor(char) {}

    Response process(const ResponseViews & response_views) const override
    {
        return response_views;
    }
};

ExtractKeyValuePairs::ExtractKeyValuePairs()
: return_type(std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()))
{
}

String ExtractKeyValuePairs::getName() const
{
    return name;
}

ColumnPtr ExtractKeyValuePairs::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    auto [data_column, escape_character, key_value_pair_delimiter, item_delimiter, enclosing_character, value_special_characters_allow_list] = parseArguments(arguments);

    auto extractor = getExtractor(escape_character, key_value_pair_delimiter, item_delimiter, enclosing_character, value_special_characters_allow_list);

    auto raw_columns = extract(extractor, data_column);

    return escape(raw_columns);
}

bool ExtractKeyValuePairs::isVariadic() const
{
    return true;
}

bool ExtractKeyValuePairs::isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const
{
    return false;
}

size_t ExtractKeyValuePairs::getNumberOfArguments() const
{
    return 0u;
}

DataTypePtr ExtractKeyValuePairs::getReturnTypeImpl(const DataTypes & /*arguments*/) const
{
    return return_type;
}

ExtractKeyValuePairs::ParsedArguments ExtractKeyValuePairs::parseArguments(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.empty()) {
        // throw exception
        return {};
    }

    std::unordered_set<char> value_special_characters_allow_list;

    auto data_column = arguments[0].column;

    if (arguments.size() == 1u)
    {
        return ParsedArguments {
            data_column,
            {},
            {},
            {},
            {},
            value_special_characters_allow_list
        };
    }

    auto escape_character = arguments[1].column->getDataAt(0).toView().front();

    if (arguments.size() == 2u)
    {
        return ParsedArguments {
            data_column,
            escape_character,
            {},
            {},
            {},
            value_special_characters_allow_list
        };
    }

    auto key_value_pair_delimiter = arguments[2].column->getDataAt(0).toView().front();

    if (arguments.size() == 3u)
    {
        return ParsedArguments {
            data_column,
            escape_character,
            key_value_pair_delimiter,
            {},
            {},
            value_special_characters_allow_list
        };
    }

    auto item_delimiter = arguments[3].column->getDataAt(0).toView().front();

    if (arguments.size() == 4u)
    {
        return ParsedArguments {
            data_column,
            escape_character,
            key_value_pair_delimiter,
            item_delimiter,
            {},
            value_special_characters_allow_list
        };
    }

    auto enclosing_character = arguments[4].column->getDataAt(0).toView().front();

    if (arguments.size() == 5u)
    {
        return ParsedArguments {
            data_column,
            escape_character,
            key_value_pair_delimiter,
            item_delimiter,
            enclosing_character,
            value_special_characters_allow_list
        };
    }

    auto value_special_characters_allow_list_characters = arguments[5].column->getDataAt(0).toView();

    value_special_characters_allow_list.insert(value_special_characters_allow_list_characters.begin(), value_special_characters_allow_list_characters.end());

    return ParsedArguments {
        data_column,
        escape_character,
        key_value_pair_delimiter,
        item_delimiter,
        enclosing_character,
        value_special_characters_allow_list
    };
}

std::shared_ptr<KeyValuePairExtractor<ExtractKeyValuePairs::EscapingProcessorOutput>> ExtractKeyValuePairs::getExtractor(
    CharArgument escape_character, CharArgument key_value_pair_delimiter, CharArgument item_delimiter,
    CharArgument enclosing_character, SetArgument value_special_characters_allow_list) const
{
    auto builder = KeyValuePairExtractorBuilder<ExtractKeyValuePairs::EscapingProcessorOutput>();

    if (escape_character) {
        builder.withEscapeCharacter(escape_character.value());
    }

    if (key_value_pair_delimiter) {
        builder.withKeyValuePairDelimiter(key_value_pair_delimiter.value());
    }

    if (item_delimiter) {
        builder.withItemDelimiter(item_delimiter.value());
    }

    if (enclosing_character) {
        builder.withEnclosingCharacter(enclosing_character.value());
    }

    builder.withEscapingProcessor<NoOpEscapingProcessor>();

    builder.withValueSpecialCharacterAllowList(value_special_characters_allow_list);

    return builder.build();
}

ExtractKeyValuePairs::RawColumns ExtractKeyValuePairs::extract(std::shared_ptr<KeyValuePairExtractor<EscapingProcessorOutput>> extractor, ColumnPtr data_column) const
{
    auto offsets = ColumnUInt64::create();

    auto keys = ColumnString::create();
    auto values = ColumnString::create();

    auto row_offset = 0u;

    for (auto i = 0u; i < data_column->size(); i++)
    {
        auto row = data_column->getDataAt(i).toString();

        // TODO avoid copying
        auto response = extractor->extract(row);

        for (auto [key, value] : response)
        {
            keys->insert(key);
            values->insert(value);

            row_offset++;
        }

        offsets->insert(row_offset);
    }

    return {
        std::move(keys),
        std::move(values),
        std::move(offsets)
    };
}

ColumnPtr ExtractKeyValuePairs::escape(RawColumns & raw_columns) const
{
    auto & [raw_keys, raw_values, offsets] = raw_columns;

    auto escaped_keys = ColumnString::create();
    auto escaped_values = ColumnString::create();

    ReplaceStringImpl<ReplaceStringTraits::Replace::All>::vector(raw_keys->getChars(), raw_keys->getOffsets(), "\\", "", escaped_keys->getChars(), escaped_keys->getOffsets());
    ReplaceStringImpl<ReplaceStringTraits::Replace::All>::vector(raw_values->getChars(), raw_values->getOffsets(), "\\", "", escaped_values->getChars(), escaped_values->getOffsets());

    ColumnPtr keys_ptr = std::move(escaped_keys);

    return ColumnMap::create(keys_ptr, std::move(escaped_values), std::move(offsets));
}

REGISTER_FUNCTION(ExtractKeyValuePairs)
{
    factory.registerFunction<ExtractKeyValuePairs>();
}

}
