#include "parseKeyValue.h"

#include <Columns/ColumnMap.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Common/assert_cast.h>
#include <Functions/keyvaluepair/KeyValuePairExtractorBuilder.h>

namespace DB
{

ParseKeyValue::ParseKeyValue()
: return_type(std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()))
{
}

String ParseKeyValue::getName() const
{
    return name;
}

ColumnPtr ParseKeyValue::executeImpl([[maybe_unused]] const ColumnsWithTypeAndName & arguments, [[maybe_unused]] const DataTypePtr & result_type, [[maybe_unused]] size_t input_rows_count) const
{
    auto column = return_type->createColumn();
    [[maybe_unused]] auto * map_column = assert_cast<ColumnMap *>(column.get());

    auto [data_column, escape_character, key_value_pair_delimiter, item_delimiter, enclosing_character, value_special_characters_allow_list] = parseArguments(arguments);

    for (auto i = 0u; i < data_column->size(); i++)
    {
        auto row = data_column->getDataAt(i);

        auto extractor = getExtractor(escape_character, key_value_pair_delimiter, item_delimiter, enclosing_character, value_special_characters_allow_list);

        auto response = extractor->extract(row.toString());

        for (auto & pair : response) {
            std::cout<<pair.first<<": "<<pair.second<<"\n";
        }
    }



    ColumnUInt64::MutablePtr offsets = ColumnUInt64::create();

    [[maybe_unused]] auto keys = ColumnString::create();
    [[maybe_unused]] auto values = ColumnString::create();

    return nullptr;
}

bool ParseKeyValue::isVariadic() const
{
    return true;
}

bool ParseKeyValue::isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const
{
    return false;
}

size_t ParseKeyValue::getNumberOfArguments() const
{
    return 0u;
}

DataTypePtr ParseKeyValue::getReturnTypeImpl(const DataTypes & /*arguments*/) const
{
    return return_type;
}

ParseKeyValue::ParsedArguments ParseKeyValue::parseArguments(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.empty()) {
        // throw exception
        return {};
    }

    auto data_column = arguments[0].column;

    if (arguments.size() == 1u)
    {
        return ParsedArguments {
            data_column,
            {},
            {},
            {},
            {},
            {}
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
            {}
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
            {}
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
            {}
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
            {}
        };
    }

    return ParsedArguments {
        data_column,
        escape_character,
        key_value_pair_delimiter,
        item_delimiter,
        enclosing_character,
        {}
    };
}

std::shared_ptr<KeyValuePairExtractor> ParseKeyValue::getExtractor(
    CharArgument escape_character, CharArgument key_value_pair_delimiter, CharArgument item_delimiter,
    CharArgument enclosing_character, SetArgument value_special_characters_allow_list) const
{
    auto builder = KeyValuePairExtractorBuilder();

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

    builder.withValueSpecialCharacterAllowList(value_special_characters_allow_list);

    return builder.build();
}

REGISTER_FUNCTION(ParseKeyValue)
{
    factory.registerFunction<ParseKeyValue>();
}

}
