#pragma once

#include <memory>
#include <optional>
#include <unordered_set>
#include <Functions/keyvaluepair/src/impl/LazyEscapingKeyValuePairExtractor.h>
#include <Functions/keyvaluepair/src/impl/SimpleKeyValuePairEscapingProcessor.h>
#include <Functions/keyvaluepair/src/impl/state/KeyStateHandler.h>
#include <Functions/keyvaluepair/src/impl/state/ValueStateHandler.h>
#include "KeyValuePairExtractor.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Response = std::unordered_map<std::string, std::string>>
class KeyValuePairExtractorBuilder
{
public:
    KeyValuePairExtractorBuilder & withKeyValuePairDelimiter(char key_value_pair_delimiter_)
    {
        key_value_pair_delimiter = key_value_pair_delimiter_;
        return *this;
    }

    KeyValuePairExtractorBuilder & withEscapeCharacter(char escape_character_)
    {
        escape_character = escape_character_;
        return *this;
    }

    KeyValuePairExtractorBuilder & withItemDelimiter(char item_delimiter_)
    {
        item_delimiter = item_delimiter_;
        return *this;
    }

    KeyValuePairExtractorBuilder & withEnclosingCharacter(std::optional<char> enclosing_character_)
    {
        enclosing_character = enclosing_character_;
        return *this;
    }

    KeyValuePairExtractorBuilder & withValueSpecialCharacterAllowList(std::unordered_set<char> value_special_character_allowlist_)
    {
        value_special_character_allowlist = std::move(value_special_character_allowlist_);
        return *this;
    }

    template <typename EscapingProcessor>
    KeyValuePairExtractorBuilder & withEscapingProcessor()
    {
        escaping_processor = std::make_shared<EscapingProcessor>(escape_character);
        return *this;
    }

    std::shared_ptr<KeyValuePairExtractor<Response>> build()
    {
        KeyStateHandler key_state_handler(key_value_pair_delimiter, escape_character, enclosing_character);
        ValueStateHandler value_state_handler(escape_character, item_delimiter, enclosing_character, value_special_character_allowlist);

        if (!escaping_processor)
        {
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Escaping processor must be set, cannot build KeyValuePairExtractor without one");
        }

        return std::make_shared<LazyEscapingKeyValuePairExtractor<Response>>(key_state_handler, value_state_handler, escaping_processor);
    }

private:
    char key_value_pair_delimiter = ':';
    char escape_character = '\\';
    char item_delimiter = ',';
    std::optional<char> enclosing_character;
    std::unordered_set<char> value_special_character_allowlist;
    std::shared_ptr<KeyValuePairEscapingProcessor<Response>> escaping_processor;
};

}
