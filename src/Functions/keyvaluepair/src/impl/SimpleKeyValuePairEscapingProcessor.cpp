#include "SimpleKeyValuePairEscapingProcessor.h"

namespace DB
{

SimpleKeyValuePairEscapingProcessor::SimpleKeyValuePairEscapingProcessor(char escape_character_)
: KeyValuePairEscapingProcessor<std::unordered_map<std::string, std::string>>(), escape_character(escape_character_)
{}

SimpleKeyValuePairEscapingProcessor::Response SimpleKeyValuePairEscapingProcessor::process(const ResponseViews & response_views) const {
    Response response;

    response.reserve(response_views.size());

    for (auto [key_view, value_view] : response_views) {
        response[escape(key_view)] = escape(value_view);
    }

    return response;
}

std::string SimpleKeyValuePairEscapingProcessor::escape(std::string_view element_view) const {
    [[maybe_unused]] bool escape = false;
    std::string element;

    element.reserve(element_view.size());

    for (char character : element_view) {
        if (escape) {
            escape = false;
        } else if (character == escape_character) {
            escape = true;
            continue;
        }

        element.push_back(character);
    }

    return element;
}

}
