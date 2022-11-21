#pragma once

#include <string>
#include <unordered_map>

class KeyValuePairEscapingProcessor {
public:

    using Response = std::unordered_map<std::string, std::string>;
    using ResponseViews = std::unordered_map<std::string_view, std::string_view>;

    explicit KeyValuePairEscapingProcessor(char escape_character);

    [[nodiscard]] Response process(const ResponseViews & input) const;

private:
    const char escape_character;

    [[nodiscard]] std::string escape(std::string_view element_view) const;
};
