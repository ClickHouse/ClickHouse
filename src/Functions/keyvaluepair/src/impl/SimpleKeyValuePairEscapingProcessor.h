#pragma once

#include <string>
#include <unordered_map>

#include "../KeyValuePairEscapingProcessor.h"

namespace DB
{

class SimpleKeyValuePairEscapingProcessor : public KeyValuePairEscapingProcessor<std::unordered_map<std::string, std::string>>
{
public:
    explicit SimpleKeyValuePairEscapingProcessor(char escape_character);

    [[nodiscard]] Response process(const ResponseViews & input) const override;

private:
    [[maybe_unused]] const char escape_character;

    [[nodiscard]] std::string escape(std::string_view element_view) const;
};

}
