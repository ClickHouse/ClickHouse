#pragma once

#include "Functions/keyvaluepair/src/impl/state/ExtractorConfiguration.h"
#include "Functions/keyvaluepair/src/impl/state/StateHandler.h"

namespace DB
{

class NoEscapingKeyStateHandler : public StateHandler
{
public:
    using ElementType = std::string_view;

    explicit NoEscapingKeyStateHandler(ExtractorConfiguration extractor_configuration_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const;

private:
    ExtractorConfiguration extractor_configuration;

    std::vector<char> wait_needles;
    std::vector<char> read_needles;
    std::vector<char> read_quoted_needles;
};

}
