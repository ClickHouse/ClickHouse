#pragma once

#include "Functions/keyvaluepair/src/impl/state/Configuration.h"
#include "Functions/keyvaluepair/src/impl/state/StateHandler.h"

namespace DB
{

class NoEscapingKeyStateHandler : public StateHandler
{
public:
    using ElementType = std::string_view;

    explicit NoEscapingKeyStateHandler(Configuration extractor_configuration_);

    [[nodiscard]] NextState wait(std::string_view file) const;

    [[nodiscard]] NextState read(std::string_view file, ElementType & key) const;

    [[nodiscard]] NextState readQuoted(std::string_view file, ElementType & key) const;

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file) const;

private:
    Configuration extractor_configuration;

    std::vector<char> wait_needles;
    std::vector<char> read_needles;
    std::vector<char> read_quoted_needles;
};

}
