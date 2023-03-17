#pragma once

#include <unordered_set>
#include "Functions/keyvaluepair/src/impl/state/Configuration.h"
#include "Functions/keyvaluepair/src/impl/state/StateHandler.h"

namespace DB
{

class NoEscapingValueStateHandler : public StateHandler
{
public:
    using ElementType = std::string_view;

    explicit NoEscapingValueStateHandler(Configuration extractor_configuration_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & value) const;

    [[nodiscard]] NextState readQuoted(std::string_view file, size_t pos, ElementType & value) const;

    [[nodiscard]] static NextState readEmpty(std::string_view, size_t pos, ElementType & value);

private:
    Configuration extractor_configuration;
    std::vector<char> read_needles;
    std::vector<char> read_quoted_needles;
};

}
