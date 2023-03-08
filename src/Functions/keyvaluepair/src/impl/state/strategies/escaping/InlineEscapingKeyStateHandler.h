#pragma once

#include <optional>
#include <string>
#include "Functions/keyvaluepair/src/impl/state/ExtractorConfiguration.h"
#include "Functions/keyvaluepair/src/impl/state/StateHandler.h"

namespace DB
{

class InlineEscapingKeyStateHandler : public StateHandler
{
public:
    using ElementType = std::string;

    explicit InlineEscapingKeyStateHandler(ExtractorConfiguration configuration_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const;

private:
    ExtractorConfiguration extractor_configuration;
};

}
