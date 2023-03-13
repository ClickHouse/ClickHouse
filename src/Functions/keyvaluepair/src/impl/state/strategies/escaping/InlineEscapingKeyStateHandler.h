#pragma once

#include <optional>
#include <string>
#include "Functions/keyvaluepair/src/impl/state/ExtractorConfiguration.h"
#include "Functions/keyvaluepair/src/impl/state/StateHandler.h"

namespace DB
{

class InlineEscapingKeyStateHandler : public StateHandler
{
    struct RuntimeConfiguration
    {
        RuntimeConfiguration() = default;

        RuntimeConfiguration(
            std::vector<char> wait_configuration_,
            std::vector<char> read_configuration_,
            std::vector<char> read_enclosed_configuration_
        ) : wait_configuration(std::move(wait_configuration_)),
            read_configuration(std::move(read_configuration_)),
            read_enclosed_configuration(std::move(read_enclosed_configuration_)) {}

        std::vector<char> wait_configuration;
        std::vector<char> read_configuration;
        std::vector<char> read_enclosed_configuration;
    };

public:
    using ElementType = std::string;

    explicit InlineEscapingKeyStateHandler(ExtractorConfiguration configuration_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const;

private:
    ExtractorConfiguration extractor_configuration;
    RuntimeConfiguration runtime_configuration;
};

}
