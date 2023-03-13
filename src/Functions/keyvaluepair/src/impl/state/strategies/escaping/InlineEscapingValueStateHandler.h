#pragma once

#include <string>
#include <unordered_set>
#include "Functions/keyvaluepair/src/impl/state/ExtractorConfiguration.h"
#include "Functions/keyvaluepair/src/impl/state/State.h"
#include "Functions/keyvaluepair/src/impl/state/StateHandler.h"

namespace DB
{

class InlineEscapingValueStateHandler : public StateHandler
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

    explicit InlineEscapingValueStateHandler(ExtractorConfiguration extractor_configuration_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & value) const;

    [[nodiscard]] NextState readEnclosed(std::string_view file, size_t pos, ElementType & value) const;

    [[nodiscard]] static NextState readEmpty(std::string_view, size_t pos, ElementType & value);

private:
    ExtractorConfiguration extractor_configuration;
    RuntimeConfiguration runtime_configuration;
};

}
