#pragma once

#include <string>
#include <unordered_set>
#include "Functions/keyvaluepair/src/impl/state/Configuration.h"
#include "Functions/keyvaluepair/src/impl/state/State.h"
#include "Functions/keyvaluepair/src/impl/state/StateHandler.h"

namespace DB
{

class InlineEscapingKeyStateHandler : public StateHandler
{
public:
    using ElementType = std::string;

    explicit InlineEscapingKeyStateHandler(Configuration configuration_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readQuoted(std::string_view file, size_t pos, ElementType & key) const;

    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file, size_t pos) const;

private:
    Configuration extractor_configuration;

    std::vector<char> wait_needles;
    std::vector<char> read_needles;
    std::vector<char> read_quoted_needles;
};


class InlineEscapingValueStateHandler : public StateHandler
{
public:
    using ElementType = std::string;

    explicit InlineEscapingValueStateHandler(Configuration extractor_configuration_);

    [[nodiscard]] NextState wait(std::string_view file, size_t pos) const;

    [[nodiscard]] NextState read(std::string_view file, size_t pos, ElementType & value) const;

    [[nodiscard]] NextState readQuoted(std::string_view file, size_t pos, ElementType & value) const;

private:
    Configuration extractor_configuration;
    std::vector<char> read_needles;
    std::vector<char> read_quoted_needles;
};

}
