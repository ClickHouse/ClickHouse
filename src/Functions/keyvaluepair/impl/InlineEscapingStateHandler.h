#pragma once

#include <Functions/keyvaluepair/impl/Configuration.h>
#include <Functions/keyvaluepair/impl/StateHandler.h>

#include <string_view>
#include <string>
#include <vector>

namespace DB
{

namespace extractKV
{

class InlineEscapingStateHandler : public StateHandler
{
public:
    using KeyType = std::string;
    using ValueType = std::string;

    explicit InlineEscapingStateHandler(Configuration configuration_);

    [[nodiscard]] NextState waitKey(std::string_view file) const;
    [[nodiscard]] NextState readKey(std::string_view file, KeyType & key) const;
    [[nodiscard]] NextState readQuotedKey(std::string_view file, KeyType & key) const;
    [[nodiscard]] NextState readKeyValueDelimiter(std::string_view file) const;
    [[nodiscard]] NextState waitValue(std::string_view file) const;
    [[nodiscard]] NextState readValue(std::string_view file, ValueType & value) const;
    [[nodiscard]] NextState readQuotedValue(std::string_view file, ValueType & value) const;

    const Configuration extractor_configuration;

private:
    std::vector<char> wait_needles;
    std::vector<char> read_needles;
    std::vector<char> read_quoted_needles;
};

}

}
