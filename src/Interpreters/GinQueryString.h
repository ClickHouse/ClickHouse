#pragma once
#include <Core/Types.h>
#include <Common/FST.h>

namespace DB
{

enum class TextSearchMode : uint8_t
{
    Any,
    All
};

class GinQueryString
{
public:
    GinQueryString() = default;
    GinQueryString(std::string_view query_string_, const std::vector<String> & tokens_);

    /// Getter
    const String & getQueryString() const { return query_string; }
    const std::vector<String> & getTokens() const { return tokens; }

    /// Set the query string of the filter
    void setQueryString(std::string_view query_string_)
    {
        query_string = query_string_;
    }

    /// Add term which are tokens generated from the query string
    bool addToken(std::string_view token)
    {
        if (token.length() > FST::MAX_TOKEN_LENGTH) [[unlikely]]
            return false;

        tokens.push_back(String(token));
        return true;
    }

private:
    /// Query string of the filter
    String query_string;
    /// Tokenized terms from query string
    std::vector<String> tokens;
};

}
