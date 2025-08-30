#pragma once
#include <Core/Types.h>
#include <Common/FST.h>

namespace DB
{

class GinQueryString
{
public:
    GinQueryString() = default;
    GinQueryString(std::string_view query_string_, const std::vector<String> & search_terms_);

    /// Getter
    const String & getQueryString() const { return query_string; }
    const std::vector<String> & getTerms() const { return terms; }

    /// Set the query string of the filter
    void setQueryString(std::string_view query_string_)
    {
        query_string = query_string_;
    }

    /// Add term which are tokens generated from the query string
    bool addTerm(std::string_view term)
    {
        if (term.length() > FST::MAX_TERM_LENGTH) [[unlikely]]
            return false;

        terms.push_back(String(term));
        return true;
    }

private:
    /// Query string of the filter
    String query_string;
    /// Tokenized terms from query string
    std::vector<String> terms;
};

}
