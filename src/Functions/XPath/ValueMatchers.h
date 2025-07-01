#pragma once

#include "Types.h"

namespace DB
{

class AlwaysTrueMatcher : public IValueMatcher
{
private:
    std::string_view pattern;

public:
    bool match(std::string_view) const override { return true; }
};

class ExactMatcher : public IValueMatcher
{
private:
    std::string_view pattern;

public:
    explicit ExactMatcher(std::string_view pattern_)
        : pattern(pattern_)
    {
    }
    bool match(std::string_view value) const override { return value == pattern; }
};

inline ValueMatcherPtr MakeAlwaysTrueMatcher()
{
    return std::make_shared<AlwaysTrueMatcher>();
}

inline ValueMatcherPtr MakeExactMatcher(std::string_view pattern)
{
    return std::make_shared<ExactMatcher>(pattern);
}

}
