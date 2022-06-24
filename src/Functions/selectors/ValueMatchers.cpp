#include "Types.h"
#include "ValueMatchers.h"

#include <Common/StringUtils/StringUtils.h>

namespace DB
{

namespace
{

class AlwaysTrueMatcher : public IValueMatcher
{
private:
    std::string_view pattern;

public:
    bool match(std::string_view) const override { return true; }
    std::string_view getPattern() const override { return ""; }
};

class ExactMatcher : public IValueMatcher
{
private:
    std::string_view pattern;

public:
    explicit ExactMatcher(std::string_view pattern_) : pattern(pattern_) { }
    bool match(std::string_view value) const override { return value == pattern; }
    std::string_view getPattern() const override { return pattern; }
};

class StartsWithMatcher : public IValueMatcher
{
private:
    std::string_view pattern;

public:
    explicit StartsWithMatcher(std::string_view pattern_) : pattern(pattern_) { }
    bool match(std::string_view value) const override { return value.starts_with(pattern); }
    std::string_view getPattern() const override { return pattern; }
};

class EndsWithMatcher : public IValueMatcher
{
private:
    std::string_view pattern;

public:
    explicit EndsWithMatcher(std::string_view pattern_) : pattern(pattern_) { }
    bool match(std::string_view value) const override { return value.ends_with(pattern); }
    std::string_view getPattern() const override { return pattern; }
};

class ContainsWordMatcher : public IValueMatcher
{
private:
    std::string_view pattern;

public:
    explicit ContainsWordMatcher(std::string_view pattern_) : pattern(pattern_) { }
    bool match(std::string_view value) const override
    {
        const char * end = value.end();
        const char * word_start = find_first_not_symbols<' ', '\t'>(value.begin(), end);
        while (word_start != end)
        {
            const char * word_end = find_first_symbols<' ', '\t'>(word_start, end);

            if (std::string_view(word_start, word_end - word_start) == pattern)
                return true;

            word_start = find_first_not_symbols<' ', '\t'>(word_end, end);
        }
        return false;
    }
    std::string_view getPattern() const override { return pattern; }
};

class SubstringMatcher : public IValueMatcher
{
private:
    std::string_view pattern;

public:
    explicit SubstringMatcher(std::string_view pattern_) : pattern(pattern_) { }
    bool match(std::string_view value) const override { return value.find(pattern) != std::string::npos; }
    std::string_view getPattern() const override { return pattern; }
};

}

ValueMatcherPtr MakeAlwaysTrueMatcher()
{
    return std::make_unique<AlwaysTrueMatcher>();
}

ValueMatcherPtr MakeExactMatcher(std::string_view pattern)
{
    return std::make_unique<ExactMatcher>(pattern);
}

ValueMatcherPtr MakeStartsWithMatcher(std::string_view pattern)
{
    return std::make_unique<StartsWithMatcher>(pattern);
}

ValueMatcherPtr MakeEndsWithMatcher(std::string_view pattern)
{
    return std::make_unique<EndsWithMatcher>(pattern);
}

ValueMatcherPtr MakeContainsWordMatcher(std::string_view pattern)
{
    return std::make_unique<ContainsWordMatcher>(pattern);
}

ValueMatcherPtr MakeSubstringMatcher(std::string_view pattern)
{
    return std::make_unique<SubstringMatcher>(pattern);
}

}
