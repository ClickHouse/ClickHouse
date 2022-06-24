#pragma once

#include <base/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>

#include <string>
#include <vector>

namespace DB
{

struct CaseInsensitiveStringView
{
    std::string_view value;

    CaseInsensitiveStringView() = default;
    CaseInsensitiveStringView(std::string_view value_) : value(value_) { }

    bool operator==(const CaseInsensitiveStringView & other) const
    {
        if (value.size() != other.value.size())
            return false;
        for (size_t i = 0; i < value.size(); ++i)
        {
            if (!equalsCaseInsensitive(value[i], other.value[i]))
                return false;
        }

        return true;
    }
};

struct TagPreview
{
    CaseInsensitiveStringView name;
    bool is_closing = false;

    bool operator==(const TagPreview & other) const = default;
};

struct Attribute
{
    CaseInsensitiveStringView key;
    std::string_view value;

    bool operator==(const Attribute & other) const = default;
};

class IValueMatcher
{
public:
    virtual bool match(std::string_view value) const = 0;
    virtual std::string_view getPattern() const = 0;
    virtual ~IValueMatcher() = default;
};

using ValueMatcherPtr = std::unique_ptr<IValueMatcher>;

struct AttributeSelector
{
    CaseInsensitiveStringView key;
    ValueMatcherPtr value_matcher;

    bool match(std::string_view value) const { return value_matcher->match(value); }
};

}
