#pragma once

#include <iostream>

#include <Functions/XPath/Types.h>

namespace DB
{

struct AttributeMatcher
{
    CaseInsensitiveStringView key;
    ValueMatcherPtr value_matcher;

    AttributeMatcher(const String & key_, ValueMatcherPtr value_matcher_)
        : key(key_)
        , value_matcher(std::move(value_matcher_))
    {
    }

    bool match(const Attribute & attribute) const
    {
        if (key != attribute.key)
        {
            return false;
        }

        return value_matcher->match(attribute.value);
    }
};

struct TagMatcher
{
    CaseInsensitiveStringView key;
    std::vector<AttributeMatcher> attribute_matchers;
    bool skip_ancestors = false;

    bool matchTag(CaseInsensitiveStringView tag_key, size_t tag_depth, size_t matcher_index) const
    {
        if (!skip_ancestors && tag_depth != matcher_index)
        {
            return false;
        }

        return tag_key == key;
    }

    bool matchAttributes(const std::vector<Attribute> & attributes) const
    {
        for (const auto & matcher : attribute_matchers)
        {
            bool is_matched = false;

            for (const auto & attribute : attributes)
            {
                if (matcher.match(attribute))
                {
                    is_matched = true;
                    break;
                }
            }

            if (!is_matched)
            {
                return false;
            }
        }

        return true;
    }
};

}
