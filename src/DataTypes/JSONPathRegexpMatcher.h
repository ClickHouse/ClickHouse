#pragma once

#include <base/types.h>

#include <compare>
#include <memory>
#include <string_view>
#include <vector>

namespace DB
{

enum class JSONPathRegexpMatchMode : UInt8
{
    Partial = 0,
    Full = 1,
};

struct JSONPathRegexpRule
{
    String pattern;
    JSONPathRegexpMatchMode mode = JSONPathRegexpMatchMode::Partial;

    auto operator<=>(const JSONPathRegexpRule &) const = default;
};

/// An immutable, shareable collection of regular expressions used to classify JSON paths.
/// Rules are validated, sorted and deduplicated by `create`. Compiled `RE2` objects are hidden
/// behind a pImpl so users of this header don't transitively include `RE2`.
class JSONPathRegexpMatcher final
{
public:
    using Ptr = std::shared_ptr<const JSONPathRegexpMatcher>;

    static constexpr size_t MAX_RULES = 256;
    static constexpr size_t MAX_PATTERN_BYTES = 64 * 1024;
    static constexpr size_t MAX_TOTAL_PATTERN_BYTES = 1024 * 1024;

    static Ptr create(std::vector<JSONPathRegexpRule> rules);

    ~JSONPathRegexpMatcher();

    JSONPathRegexpMatcher(const JSONPathRegexpMatcher &) = delete;
    JSONPathRegexpMatcher & operator=(const JSONPathRegexpMatcher &) = delete;
    JSONPathRegexpMatcher(JSONPathRegexpMatcher &&) = delete;
    JSONPathRegexpMatcher & operator=(JSONPathRegexpMatcher &&) = delete;

    bool matches(std::string_view path) const;

    const std::vector<JSONPathRegexpRule> & getRules() const { return rules; }
    bool empty() const { return rules.empty(); }

    /// Returns the matcher object, canonical rule storage and the known compiled matcher storage.
    /// `RE2`'s lazily-created DFA cache is not exposed by `RE2`; its real allocations are nevertheless
    /// accounted by ClickHouse's global new/delete memory-tracking interceptors and bounded by the
    /// `RE2` `max_mem` option configured in the implementation.
    size_t allocatedBytes() const;

private:
    struct Impl;

    explicit JSONPathRegexpMatcher(std::vector<JSONPathRegexpRule> rules_);

    std::vector<JSONPathRegexpRule> rules;
    std::unique_ptr<Impl> impl;
};

using JSONPathRegexpMatcherPtr = JSONPathRegexpMatcher::Ptr;

}
