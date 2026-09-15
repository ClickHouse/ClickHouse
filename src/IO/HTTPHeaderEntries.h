#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace DB
{

struct HTTPHeaderEntry
{
    std::string name;
    std::string value;

    HTTPHeaderEntry(const std::string & name_, const std::string & value_) : name(name_), value(value_) {}
    bool operator==(const HTTPHeaderEntry & other) const { return name == other.name && value == other.value; }
};

using HTTPHeaderEntries = std::vector<HTTPHeaderEntry>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

/// Header names lower-cased on insertion.
///
/// S3 reserves the `x-amz-` family and expects it in lower case. The client picks those headers out
/// by that literal prefix, so a name spelled `X-Amz-Meta-Owner` is treated as an ordinary header and
/// is neither signed nor translated.
class NormalizedHTTPHeaderEntries
{
public:
    using const_iterator = HTTPHeaderEntries::const_iterator;

    NormalizedHTTPHeaderEntries() = default;
    explicit NormalizedHTTPHeaderEntries(const HTTPHeaderEntries & headers);

    void push_back(HTTPHeaderEntry entry); /// NOLINT
    void append(const HTTPHeaderEntries & headers);
    void append(const NormalizedHTTPHeaderEntries & headers);

    /// Remove every entry with this name. The name is normalized first, so the caller may spell it
    /// in any case.
    void eraseByName(std::string_view name);

    void clear() { entries.clear(); }
    bool empty() const { return entries.empty(); }
    size_t size() const { return entries.size(); }

    bool operator==(const NormalizedHTTPHeaderEntries & other) const { return entries == other.entries; }

    const_iterator begin() const { return entries.begin(); }
    const_iterator end() const { return entries.end(); }

private:
    /// `HTTPHeaderFilter` strips control characters from a name in place, then restores the
    /// invariant. It is the only code that edits an entry already held here.
    friend class HTTPHeaderFilter;

    HTTPHeaderEntries entries;
};

}
