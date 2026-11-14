#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <base/types.h>
#include <Common/SecretString.h>

namespace DB
{

struct HTTPHeaderEntry
{
    String name;
    SecretString value;

    HTTPHeaderEntry(std::string_view name_, std::string_view value_) : name(name_), value(value_) {}
    bool operator==(const HTTPHeaderEntry & other) const = default;
};

using HTTPHeaderEntries = std::vector<HTTPHeaderEntry>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

/// Header names lower-cased on insertion. The S3 client picks the reserved `x-amz-` family out by
/// that literal lower-case prefix.
class NormalizedHTTPHeaderEntries
{
public:
    using const_iterator = HTTPHeaderEntries::const_iterator;

    NormalizedHTTPHeaderEntries() = default;
    explicit NormalizedHTTPHeaderEntries(const HTTPHeaderEntries & headers);

    void push_back(HTTPHeaderEntry entry); /// NOLINT
    void append(const HTTPHeaderEntries & headers);
    void append(const NormalizedHTTPHeaderEntries & headers);

    /// The name is normalized first, so the caller may spell it in any case.
    void eraseByName(std::string_view name);

    void clear() { entries.clear(); }
    bool empty() const { return entries.empty(); }
    size_t size() const { return entries.size(); }

    bool operator==(const NormalizedHTTPHeaderEntries & other) const { return entries == other.entries; }

    const_iterator begin() const { return entries.begin(); }
    const_iterator end() const { return entries.end(); }

private:
    /// `HTTPHeaderFilter` only validates these entries; it does not modify them, so the lower-case
    /// invariant holds.
    friend class HTTPHeaderFilter;

    HTTPHeaderEntries entries;
};

}
