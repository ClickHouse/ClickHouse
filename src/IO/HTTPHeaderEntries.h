#pragma once

#include <string>
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
/// The S3 client sorts a header by the literal prefix `x-amz-`: once before signing, to pick what
/// joins the canonical list, and once after, to pick what is attached as an ordinary header. A name
/// in the spelling an operator wrote, such as `X-Amz-Meta-Owner`, fails both tests and leaves
/// unsigned and untranslated.
///
/// The conversion cannot happen any earlier than the client. `HTTPHeaderFilter` matches
/// `<http_forbid_headers>` regexps against the original case, so that an inline `(?-i)` scope keeps
/// working, and the refresh of a GCS token finds the header to replace by comparing the name to
/// `Authorization`. Both run on the way here. Holding the rule in the type is what keeps the last
/// step from being skipped.
class NormalizedHTTPHeaderEntries
{
public:
    using const_iterator = HTTPHeaderEntries::const_iterator;

    NormalizedHTTPHeaderEntries() = default;
    explicit NormalizedHTTPHeaderEntries(const HTTPHeaderEntries & headers);

    void push_back(HTTPHeaderEntry entry); /// NOLINT

    const_iterator begin() const { return entries.begin(); }
    const_iterator end() const { return entries.end(); }

private:
    HTTPHeaderEntries entries;
};

}
