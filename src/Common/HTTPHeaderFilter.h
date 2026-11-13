#pragma once

#include <IO/HTTPHeaderEntries.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <memory>
#include <vector>
#include <unordered_set>
#include <mutex>

namespace re2 { class RE2; }

namespace DB
{

class HTTPHeaderFilter
{
public:

    void setValuesFromConfig(const Poco::Util::AbstractConfiguration & config);
    void checkAndNormalizeHeaders(HTTPHeaderEntries & entries) const;

private:
    /// Header names are case-insensitive (RFC 7230 3.2): entries are stored
    /// lower-cased and the incoming name is lower-cased before lookup.
    std::unordered_set<std::string> forbidden_headers;
    /// Pre-compiled once with case-insensitive matching (compiling per check would also be case-sensitive).
    std::vector<std::shared_ptr<const re2::RE2>> forbidden_headers_regexp;

    mutable std::mutex mutex;
};

}
