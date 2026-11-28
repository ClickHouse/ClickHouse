#pragma once

#include <IO/HTTPHeaderEntries.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <memory>
#include <vector>
#include <unordered_set>
#include <mutex>

namespace re2 { class RE2; }
namespace Poco { class URI; }
namespace Poco::Net { class HTTPBasicCredentials; }

namespace DB
{

class HTTPHeaderFilter
{
public:

    void setValuesFromConfig(const Poco::Util::AbstractConfiguration & config);
    void checkAndNormalizeHeaders(HTTPHeaderEntries & entries) const;

    /// Throws if the `Authorization` header that sends these credentials is forbidden. An empty username sends no header.
    void checkBasicCredentials(const Poco::Net::HTTPBasicCredentials & credentials) const;
    /// The same for a sender that sends any non-empty userinfo of `uri` as Basic credentials, even without a username.
    void checkURIUserInfo(const Poco::URI & uri) const;

private:
    void checkAuthorizationHeader(const std::string & reason) const;

    /// Header names are case-insensitive (RFC 7230 3.2): entries are stored
    /// lower-cased and the incoming name is lower-cased before lookup.
    std::unordered_set<std::string> forbidden_headers;
    /// Pre-compiled once with case-insensitive matching (compiling per check would also be case-sensitive).
    std::vector<std::shared_ptr<const re2::RE2>> forbidden_headers_regexp;

    mutable std::mutex mutex;
};

}
