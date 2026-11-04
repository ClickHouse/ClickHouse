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
    /// Validates header names and values, throwing BAD_ARGUMENTS on an invalid or forbidden one.
    void checkHeaders(HTTPHeaderEntries & entries) const;
    void checkHeaders(NormalizedHTTPHeaderEntries & entries) const;

    /// Basic credentials are sent in the `Authorization` header, so they are rejected like an explicit
    /// `Authorization` header when <http_forbid_headers> forbids it. Credentials with an empty username
    /// send no header, see `ReadWriteBufferFromHTTP::prepareRequest`, so they are not checked.
    void checkBasicCredentials(const Poco::Net::HTTPBasicCredentials & credentials) const;
    /// The same for a sender that turns any non-empty userinfo of a URL into Basic credentials, even without a username,
    /// such as `ConfluentSchemaRegistry::applyAuth`. The userinfo of the other URLs goes through `setCredentialsFromURL`.
    void checkURIUserInfo(const Poco::URI & uri) const;

private:
    /// Checks a synthetic `Authorization` header and appends `reason` to the error.
    void checkAuthorizationHeader(const std::string & reason) const;

    /// Header names are case-insensitive (RFC 9110 5.1): entries are stored
    /// lower-cased and the incoming name is lower-cased before lookup.
    std::unordered_set<std::string> forbidden_headers;
    /// Pre-compiled once with case-insensitive matching (compiling per check would also be case-sensitive).
    std::vector<std::shared_ptr<const re2::RE2>> forbidden_headers_regexp;

    mutable std::mutex mutex;
};

}
