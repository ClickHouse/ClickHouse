#pragma once

#include <Access/OPA/OpaConfiguration.h>
#include <Access/OPA/OpaExpressions.h>
#include <Access/OPA/OpaRequest.h>
#include <Core/Types.h>
#include <base/types.h>

#include <Poco/JSON/Object.h>
#include <Poco/URI.h>

#include <vector>


namespace DB
{

/** Sends requests to OPA and interprets the responses.
  *
  * Every failure - a transport error, a non-200 status, a body that is not the documented shape -
  * propagates as an exception. There is deliberately no path that turns a failed decision into an
  * allow: that would disable the authorization layer at the worst possible moment, and silently.
  */
class OpaClient
{
public:
    explicit OpaClient(OpaConfigurationPtr configuration_);

    /// Asks the single-decision endpoint whether the action is allowed.
    bool isAllowed(const OpaRequest & request, const OpaRequestContext & request_context) const;

    /// Asks for the row filters that apply to the resource. An empty result means no filtering, which
    /// is the common case and is not an error - unlike a missing decision from the allow endpoint,
    /// the absence of a filter is a meaningful answer.
    Strings getRowFilters(const OpaRequest & request, const OpaRequestContext & request_context) const;

    /// Asks for the masks that apply to the columns named in the request. One request covers a whole
    /// table, so a wide table does not turn into one request per column.
    std::vector<OpaColumnMask> getColumnMasks(const OpaRequest & request, const OpaRequestContext & request_context) const;

private:
    /// Performs the POST, retrying only transport failures. A response that arrived and was
    /// understood is never retried, whichever way it decided.
    String send(const Poco::URI & uri, const String & body) const;

    /// Extracts the `result` field, accepting both a bare boolean and an object with an `allow` field,
    /// since which one a deployment produces depends on how the policy is written.
    static bool parseDecision(const Poco::URI & uri, const String & response_body);

    /// Extracts the filter expressions. An absent or null `result` yields an empty list.
    static Strings parseRowFilters(const Poco::URI & uri, const String & response_body);

    /// Extracts a list of per-column masks. An absent or null `result` yields an empty list.
    static std::vector<OpaColumnMask> parseColumnMasks(const Poco::URI & uri, const String & response_body);

    static Poco::JSON::Object::Ptr parseResponseObject(const Poco::URI & uri, const String & response_body);

    const OpaConfigurationPtr configuration;
};

}
