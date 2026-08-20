#include <IO/GCPServiceAccountImpersonation.h>

#include <algorithm>
#include <fmt/format.h>
#include <Poco/DateTimeParser.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Timestamp.h>
#include <Poco/URI.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <IO/HTTPCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
}

Strings parseGCPCommaSeparatedList(const std::string & value)
{
    Poco::StringTokenizer tokenizer(
        value, ",", Poco::StringTokenizer::TOK_TRIM | Poco::StringTokenizer::TOK_IGNORE_EMPTY);
    return Strings(tokenizer.begin(), tokenizer.end());
}

namespace
{

/// A service account is identified by an email (`name@project.iam.gserviceaccount.com`) or a numeric unique
/// id. Because the identifier goes into the request path, validate it against that alphabet rather than
/// escaping it: an identifier carrying `/`, `?` or `%` could otherwise redirect the request to another method.
void validateServiceAccountIdentifier(const std::string & service_account, std::string_view what)
{
    if (service_account.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} is empty", what);

    for (char c : service_account)
    {
        const bool is_allowed = isAlphaNumericASCII(c) || c == '.' || c == '-' || c == '_' || c == '@' || c == '+';
        if (!is_allowed)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "{} '{}' contains a character that is not valid in a GCP service account email or unique id",
                what,
                service_account);
    }
}

static constexpr std::string_view resource_prefix = "projects/-/serviceAccounts/";

/// GCP spells a service account either as a bare email/unique id or as the `projects/-/serviceAccounts/<id>`
/// resource name, and its own `generateAccessToken` documentation uses the latter. Accept both wherever a
/// service account is configured, and reduce to the bare identifier the request path needs.
std::string toBareServiceAccountIdentifier(const std::string & service_account, std::string_view what)
{
    std::string_view identifier = service_account;
    if (identifier.starts_with(resource_prefix))
        identifier.remove_prefix(resource_prefix.size());

    validateServiceAccountIdentifier(std::string(identifier), what);
    return std::string(identifier);
}

/// `delegates` entries are resource names, not bare emails; accept both in the setting.
std::string toServiceAccountResourceName(const std::string & service_account)
{
    return fmt::format("{}{}", resource_prefix, toBareServiceAccountIdentifier(service_account, "Delegate service account"));
}

std::string buildRequestBody(const GCPImpersonationParams & params)
{
    Poco::JSON::Object body;

    Poco::JSON::Array scopes;
    if (params.scopes.empty())
        scopes.add(std::string(DEFAULT_GCP_IMPERSONATION_SCOPE));
    else
        for (const auto & scope : params.scopes)
            scopes.add(scope);
    body.set("scope", scopes);

    body.set("lifetime", fmt::format("{}s", params.lifetime_seconds));

    if (!params.delegates.empty())
    {
        Poco::JSON::Array delegates;
        for (const auto & delegate : params.delegates)
            delegates.add(toServiceAccountResourceName(delegate));
        body.set("delegates", delegates);
    }

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    body.stringify(oss);
    return oss.str();
}

/// `Poco::URI::getPath` is decoded, so this rejects a percent-encoded `%2E%2E` as well as a literal `..`.
/// Such a segment survives the percent-encoding of the request target below -- `.` is unreserved, so
/// `Poco::URI::encode` passes it through -- and a proxy or the peer that normalizes the path would then route
/// the request, which carries the source identity's access token, to a path the operator never configured.
void validateEndpointPath(const std::string & path)
{
    Poco::StringTokenizer segments(path, "/");
    for (const auto & segment : segments)
        if (segment == "..")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The path of `iam_credentials_endpoint` must not contain a `..` segment");
}

/// RFC 3986 allows in a query only unreserved characters, sub-delims, `:`, `@`, `/`, `?` and percent-encoded
/// octets. `Poco::URI` copies the query out of the endpoint verbatim -- `parseQuery` neither validates nor
/// encodes it -- and it is appended to the request target as it stands, because it is already encoded and
/// encoding it again would turn a legitimate `%2F` into `%252F`. A CR or LF there splits the request line of
/// a request that carries the source identity's access token, letting the endpoint inject headers or a whole
/// second request onto that connection, so reject what is not a valid query instead of encoding it.
void validateEndpointQuery(const std::string & query)
{
    static constexpr std::string_view allowed_punctuation = "-._~!$&'()*+,;=:@/?";

    for (size_t i = 0; i < query.size(); ++i)
    {
        const char c = query[i];
        if (isAlphaNumericASCII(c) || allowed_punctuation.find(c) != std::string_view::npos)
            continue;

        if (c == '%' && i + 2 < query.size() && isHexDigit(query[i + 1]) && isHexDigit(query[i + 2]))
        {
            i += 2;
            continue;
        }

        /// The offending byte is named rather than echoed: it is control characters that matter here, and
        /// quoting them in the message would only carry the injection from the wire into the server log.
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The query of `iam_credentials_endpoint` contains the byte 0x{:02X} at position {}, which is not "
            "valid in a URL query. Percent-encode it if it is meant to be part of a parameter.",
            static_cast<unsigned>(static_cast<unsigned char>(c)),
            i);
    }
}

/// The endpoint has to be an absolute URL: `Poco::URI` parses a bare `host` as a relative path, which would
/// leave the session with an empty host.
Poco::URI parseEndpoint(const std::string & endpoint)
{
    /// The IAM Service Account Credentials API, which mints short-lived credentials.
    static constexpr auto DEFAULT_IAM_CREDENTIALS_ENDPOINT = "https://iamcredentials.googleapis.com";

    Poco::URI url;
    try
    {
        url = Poco::URI(endpoint.empty() ? DEFAULT_IAM_CREDENTIALS_ENDPOINT : endpoint);
    }
    catch (const Poco::Exception & e)
    {
        /// `Poco::URI` throws on input it cannot parse at all (an unterminated IPv6 literal, say). Report that
        /// as the same `BAD_ARGUMENTS` naming the setting, rather than letting a bare Poco exception escape.
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "`iam_credentials_endpoint` is not a valid URL: '{}' ({})",
            endpoint,
            e.displayText());
    }

    if (url.getScheme().empty() || url.getHost().empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "`iam_credentials_endpoint` must be an absolute URL with a scheme and a host, e.g. '{}', got '{}'",
            DEFAULT_IAM_CREDENTIALS_ENDPOINT,
            endpoint);

    /// Whatever the endpoint carries beyond scheme and host ends up in the request line of the token exchange,
    /// so it is validated here rather than where the request target is assembled: this runs from
    /// `validateGCPImpersonationParams` too, which rejects a bad endpoint when the client is built instead of
    /// on the first read that needs a token.
    validateEndpointPath(url.getPath());
    validateEndpointQuery(url.getRawQuery());

    return url;
}

/// Seconds left until `expire_time`, which the API returns as an RFC 3339 timestamp.
Int64 secondsUntil(const std::string & expire_time)
{
    int tz_diff = 0;
    Poco::DateTime expires_at;
    /// The format-guessing overload, because RFC 3339 allows fractional seconds (which GCP does emit) and the
    /// fixed `ISO8601_FORMAT` cannot consume them.
    if (!Poco::DateTimeParser::tryParse(expire_time, expires_at, tz_diff))
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Failed to parse 'expireTime' of the impersonated GCP access token: '{}' is not an RFC 3339 timestamp",
            expire_time);

    expires_at.makeUTC(tz_diff);
    return static_cast<Int64>(expires_at.timestamp().epochTime()) - static_cast<Int64>(Poco::Timestamp().epochTime());
}

}

void validateGCPImpersonationParams(const GCPImpersonationParams & params)
{
    if (params.target_service_account.empty())
        return;

    toBareServiceAccountIdentifier(params.target_service_account, "Service account to impersonate");
    for (const auto & delegate : params.delegates)
        toServiceAccountResourceName(delegate);
    parseEndpoint(params.endpoint);

    /// Bound the lifetime here rather than only rejecting `<= 0`: the value is user-settable, and it is
    /// multiplied when the client computes its refresh margin, which overflows for very large values.
    /// `MAX_GCP_IMPERSONATION_LIFETIME_SECONDS` is Google's own ceiling with
    /// `constraints/iam.allowServiceAccountCredentialLifetimeExtension` granted; anything above it would be
    /// refused by the API anyway.
    if (params.lifetime_seconds <= 0 || params.lifetime_seconds > MAX_GCP_IMPERSONATION_LIFETIME_SECONDS)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "`impersonation_lifetime_seconds` must be between 1 and {} (12 hours, Google's maximum), got {}",
            MAX_GCP_IMPERSONATION_LIFETIME_SECONDS,
            params.lifetime_seconds);
}

GCPOAuthToken fetchImpersonatedGCPAccessToken(
    const std::string & source_access_token,
    const GCPImpersonationParams & params,
    const ConnectionTimeouts & timeouts,
    const RemoteHostFilter & remote_host_filter,
    HTTPConnectionGroupType group)
{
    const auto target_service_account = toBareServiceAccountIdentifier(params.target_service_account, "Service account to impersonate");
    if (source_access_token.empty())
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Cannot impersonate service account '{}': the source access token is empty",
            params.target_service_account);

    auto url = parseEndpoint(params.endpoint);

    /// The endpoint is configurable, and this request carries the source identity's access token, so it is
    /// subject to the operator's egress allow-list just like the S3 URL itself.
    remote_host_filter.checkURL(url);

    /// `Poco::URI::getPath` returns the *decoded* path, so it cannot be concatenated into the request target
    /// directly: a percent-encoded space, `?`, or CRLF in `iam_credentials_endpoint` would be written to the
    /// wire literally, splitting the request line and letting a chosen endpoint inject headers onto a
    /// connection that carries the source identity's access token. Encode the assembled path exactly once
    /// instead -- what `getPathAndQuery` does to the decoded path it holds. Note that it cannot be routed
    /// through `Poco::URI::setPath`, which *decodes* what it is given: that would strip a layer of encoding
    /// rather than add one, turning a `%252E%252E` in the endpoint into a real `..` and a `%252F` into a real
    /// path separator, both of which reach the wire as-is.
    auto base_path = url.getPath();
    while (base_path.ends_with('/'))
        base_path.pop_back();

    std::string request_target;
    /// `Poco::URI::RESERVED_PATH`, which is not accessible from here.
    static constexpr auto reserved_path = "?#";
    Poco::URI::encode(
        fmt::format("{}/v1/projects/-/serviceAccounts/{}:generateAccessToken", base_path, target_service_account),
        reserved_path,
        request_target);
    /// The raw query, because it is already percent-encoded; `parseEndpoint` has checked that it holds nothing
    /// that cannot appear in a request line.
    if (!url.getRawQuery().empty())
        request_target += fmt::format("?{}", url.getRawQuery());

    auto body = buildRequestBody(params);

    auto log = getLogger("GCPServiceAccountImpersonation");
    LOG_DEBUG(
        log,
        "Requesting an impersonated GCP access token for service account {} ({} delegate(s))",
        params.target_service_account,
        params.delegates.size());

    auto session = makeGCPTokenEndpointSession(group, url, timeouts, log);

    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, request_target, Poco::Net::HTTPMessage::HTTP_1_1);
    request.set("Authorization", fmt::format("Bearer {}", source_access_token));
    request.setContentType("application/json; charset=utf-8");
    request.setContentLength(body.size());
    request.set("Accept", "application/json");

    std::ostream & os = session->sendRequest(request);
    os << body;

    Poco::Net::HTTPResponse response;
    std::istream & rs = session->receiveResponse(response);

    String token_json_raw;
    Poco::StreamCopier::copyToString(rs, token_json_raw);

    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
    {
        /// The API states the cause in the response body; `getReason` is only the HTTP reason phrase. The missing
        /// role is one cause among many (a rejected lifetime, a wrong endpoint, exhausted quota), so name it only
        /// for the status it explains. 403 and 401 are different failures and must not share a hint: the API
        /// answers 403 when the role binding on the *target* is missing, and 401 when the *source* token it was
        /// called with was itself rejected, which no IAM change on the target can fix.
        std::string_view hint;
        if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_FORBIDDEN)
            hint = ". The source identity needs the `roles/iam.serviceAccountTokenCreator` role on the target "
                   "service account";
        else if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED)
            hint = ". The source identity's own access token was rejected, so check the source credentials "
                   "(the metadata service or the Google Application Default Credentials triple), not the role "
                   "binding on the target";

        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Failed to impersonate GCP service account '{}': {} (HTTP {}): {}{}",
            params.target_service_account,
            response.getReason(),
            static_cast<int>(response.getStatus()),
            std::string_view(token_json_raw).substr(0, 1024),
            hint);
    }

    String access_token;
    String expire_time;
    try
    {
        Poco::JSON::Parser parser;
        auto object = parser.parse(token_json_raw).extract<Poco::JSON::Object::Ptr>();
        if (object && object->has("accessToken") && object->has("expireTime"))
        {
            access_token = object->getValue<String>("accessToken");
            expire_time = object->getValue<String>("expireTime");
        }
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Cannot read the generateAccessToken response from '{}': {}",
            url.getHost(),
            e.displayText());
    }

    if (access_token.empty() || expire_time.empty())
        throw Exception(
            ErrorCodes::AUTHENTICATION_FAILED,
            "Unexpected generateAccessToken response: missing or empty 'accessToken' or 'expireTime'");

    GCPOAuthToken result;
    result.access_token = std::move(access_token);

    /// `expireTime` is authoritative, the requested lifetime is not: the API may grant less. A non-positive
    /// value means the token is already expired by this host's clock -- either it is ahead of Google's, or a
    /// very short lifetime rounded down to zero. Keeping the requested lifetime there would sign every request
    /// for the next ~0.9 x lifetime with a token the API considers dead, so expire it at once instead and let
    /// the next request mint a new one.
    const auto reported = secondsUntil(expire_time);
    if (reported <= 0)
    {
        LOG_WARNING(
            log,
            "The impersonated token for service account {} reports 'expireTime' {}, which is not in the future "
            "according to this host's clock. Treating it as expired; check that the clock is in sync.",
            params.target_service_account,
            expire_time);
        result.expires_in = 1;
    }
    else
        result.expires_in = std::min(reported, params.lifetime_seconds);

    return result;
}

}
