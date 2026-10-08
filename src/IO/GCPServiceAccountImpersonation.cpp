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

/// Restrict identifiers embedded in the request path to prevent changing the API method.
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

/// Accept both bare identifiers and `projects/-/serviceAccounts/<id>` resource names.
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

/// `Poco::URI::getPath` decodes escapes. Reject traversal before a proxy can normalize it.
void validateEndpointPath(const std::string & path)
{
    Poco::StringTokenizer segments(path, "/");
    for (const auto & segment : segments)
        if (segment == "..")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The path of `iam_credentials_endpoint` must not contain a `..` segment");
}

/// `Poco::URI` neither validates nor encodes the raw query. Validate RFC 3986 characters
/// without re-encoding, which would double-escape parameters. This also rejects request-line injection.
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

        /// Do not echo control characters into the server log.
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The query of `iam_credentials_endpoint` contains the byte 0x{:02X} at position {}, which is not "
            "valid in a URL query. Percent-encode it if it is meant to be part of a parameter.",
            static_cast<unsigned>(static_cast<unsigned char>(c)),
            i);
    }
}

Poco::URI parseEndpoint(const std::string & endpoint)
{
    static constexpr auto DEFAULT_IAM_CREDENTIALS_ENDPOINT = "https://iamcredentials.googleapis.com";

    Poco::URI url;
    try
    {
        url = Poco::URI(endpoint.empty() ? DEFAULT_IAM_CREDENTIALS_ENDPOINT : endpoint);
    }
    catch (const Poco::Exception & e)
    {
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

    validateEndpointPath(url.getPath());
    validateEndpointQuery(url.getRawQuery());

    return url;
}

Int64 secondsUntil(const std::string & expire_time)
{
    int tz_diff = 0;
    Poco::DateTime expires_at;
    /// The format-guessing overload accepts RFC 3339 fractional seconds; `ISO8601_FORMAT` does not.
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

    /// Google's 12-hour ceiling also bounds the arithmetic used to compute the refresh margin.
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

    remote_host_filter.checkURL(url);

    /// `getPath` is decoded: encode the assembled path exactly once to prevent request-line injection.
    /// Do not use `Poco::URI::setPath`, which would decode it again and turn `%252F` into a separator.
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
    /// `parseEndpoint` validated this already-encoded query.
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
        /// A 403 concerns the target role binding; a 401 concerns the source credentials.
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

    /// Honor the returned expiry even if the API grants less than the requested lifetime.
    /// Do not substitute the requested lifetime when the returned expiry is already in the past.
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
