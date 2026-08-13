#include <Storages/BigQuery/BigQueryClient.h>

#include "config.h"

#include <IO/ConnectionTimeouts.h>
#include <IO/GCPOAuth.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>

#if USE_SSL
#    include <Common/Crypto/KeyPair.h>
#    include <Common/OpenSSLHelpers.h>
#endif

#include <Poco/JSON/Parser.h>
#include <Poco/Net/HTTPRequest.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

#if USE_SSL
constexpr auto BIGQUERY_OAUTH_SCOPE = "https://www.googleapis.com/auth/bigquery";
#endif
constexpr auto GOOGLE_OAUTH2_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token";

Poco::JSON::Object::Ptr parseJSONObject(const String & data, const String & what)
{
    try
    {
        Poco::JSON::Parser parser;
        auto object = parser.parse(data).extract<Poco::JSON::Object::Ptr>();
        if (!object)
            throw Exception(ErrorCodes::INCORRECT_DATA, "{} is not a JSON object", what);
        return object;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse {}: {}", what, e.displayText());
    }
}

/// Build an RS256-signed JWT assertion for the OAuth 2.0 service account flow.
/// Returns the assertion and the token endpoint to POST it to.
std::pair<String, String> makeServiceAccountAssertion(const String & service_account_key, const String & token_url_override)
{
    auto key_object = parseJSONObject(service_account_key, "BigQuery service account key");

    if (!key_object->has("client_email") || !key_object->has("private_key"))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "BigQuery service account key must be a JSON object with 'client_email' and 'private_key' "
            "(the content of a key file downloaded from Google Cloud IAM)");

    const auto client_email = key_object->getValue<String>("client_email");
    const auto private_key = key_object->getValue<String>("private_key");
    String token_endpoint = GOOGLE_OAUTH2_TOKEN_ENDPOINT;
    if (key_object->has("token_uri"))
        token_endpoint = key_object->getValue<String>("token_uri");
    if (!token_url_override.empty())
        token_endpoint = token_url_override;

#if USE_SSL
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    Poco::JSON::Object claims;
    claims.set("iss", client_email);
    claims.set("scope", String(BIGQUERY_OAUTH_SCOPE));
    claims.set("aud", token_endpoint);
    claims.set("iat", now);
    claims.set("exp", now + 3600);

    std::ostringstream claims_stream;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    claims.stringify(claims_stream);

    static constexpr auto header = R"({"alg":"RS256","typ":"JWT"})";
    String to_sign = fmt::format(
        "{}.{}",
        base64Encode(header, /*url_encoding*/ true, /*no_padding*/ true),
        base64Encode(claims_stream.str(), /*url_encoding*/ true, /*no_padding*/ true));

    auto key_pair = KeyPair::fromPEMString(private_key);
    String signature = rsaSHA256Sign(static_cast<EVP_PKEY *>(key_pair), to_sign);

    String assertion = fmt::format("{}.{}", to_sign, base64Encode(signature, /*url_encoding*/ true, /*no_padding*/ true));
    return {std::move(assertion), std::move(token_endpoint)};
#else
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED,
        "BigQuery authentication with a service account key requires ClickHouse to be built with SSL support");
#endif
}

}

BigQueryTokenProvider::BigQueryTokenProvider(BigQueryConfiguration configuration_)
    : configuration(std::move(configuration_))
{
}

std::pair<String, Int64> BigQueryTokenProvider::fetchTokenWithExpiration(const ContextPtr & context) const
{
    const auto timeouts = ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings());

    switch (configuration.credentials_kind)
    {
        case BigQueryConfiguration::CredentialsKind::AccessToken:
        {
            /// A static token: we cannot know its expiry, callers never refresh it.
            return {configuration.access_token, std::numeric_limits<Int64>::max()};
        }
        case BigQueryConfiguration::CredentialsKind::ServiceAccountKey:
        {
            auto [assertion, token_endpoint] = makeServiceAccountAssertion(configuration.service_account_key, configuration.token_url);
            /// The token endpoint comes from the user-provided key, validate it against the allowed hosts.
            context->getRemoteHostFilter().checkURL(Poco::URI(token_endpoint));
            auto token = fetchGCPOAuthTokenWithJWTAssertion(assertion, token_endpoint, timeouts);
            return {std::move(token.access_token), token.expires_in};
        }
        case BigQueryConfiguration::CredentialsKind::RefreshToken:
        {
            /// Validate the token endpoint - the default Google one or a user override - against the
            /// allowed hosts, so this auth path cannot bypass the admin host allowlist either.
            const String token_endpoint = configuration.token_url.empty() ? GOOGLE_OAUTH2_TOKEN_ENDPOINT : configuration.token_url;
            context->getRemoteHostFilter().checkURL(Poco::URI(token_endpoint));
            auto token = fetchGCPOAuthToken(
                configuration.client_id, configuration.client_secret, configuration.refresh_token,
                timeouts, HTTPConnectionGroupType::HTTP, token_endpoint);
            return {std::move(token.access_token), token.expires_in};
        }
    }
}

String BigQueryTokenProvider::getToken(const ContextPtr & context, bool force_refresh)
{
    std::lock_guard lock(mutex);

    if (!cached_token.empty() && !force_refresh && std::chrono::system_clock::now() < expires_at)
        return cached_token;

    auto [token, expires_in] = fetchTokenWithExpiration(context);
    if (token.empty())
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Received an empty BigQuery access token");

    cached_token = std::move(token);
    if (expires_in == std::numeric_limits<Int64>::max())
        expires_at = std::chrono::system_clock::time_point::max();
    else
        expires_at = std::chrono::system_clock::now() + std::chrono::seconds(expires_in * 9 / 10);

    return cached_token;
}

BigQueryClient::BigQueryClient(const BigQueryConfiguration & configuration_, ContextPtr context_)
    : configuration(configuration_)
    , context(std::move(context_))
    , token_provider(std::make_shared<BigQueryTokenProvider>(configuration))
    , log(getLogger("BigQueryClient"))
{
}

BigQueryClient::BigQueryClient(
    const BigQueryConfiguration & configuration_, ContextPtr context_, std::shared_ptr<BigQueryTokenProvider> token_provider_)
    : configuration(configuration_)
    , context(std::move(context_))
    , token_provider(std::move(token_provider_))
    , log(getLogger("BigQueryClient"))
{
}

String BigQueryClient::tablePath() const
{
    return fmt::format("/bigquery/v2/projects/{}/datasets/{}/tables/{}", configuration.project, configuration.dataset, configuration.table);
}

Poco::URI BigQueryClient::buildRequestURI(const String & path, const Poco::URI::QueryParameters & params) const
{
    Poco::URI uri(configuration.base_url);
    uri.setPath(path);
    for (const auto & [name, value] : params)
        uri.addQueryParameter(name, value);
    return uri;
}

Poco::JSON::Object::Ptr BigQueryClient::requestJSON(
    const String & method,
    const String & path,
    const Poco::URI::QueryParameters & params,
    const String & request_body) const
{
    Poco::URI uri = buildRequestURI(path, params);

    context->getRemoteHostFilter().checkURL(uri);

    auto do_request = [&](bool force_new_token)
    {
        HTTPHeaderEntries headers;
        headers.emplace_back("Authorization", "Bearer " + token_provider->getToken(context, force_new_token));
        if (!configuration.billing_project.empty())
            headers.emplace_back("X-Goog-User-Project", configuration.billing_project);

        ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback;
        if (!request_body.empty())
        {
            headers.emplace_back("Content-Type", "application/json");
            out_stream_callback = [&request_body](std::ostream & os) { os << request_body; };
        }

        auto buf = BuilderRWBufferFromHTTP(uri)
            .withConnectionGroup(HTTPConnectionGroupType::HTTP)
            .withMethod(method)
            .withSettings(context->getReadSettings())
            .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
            .withHostFilter(&context->getRemoteHostFilter())
            .withHeaders(headers)
            .withOutCallback(std::move(out_stream_callback))
            .withDelayInit(false)
            .create(credentials);

        WriteBufferFromOwnString response;
        copyData(*buf, response);
        response.finalize();
        return response.str();
    };

    String response;
    try
    {
        response = do_request(/*force_new_token*/ false);
    }
    catch (const HTTPException & e)
    {
        const auto status = e.getHTTPStatus();
        bool auth_error = status == Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED || status == Poco::Net::HTTPResponse::HTTP_FORBIDDEN;
        if (!auth_error || !token_provider->canRefresh())
            throw;
        /// The cached token could have expired, retry once with a fresh one.
        LOG_DEBUG(log, "Retrying BigQuery request with a fresh token after HTTP status {}", static_cast<int>(status));
        response = do_request(/*force_new_token*/ true);
    }

    return parseJSONObject(response, fmt::format("BigQuery response for '{}'", path));
}

Poco::JSON::Object::Ptr BigQueryClient::getTable() const
{
    return requestJSON(Poco::Net::HTTPRequest::HTTP_GET, tablePath(), {{"prettyPrint", "false"}}, {});
}

Poco::URI::QueryParameters BigQueryClient::listTableDataParams(const String & page_token, const String & selected_fields, UInt64 max_results)
{
    Poco::URI::QueryParameters params;
    params.emplace_back("prettyPrint", "false");
    /// Return TIMESTAMP values as int64 microseconds since the epoch instead of a floating-point number of seconds.
    params.emplace_back("formatOptions.useInt64Timestamp", "true");
    params.emplace_back("maxResults", toString(max_results));
    if (!page_token.empty())
        params.emplace_back("pageToken", page_token);
    if (!selected_fields.empty())
        params.emplace_back("selectedFields", selected_fields);
    return params;
}

size_t BigQueryClient::tableDataRequestUriLength(const String & selected_fields, UInt64 max_results) const
{
    /// Measure the first-page request (no `pageToken`); the caller reserves headroom for the token separately.
    auto uri = buildRequestURI(tablePath() + "/data", listTableDataParams(/*page_token*/ "", selected_fields, max_results));
    return uri.getPathAndQuery().size();
}

BigQueryClient::TableDataPage BigQueryClient::listTableData(const String & page_token, const String & selected_fields, UInt64 max_results) const
{
    auto params = listTableDataParams(page_token, selected_fields, max_results);

    /// Validate the *actual* request URL, including the opaque `pageToken` BigQuery returns from the second page
    /// onward. The up-front guard in `StorageBigQuery::read` can only measure the first page (the token is not
    /// known until a page is fetched), and the token length is not documented, so a long token could otherwise
    /// push a later request over the HTTP front-end URL length limit and fail remotely with an opaque error after
    /// the read has already started. Reject it here, before issuing the request, with the same clear message.
    const auto uri = buildRequestURI(tablePath() + "/data", params);
    const size_t request_uri_length = uri.getPathAndQuery().size();
    if (request_uri_length > max_request_uri_length)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The `tabledata.list` request URL is too long: {} bytes, over the {}-byte limit (the BigQuery "
            "`pageToken` for the next page does not fit); select fewer columns",
            request_uri_length,
            max_request_uri_length);

    auto response = requestJSON(Poco::Net::HTTPRequest::HTTP_GET, tablePath() + "/data", params, {});

    TableDataPage page;
    if (response->has("rows"))
        page.rows = response->getArray("rows");
    if (response->has("pageToken"))
        page.next_page_token = response->getValue<String>("pageToken");
    if (response->has("totalRows"))
        page.total_rows = response->get("totalRows").convert<UInt64>();
    return page;
}

void BigQueryClient::insertAll(const Poco::JSON::Array::Ptr & rows) const
{
    Poco::JSON::Object request;
    request.set("kind", String("bigquery#tableDataInsertAllRequest"));
    request.set("rows", rows);

    std::ostringstream body;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    request.stringify(body);

    auto response = requestJSON(Poco::Net::HTTPRequest::HTTP_POST, tablePath() + "/insertAll", {{"prettyPrint", "false"}}, body.str());

    if (response->has("insertErrors"))
    {
        auto insert_errors = response->getArray("insertErrors");
        if (insert_errors && insert_errors->size() > 0)
        {
            WriteBufferFromOwnString message;
            size_t errors_to_show = std::min<size_t>(insert_errors->size(), 3);
            for (size_t i = 0; i < errors_to_show; ++i)
            {
                auto entry = insert_errors->getObject(static_cast<unsigned>(i));
                if (!entry)
                    continue;
                if (i > 0)
                    message << "; ";
                message << "row " << entry->getValue<UInt64>("index") << ": ";
                if (auto errors = entry->getArray("errors"); errors && errors->size() > 0)
                {
                    auto error = errors->getObject(0);
                    if (error && error->has("message"))
                        message << error->getValue<String>("message");
                    if (error && error->has("reason"))
                        message << " (" << error->getValue<String>("reason") << ")";
                }
            }
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "BigQuery rejected {} of {} rows in a streaming insert: {}",
                insert_errors->size(), rows->size(), message.str());
        }
    }
}

}
