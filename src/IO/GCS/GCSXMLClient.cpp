#include <IO/GCS/GCSXMLClient.h>

#include <Common/Exception.h>
#include <Poco/URI.h>

#if USE_GOOGLE_CLOUD
#    include <google/cloud/credentials.h>
#    include <google/cloud/oauth2/access_token_generator.h>
#endif

#if USE_AWS_S3
#    include <Common/HTTPHeaderFilter.h>
#    include <IO/S3/Credentials.h>
#    include <IO/S3/PocoHTTPClient.h>
#    include <Interpreters/Context.h>
#endif

namespace DB::ErrorCodes
{
extern const int AUTHENTICATION_FAILED;
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace DB::GCS
{
namespace
{

constexpr std::string_view default_xml_endpoint = "https://storage.googleapis.com";
constexpr std::string_view default_gcs_endpoint = "storage.googleapis.com";
constexpr std::string_view default_gcs_https_endpoint = "https://storage.googleapis.com";
constexpr std::string_view default_gcs_http_endpoint = "http://storage.googleapis.com";
constexpr std::string_view direct_path_prefix = "google-c2p:///";
constexpr std::string_view gcp_oauth_http_client = "gcp_oauth";

String withHTTPSDefaultScheme(const String & endpoint)
{
    if (endpoint.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS XML multipart endpoint cannot be empty");

    if (endpoint.contains("://"))
        return endpoint;

    return "https://" + endpoint;
}

void validateHTTPXMLMultipartEndpoint(const String & endpoint)
{
    Poco::URI uri(endpoint);
    if (uri.getScheme() != "https")
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS XML multipart endpoint '{}' must use https", endpoint);
    if (uri.getHost().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS XML multipart endpoint '{}' must include a host", endpoint);
}

#if USE_GOOGLE_CLOUD
class GoogleCloudXMLBearerTokenProvider final : public IXMLBearerTokenProvider
{
public:
    explicit GoogleCloudXMLBearerTokenProvider(ClientSettings client_settings_)
        : client_settings(std::move(client_settings_))
        , credential_mode(credentialMode(client_settings))
    {
        std::shared_ptr<google::cloud::Credentials> credentials;
        if (credential_mode == CredentialMode::GoogleDefault)
            credentials = google::cloud::MakeGoogleDefaultCredentials();
        else if (credential_mode == CredentialMode::ServiceAccountKey)
            credentials = google::cloud::MakeServiceAccountCredentials(client_settings.service_account_json);
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native GCS XML multipart does not support insecure test credentials in the production token provider");

        token_generator = google::cloud::oauth2::MakeAccessTokenGenerator(*credentials);
    }

    XMLBearerToken getToken() override
    {
        auto token = token_generator->GetToken();
        if (!token)
            throw Exception(
                ErrorCodes::AUTHENTICATION_FAILED,
                "Failed to get OAuth bearer token for native GCS XML multipart using {} credentials: {}",
                credentialModeName(credential_mode),
                token.status().message());

        return XMLBearerToken{std::move(token->token), token->expiration};
    }

    CredentialMode getCredentialMode() const override { return credential_mode; }

private:
    ClientSettings client_settings;
    CredentialMode credential_mode;
    std::shared_ptr<google::cloud::oauth2::AccessTokenGenerator> token_generator;
};
#endif

}

const char * writeTransportName(WriteTransport transport)
{
    switch (transport)
    {
        case WriteTransport::Grpc:
            return "grpc";
        case WriteTransport::XMLMultipart:
            return "xml_multipart";
    }
}

WriteTransport parseWriteTransport(std::string_view value)
{
    if (value == "grpc")
        return WriteTransport::Grpc;
    if (value == "xml_multipart")
        return WriteTransport::XMLMultipart;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown native GCS write_transport '{}'; expected 'grpc' or 'xml_multipart'",
        value);
}

CallbackXMLBearerTokenProvider::CallbackXMLBearerTokenProvider(CredentialMode credential_mode_, TokenCallback callback_)
    : credential_mode(credential_mode_)
    , callback(std::move(callback_))
{
    if (!callback)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS XML multipart token callback cannot be empty");
}

XMLBearerToken CallbackXMLBearerTokenProvider::getToken()
{
    auto token = callback();
    if (token.token.empty())
        throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Native GCS XML multipart token callback returned an empty token");
    return token;
}

String normalizeXMLMultipartEndpoint(const String & native_endpoint, const String & xml_endpoint)
{
    const String & selected_endpoint = xml_endpoint.empty() ? native_endpoint : xml_endpoint;
    if (selected_endpoint.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS XML multipart endpoint cannot be empty");

    if (xml_endpoint.empty())
    {
        if (selected_endpoint == default_gcs_endpoint || selected_endpoint == default_gcs_https_endpoint || selected_endpoint == default_gcs_http_endpoint)
            return String(default_xml_endpoint);

        if (selected_endpoint.starts_with(direct_path_prefix))
        {
            const auto target = selected_endpoint.substr(direct_path_prefix.size());
            if (target == default_gcs_endpoint)
                return String(default_xml_endpoint);
        }

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Native GCS XML multipart cannot infer an HTTP endpoint from native endpoint '{}'; configure xml_endpoint",
            selected_endpoint);
    }

    String normalized = withHTTPSDefaultScheme(selected_endpoint);
    validateHTTPXMLMultipartEndpoint(normalized);
    return normalized;
}

XMLMultipartClientSettings makeXMLMultipartClientSettings(const ClientSettings & client_settings, const String & xml_endpoint)
{
    return XMLMultipartClientSettings{
        .endpoint = normalizeXMLMultipartEndpoint(client_settings.endpoint, xml_endpoint),
        .user_project = client_settings.user_project,
        .credential_mode = credentialMode(client_settings),
        .use_insecure_credentials_for_tests = client_settings.use_insecure_credentials_for_tests,
    };
}

std::vector<XMLMultipartHeader> makeXMLMultipartHeaders(const ClientSettings & client_settings)
{
    std::vector<XMLMultipartHeader> headers;
    if (!client_settings.user_project.empty())
        headers.push_back({"x-goog-user-project", client_settings.user_project});
    return headers;
}

void assertXMLMultipartSupportAvailable()
{
#if !USE_AWS_S3
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Native GCS XML multipart write transport requires ClickHouse to be built with S3/XML multipart support (USE_AWS_S3)");
#endif
}

std::shared_ptr<IXMLBearerTokenProvider> createXMLBearerTokenProvider(const ClientSettings & client_settings)
{
#if USE_GOOGLE_CLOUD
    return std::make_shared<GoogleCloudXMLBearerTokenProvider>(client_settings);
#else
    (void)client_settings;
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Native GCS XML multipart OAuth token generation is not available because ClickHouse was built without Google Cloud support");
#endif
}

#if USE_AWS_S3
std::unique_ptr<S3::Client> createXMLMultipartClient(
    const ClientSettings & client_settings,
    const String & xml_endpoint,
    ContextPtr context,
    const String & disk_name,
    const std::shared_ptr<IXMLBearerTokenProvider> & token_provider)
{
    if (!token_provider)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Native GCS XML multipart token provider cannot be empty");

    auto endpoint = normalizeXMLMultipartEndpoint(client_settings.endpoint, xml_endpoint);

    S3::PocoHTTPClientConfiguration::RetryStrategy retry_strategy;
    retry_strategy.max_retries = static_cast<unsigned int>(client_settings.max_retry_attempts);

    auto client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        /* force_region */ "",
        context->getRemoteHostFilter(),
        S3::DEFAULT_MAX_REDIRECTS,
        retry_strategy,
        /* s3_slow_all_threads_after_network_error */ false,
        /* s3_slow_all_threads_after_retryable_error */ false,
        /* enable_s3_requests_logging */ false,
        /* for_disk_s3 */ true,
        disk_name,
        client_settings.request_throttler,
        "https");

    client_configuration.requestTimeoutMs = client_settings.request_timeout_ms;
    client_configuration.endpointOverride = endpoint;
    client_configuration.http_client = String(gcp_oauth_http_client);
    client_configuration.profile_events_namespace = S3::ProfileEventsNamespace::GCS;
    client_configuration.gcs_oauth_token_provider = [token_provider]
    {
        return token_provider->getToken().token;
    };

    S3::ClientSettings s3_client_settings;
    s3_client_settings.disable_checksum = true;
    s3_client_settings.gcs_issue_compose_request = false;

    HTTPHeaderEntries headers;
    for (const auto & header : makeXMLMultipartHeaders(client_settings))
        headers.push_back({header.name, header.value});
    context->getHTTPHeaderFilter().checkAndNormalizeHeaders(headers);

    S3::CredentialsConfiguration credentials_configuration;
    credentials_configuration.no_sign_request = true;

    return S3::ClientFactory::instance().create(
        client_configuration,
        s3_client_settings,
        /* access_key_id */ "",
        /* secret_access_key */ "",
        /* server_side_encryption_customer_key_base64 */ "",
        {},
        std::move(headers),
        credentials_configuration);
}
#endif

}
