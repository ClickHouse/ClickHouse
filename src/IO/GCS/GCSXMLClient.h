#pragma once

#include <IO/GCS/GCSClient.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include <config.h>

#if USE_AWS_S3
#    include <Interpreters/Context_fwd.h>
#    include <IO/S3/Client.h>
#endif

namespace DB::GCS
{

enum class WriteTransport
{
    Grpc,
    XMLMultipart,
};

const char * writeTransportName(WriteTransport transport);
WriteTransport parseWriteTransport(std::string_view value);

struct XMLMultipartHeader
{
    String name;
    String value;
};

struct XMLMultipartClientSettings
{
    String endpoint;
    String user_project;
    CredentialMode credential_mode = CredentialMode::GoogleDefault;
    bool use_insecure_credentials_for_tests = false;
};

struct XMLBearerToken
{
    String token;
    std::chrono::system_clock::time_point expiration;
};

class IXMLBearerTokenProvider
{
public:
    virtual ~IXMLBearerTokenProvider() = default;
    virtual XMLBearerToken getToken() = 0;
    virtual CredentialMode getCredentialMode() const = 0;
};

class CallbackXMLBearerTokenProvider final : public IXMLBearerTokenProvider
{
public:
    using TokenCallback = std::function<XMLBearerToken()>;

    CallbackXMLBearerTokenProvider(CredentialMode credential_mode_, TokenCallback callback_);

    XMLBearerToken getToken() override;
    CredentialMode getCredentialMode() const override { return credential_mode; }

private:
    CredentialMode credential_mode;
    TokenCallback callback;
};

String normalizeXMLMultipartEndpoint(const String & native_endpoint, const String & xml_endpoint);
XMLMultipartClientSettings makeXMLMultipartClientSettings(const ClientSettings & client_settings, const String & xml_endpoint);
std::vector<XMLMultipartHeader> makeXMLMultipartHeaders(const ClientSettings & client_settings);
void assertXMLMultipartSupportAvailable();
std::shared_ptr<IXMLBearerTokenProvider> createXMLBearerTokenProvider(const ClientSettings & client_settings);

#if USE_AWS_S3
std::unique_ptr<S3::Client> createXMLMultipartClient(
    const ClientSettings & client_settings,
    const String & xml_endpoint,
    ContextPtr context,
    const String & disk_name,
    const std::shared_ptr<IXMLBearerTokenProvider> & token_provider);
#endif

}
