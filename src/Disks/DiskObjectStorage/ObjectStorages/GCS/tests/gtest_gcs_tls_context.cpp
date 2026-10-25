#include "config.h"

#if USE_GOOGLE_CLOUD

#include <Common/tests/gtest_ephemeral_certificate.h>

#include <google/cloud/credentials.h>
#include <google/cloud/options.h>
#include <google/cloud/storage/client.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/storage/retry_policy.h>

#include <poco_rest_options.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/SecureServerSocket.h>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>

namespace gcs = ::google::cloud::storage;
namespace gc = ::google::cloud;

namespace
{

class ObjectMetadataHandler : public Poco::Net::HTTPRequestHandler
{
public:
    void handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response) override
    {
        const std::string body = R"({"kind":"storage#object","bucket":"test-bucket","name":"key",)"
                                 R"("generation":"1","size":"4","updated":"2026-01-01T00:00:00Z"})";
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentType("application/json");
        response.setContentLength(static_cast<std::streamsize>(body.size()));
        response.send() << body;
    }
};

class ObjectMetadataHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new ObjectMetadataHandler;
    }
};

/// A client context that verifies the peer strictly against `ca_location` only, without the default roots,
/// as `openSSL.client` with `caConfig` and `loadDefaultCAFile = false` configures it. The host name is not
/// checked: the server listens on `127.0.0.1` and the ephemeral certificate is issued to `localhost`.
Poco::Net::Context::Ptr makeClientContext(const std::string & ca_location)
{
    Poco::Net::Context::Params params;
    params.caLocation = ca_location;
    params.verificationMode = Poco::Net::Context::VERIFY_STRICT;
    params.loadDefaultCAs = false;
    Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, params);
    context->enableExtendedCertificateVerification(false);
    return context;
}

/// Requests object metadata over HTTPS from a server with a self-signed certificate, with the transport using
/// `client_context` when it is set and its own context otherwise. Returns whether the request succeeded.
bool fetchMetadataOverTLS(const EphemeralCert & cert, std::optional<Poco::Net::Context::Ptr> client_context)
{
    Poco::Net::SecureServerSocket server_socket(
        Poco::Net::SocketAddress("127.0.0.1", 0), 64, cert.makeContext(Poco::Net::Context::SERVER_USE));
    Poco::Net::HTTPServer server(new ObjectMetadataHandlerFactory, server_socket, new Poco::Net::HTTPServerParams);
    server.start();

    gc::Options options;
    options.set<gc::UnifiedCredentialsOption>(gc::MakeInsecureCredentials());
    options.set<gcs::RestEndpointOption>(fmt::format("https://{}", server_socket.address().toString()));
    options.set<gcs::RetryPolicyOption>(gcs::LimitedErrorCountRetryPolicy(0).clone());
    if (client_context)
        options.set<::ClickHouse::PocoRestSslContextProviderOption>([context = *client_context] { return context; });
    gcs::Client client(std::move(options));

    const bool ok = client.GetObjectMetadata("test-bucket", "key").ok();
    server.stop();
    return ok;
}

}

/// The HTTPS sessions of the native transport verify the peer with the client TLS context ClickHouse supplies
/// (the one configured in `openSSL.client`), so a storage endpoint behind a private CA is reachable when
/// that CA is configured, and a configuration without the default roots does not trust them again.
TEST(GCSTLSContext, SuppliedContextTrustsItsPrivateCA)
{
    EphemeralCert cert;
    EXPECT_TRUE(fetchMetadataOverTLS(cert, makeClientContext(cert.cert_path)));
}

TEST(GCSTLSContext, SuppliedContextWithoutTheCAIsRejected)
{
    EphemeralCert cert;
    EXPECT_FALSE(fetchMetadataOverTLS(cert, makeClientContext("")));
}

TEST(GCSTLSContext, OwnContextDoesNotTrustAPrivateCA)
{
    EphemeralCert cert;
    EXPECT_FALSE(fetchMetadataOverTLS(cert, std::nullopt));
}

#endif
