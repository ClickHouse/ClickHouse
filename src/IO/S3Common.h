#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>

namespace Aws::S3
{
    class S3Client;
}

namespace DB
{
   struct HttpHeader;
   using HeaderCollection = std::vector<HttpHeader>;
}

namespace DB::S3
{

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    std::shared_ptr<Aws::S3::S3Client> create(
        const String & endpoint,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key);

    std::shared_ptr<Aws::S3::S3Client> create(
        Aws::Client::ClientConfiguration & cfg,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key);

    std::shared_ptr<Aws::S3::S3Client> create(
        const String & endpoint,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        HeaderCollection headers);
    
    std::shared_ptr<Aws::S3::S3Client> create(
        const String & endpoint,
        const String & region,
        bool is_https_scheme,
        const String & access_key_id,
        const String & secret_access_key);

private:
    ClientFactory();

private:
    Aws::SDKOptions aws_options;
};

/**
 * Represents S3 URI.
 *
 * The following patterns are allowed:
 * s3://bucket/key
 * http(s)://endpoint/bucket/key
 */
struct URI
{
    Poco::URI uri;
    // Custom endpoint if URI scheme is not S3.
    String endpoint;
    String bucket;
    String key;

    bool is_virtual_hosted_style;

    explicit URI(const Poco::URI & uri_);
};

class AWSLogger final : public Aws::Utils::Logging::LogSystemInterface
{
public:
    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return Aws::Utils::Logging::LogLevel::Trace; }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final; // NOLINT

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream) final;

    void Flush() final {}

private:
    Poco::Logger * log = &Poco::Logger::get("AWSClient");
};

class S3AuthSigner : public Aws::Client::AWSAuthV4Signer
{
public:
    S3AuthSigner(
        const Aws::Client::ClientConfiguration & client_configuration,
        const Aws::Auth::AWSCredentials & credentials,
        const DB::HeaderCollection & headers_);

    bool SignRequest(Aws::Http::HttpRequest & request, const char * region, bool sign_body) const override;

    bool PresignRequest(
        Aws::Http::HttpRequest & request,
        const char * region,
        const char * serviceName,
        long long expiration_time_sec) const override; // NOLINT

private:
    const DB::HeaderCollection headers;
};
}

#endif
