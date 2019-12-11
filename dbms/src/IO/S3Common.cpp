#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <regex>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentialsProvider.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace S3
{
    ClientFactory::ClientFactory()
    {
        aws_options = Aws::SDKOptions {};
        Aws::InitAPI(aws_options);
    }

    ClientFactory::~ClientFactory()
    {
        Aws::ShutdownAPI(aws_options);
    }

    ClientFactory & ClientFactory::instance()
    {
        static ClientFactory ret;
        return ret;
    }

    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create(
        const String & endpoint,
        const String & access_key_id,
        const String & secret_access_key)
    {
        Aws::Client::ClientConfiguration cfg;
        if (!endpoint.empty())
            cfg.endpointOverride = endpoint;

        auto cred_provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(access_key_id,
                secret_access_key);

        return std::make_shared<Aws::S3::S3Client>(
                std::move(cred_provider), // Credentials provider.
                std::move(cfg), // Client configuration.
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, // Sign policy.
                endpoint.empty() // Use virtual addressing only if endpoint is not specified.
        );
    }


    URI::URI(Poco::URI & uri_)
    {
        static const std::regex BUCKET_KEY_PATTERN("([^/]+)/(.*)");

        uri = uri_;

        // s3://*
        if (uri.getScheme() == "s3" || uri.getScheme() == "S3")
        {
            bucket = uri.getAuthority();
            if (bucket.empty())
                throw Exception ("Invalid S3 URI: no bucket: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

            const auto & path = uri.getPath();
            // s3://bucket or s3://bucket/
            if (path.length() <= 1)
                throw Exception ("Invalid S3 URI: no key: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

            key = path.substr(1);
            return;
        }

        if (uri.getHost().empty())
            throw Exception("Invalid S3 URI: no host: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

        endpoint = uri.getScheme() + "://" + uri.getAuthority();

        // Parse bucket and key from path.
        std::smatch match;
        std::regex_search(uri.getPath(), match, BUCKET_KEY_PATTERN);
        if (!match.empty())
        {
            bucket = match.str(1);
            if (bucket.empty())
                throw Exception ("Invalid S3 URI: no bucket: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

            key = match.str(2);
            if (key.empty())
                throw Exception ("Invalid S3 URI: no key: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
        }
        else
            throw Exception("Invalid S3 URI: no bucket or key: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
    }
}

}

#endif
