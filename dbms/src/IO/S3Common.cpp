#include <IO/S3Common.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <iterator>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


static std::mutex aws_init_lock;
static Aws::SDKOptions aws_options;
static std::atomic<bool> aws_initialized(false);

static const std::regex S3_URL_REGEX(R"((https?://.*)/(.*)/(.*))");


static void initializeAwsAPI() {
    std::lock_guard<std::mutex> lock(aws_init_lock);

    if (!aws_initialized.load()) {
        Aws::InitAPI(aws_options);
        aws_initialized.store(true);
    }
}

std::shared_ptr<Aws::S3::S3Client> S3Helper::createS3Client(const String & endpoint_url,
    const String & access_key_id,
    const String & secret_access_key)
{
    initializeAwsAPI();

    Aws::Client::ClientConfiguration cfg;
    cfg.endpointOverride = endpoint_url;
    cfg.scheme = Aws::Http::Scheme::HTTP;

    auto cred_provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(access_key_id, secret_access_key);

    return std::make_shared<Aws::S3::S3Client>(
            std::move(cred_provider),
            std::move(cfg),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false
    );
}


S3Endpoint S3Helper::parseS3EndpointFromUrl(const String & url) {
    std::smatch match;
    if (std::regex_search(url, match, S3_URL_REGEX) && match.size() > 1) {
        S3Endpoint endpoint;
        endpoint.endpoint_url = match.str(1);
        endpoint.bucket = match.str(2);
        endpoint.key = match.str(3);
        return endpoint;
    }
    else
        throw Exception("Failed to parse S3 Storage URL. It should contain endpoint url, bucket and file. "
                        "Regex is (https?://.*)/(.*)/(.*)", ErrorCodes::BAD_ARGUMENTS);
}

}
