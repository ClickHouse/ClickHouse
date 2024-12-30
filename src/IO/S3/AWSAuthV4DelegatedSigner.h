#pragma once

#include "config.h"

#if USE_AWS_S3
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <Common/logger_useful.h>
#include <IO/S3/PocoHTTPClient.h>

namespace DB::S3
{
class AWSAuthV4DelegatedSigner : public Aws::Client::AWSAuthV4Signer
{
public:
    AWSAuthV4DelegatedSigner(
        const Aws::String & signatureDelegationUrl,
        const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentialsProvider,
        const char * serviceName,
        const Aws::String & region,
        PayloadSigningPolicy signingPolicy = PayloadSigningPolicy::RequestDependent,
        bool urlEscapePath = true,
        Aws::Auth::AWSSigningAlgorithm signingAlgorithm = Aws::Auth::AWSSigningAlgorithm::SIGV4);

    Aws::String GenerateSignature(
        const Aws::String & canonicalRequestString,
        const Aws::String & date,
        const Aws::String & simpleDate,
        const Aws::String & signingRegion,
        const Aws::String & signingServiceName,
        const Aws::Auth::AWSCredentials& credentials) const override;
private:
    Aws::String signature_delegation_url;
    Poco::Logger * logger;
};
}

#endif
