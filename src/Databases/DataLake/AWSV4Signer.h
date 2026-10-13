#pragma once

#include "config.h"

#if USE_AVRO && USE_SSL && USE_AWS_S3

#include <Core/Types.h>
#include <IO/HTTPHeaderEntries.h>
#include <Poco/URI.h>

namespace Aws::Client
{
class AWSAuthV4Signer;
}

namespace DataLake
{

/// Sign a Poco-style HTTP request using the AWS SDK's AWSAuthV4Signer.
/// Builds a temporary Aws::Http::StandardHttpRequest, signs it, then extracts
/// the resulting headers into out_headers (excluding Host; ReadWriteBufferFromHTTP sets it from the URI).
void signRequestWithAWSV4(
    const String & method,
    const Poco::URI & uri,
    const DB::HTTPHeaderEntries & extra_headers,
    const String & payload,
    Aws::Client::AWSAuthV4Signer & signer,
    const String & region,
    const String & service,
    DB::HTTPHeaderEntries & out_headers);

}

#endif
