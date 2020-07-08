#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Core/Types.h>
#include <Interpreters/Context.h>

namespace DB::COS
{
/**
 * Represents COS URI.
 *
 * The following patterns are allowed:
 * http://${BUCKET}.cos.${REGION}.myqcloud.com/key
 * https://${BUCKET}.cos.${REGION}.myqcloud.com/key
 */
struct URI
{
    Poco::URI uri;
    String endpoint;
    String region;
    String bucket;
    String key;

    bool is_https_scheme = false;

    explicit URI(const Poco::URI & uri_);
};

}

#endif
