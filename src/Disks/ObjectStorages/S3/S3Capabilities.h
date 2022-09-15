#pragma once

#include <string>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Supported/unsupported features by different S3 implementations
/// Can be useful only for almost compatible with AWS S3 versions.
struct S3Capabilities
{
    /// Google S3 implementation doesn't support batch delete
    /// TODO: possibly we have to use Google SDK https://github.com/googleapis/google-cloud-cpp/tree/main/google/cloud/storage
    /// because looks like it miss a lot of features like:
    /// 1) batch delete
    /// 2) list_v2
    /// 3) multipart upload works differently
    bool support_batch_delete{true};

    /// Y.Cloud S3 implementation support proxy for connection
    bool support_proxy{false};
};

S3Capabilities getCapabilitiesFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

}
