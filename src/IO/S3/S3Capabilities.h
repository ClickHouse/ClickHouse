#pragma once

#include <base/defines.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <mutex>
#include <optional>
#include <string>


namespace DB
{

/// Supported/unsupported features by different S3 implementations
/// Can be useful only for almost compatible with AWS S3 versions.
class S3Capabilities
{
public:
    explicit S3Capabilities(std::optional<bool> support_batch_delete_ = {}, bool support_proxy_ = false)
        : support_proxy(support_proxy_), support_batch_delete(support_batch_delete_)
    {
    }

    S3Capabilities(const S3Capabilities & src);

    /// Google S3 implementation doesn't support batch delete
    /// TODO: possibly we have to use Google SDK https://github.com/googleapis/google-cloud-cpp/tree/main/google/cloud/storage
    /// because looks like it misses some features:
    /// 1) batch delete (DeleteObjects)
    /// 2) upload part copy (UploadPartCopy)
    /// If `isBatchDeleteSupported()` returns `nullopt` it means that it isn't clear yet if it's supported or not
    /// and should be detected automatically from responses of the cloud storage.
    std::optional<bool> isBatchDeleteSupported() const;
    void setIsBatchDeleteSupported(bool support_batch_delete_);

    /// Y.Cloud S3 implementation support proxy for connection
    const bool support_proxy{false};

private:
    /// `support_batch_delete` is guarded by mutex because function deleteFilesFromS3() can update this field from another thread.
    /// If `support_batch_delete == nullopt` that means it's not clear yet if it's supported or not.
    std::optional<bool> support_batch_delete TSA_GUARDED_BY(mutex);

    mutable std::mutex mutex;
};

S3Capabilities getCapabilitiesFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

}
