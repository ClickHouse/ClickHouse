#pragma once

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/URI.h>
#include <IO/S3/ProviderType.h>

#include <aws/core/endpoint/EndpointParameter.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectTaggingRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectTaggingRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ChecksumAlgorithm.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/core/utils/HashingUtils.h>

#include <base/defines.h>

#include <optional>

namespace DB::S3
{

namespace Model = Aws::S3::Model;
struct S3RequestSettings;

/// Used for `S3Express` and user-requested upload checksums.
namespace RequestChecksum
{
enum class Algorithm
{
    /// User-visible `MD5` mode. AWS represents it through the SDK `Content-MD5` path,
    /// not through `ChecksumAlgorithm` / `x-amz-checksum-*`.
    /// With checksums disabled it means no checksum at all.
    MD5,
    CRC32,
    SHA256,
};

inline bool usesFlexibleChecksumHeader(Algorithm algorithm)
{
    return algorithm == Algorithm::CRC32 || algorithm == Algorithm::SHA256;
}

inline std::optional<Aws::S3::Model::ChecksumAlgorithm> toSDKFlexibleChecksumAlgorithm(Algorithm algorithm)
{
    switch (algorithm)
    {
        case Algorithm::CRC32:
            return Model::ChecksumAlgorithm::CRC32;
        case Algorithm::SHA256:
            return Model::ChecksumAlgorithm::SHA256;
        case Algorithm::MD5:
            return std::nullopt;
    }
    UNREACHABLE();
}

inline void setPartChecksum(Model::CompletedPart & part, Algorithm algorithm, const std::string & checksum)
{
    switch (algorithm)
    {
        case Algorithm::CRC32:
            part.SetChecksumCRC32(checksum);
            return;
        case Algorithm::SHA256:
            part.SetChecksumSHA256(checksum);
            return;
        case Algorithm::MD5:
            return;
    }
    UNREACHABLE();
}

inline void setRequestChecksum(Model::UploadPartRequest & req, Algorithm algorithm, const std::string & checksum)
{
    switch (algorithm)
    {
        case Algorithm::CRC32:
            req.SetChecksumCRC32(checksum);
            return;
        case Algorithm::SHA256:
            req.SetChecksumSHA256(checksum);
            return;
        case Algorithm::MD5:
            return;
    }
    UNREACHABLE();
}

inline std::optional<std::string> calculateFlexibleChecksum(Model::UploadPartRequest & req, Algorithm algorithm)
{
    const auto sdk_algorithm = toSDKFlexibleChecksumAlgorithm(algorithm);
    if (!sdk_algorithm)
        return std::nullopt;

    chassert(req.GetChecksumAlgorithm() == *sdk_algorithm);
    switch (algorithm)
    {
        case Algorithm::CRC32:
            return Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateCRC32(*(req.GetBody())));
        case Algorithm::SHA256:
            return Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateSHA256(*(req.GetBody())));
        case Algorithm::MD5:
            return std::nullopt;
    }
    UNREACHABLE();
}

Algorithm getUploadChecksumAlgorithm(const S3RequestSettings & request_settings, bool is_s3express_bucket);

template <typename R>
inline void setChecksumAlgorithm(R & request, Algorithm algorithm)
{
    if constexpr (requires { request.SetChecksumAlgorithm(Model::ChecksumAlgorithm::CRC32); })
    {
        if (auto sdk_algorithm = toSDKFlexibleChecksumAlgorithm(algorithm))
            request.SetChecksumAlgorithm(*sdk_algorithm);
    }
}
};

template <typename BaseRequest>
class ExtendedRequest : public BaseRequest
{
public:
    Aws::Endpoint::EndpointParameters GetEndpointContextParams() const override
    {
        auto params = BaseRequest::GetEndpointContextParams();
        if (!region_override.empty())
            params.emplace_back("Region", region_override);

        if (uri_override.has_value())
        {
            static const Aws::String AWS_S3_FORCE_PATH_STYLE = "ForcePathStyle";
            params.emplace_back(AWS_S3_FORCE_PATH_STYLE, !uri_override->is_virtual_hosted_style);
            params.emplace_back("Endpoint", uri_override->endpoint);
        }

        return params;
    }

    Aws::String GetChecksumAlgorithmName() const override
    {
        chassert(!is_s3express_bucket || checksum);

        /// Return empty string is enough to disable checksums (see
        /// AWSClient::AddChecksumToRequest [1] for more details).
        ///
        ///   [1]: https://github.com/aws/aws-sdk-cpp/blob/b0ee1c0d336dbb371c34358b68fba6c56aae2c92/src/aws-cpp-sdk-core/source/client/AWSClient.cpp#L783-L839
        if (!hasRequestChecksum() && !checksum)
            return "";
        return BaseRequest::GetChecksumAlgorithmName();
    }

    /// TODO Understand what is it. Maybe we need it...
    bool IsStreaming() const override
    {
        return false;
    }

    std::string getRegionOverride() const
    {
        return region_override;
    }

    void overrideRegion(std::string region) const
    {
        region_override = std::move(region);
    }

    void overrideURI(S3::URI uri) const
    {
        uri_override = std::move(uri);
    }

    const auto & getURIOverride() const
    {
        return uri_override;
    }

    void setApiMode(ApiMode api_mode_) const
    {
        api_mode = api_mode_;
    }

    /// Disable checksum to avoid extra read of the input stream
    void disableChecksum() const { checksum = false; }

    void setIsS3ExpressBucket()
    {
        is_s3express_bucket = true;
        /// `S3Express` requires a flexible checksum. Keep an explicit `CRC32`/`SHA256` if one was already set
        /// via `setUploadChecksumAlgorithm`; otherwise default to `CRC32`. This runs when the request is sent,
        /// after the upload algorithm has been chosen, so it must not clobber that choice.
        if (!RequestChecksum::usesFlexibleChecksumHeader(upload_checksum_algorithm))
            setUploadChecksumAlgorithm(RequestChecksum::Algorithm::CRC32);
    }

    void setUploadChecksumAlgorithm(RequestChecksum::Algorithm algorithm)
    {
        if (!RequestChecksum::usesFlexibleChecksumHeader(algorithm))
            return;

        upload_checksum_algorithm = algorithm;
        RequestChecksum::setChecksumAlgorithm(*this, algorithm);
    }

    bool hasRequestChecksum() const
    {
        return RequestChecksum::usesFlexibleChecksumHeader(upload_checksum_algorithm);
    }

protected:
    mutable std::string region_override;
    mutable std::optional<S3::URI> uri_override;
    mutable ApiMode api_mode{ApiMode::AWS};
    mutable bool checksum = true;
    bool is_s3express_bucket = false;
    RequestChecksum::Algorithm upload_checksum_algorithm = RequestChecksum::Algorithm::MD5;
};

class CopyObjectRequest : public ExtendedRequest<Model::CopyObjectRequest>
{
public:
    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;
};

class HeadObjectRequest: public ExtendedRequest<Model::HeadObjectRequest>
{
public:
    void SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue) override;
};

using ListObjectsV2Request = ExtendedRequest<Model::ListObjectsV2Request>;
using ListObjectsRequest = ExtendedRequest<Model::ListObjectsRequest>;
using GetObjectRequest = ExtendedRequest<Model::GetObjectRequest>;
using GetObjectTaggingRequest = ExtendedRequest<Model::GetObjectTaggingRequest>;

class UploadPartRequest : public ExtendedRequest<Model::UploadPartRequest>
{
public:
    void SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue) override;
    bool RequestChecksumRequired() const override { return hasRequestChecksum(); }
    bool ShouldComputeContentMd5() const override { return !hasRequestChecksum() && checksum; }
};

class PutObjectRequest : public ExtendedRequest<Model::PutObjectRequest>
{
public:
    bool RequestChecksumRequired() const override { return hasRequestChecksum(); }
    bool ShouldComputeContentMd5() const override { return !hasRequestChecksum() && checksum; }
};

class CompleteMultipartUploadRequest : public ExtendedRequest<Model::CompleteMultipartUploadRequest>
{
public:
    void SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue) override;
};

using CreateMultipartUploadRequest = ExtendedRequest<Model::CreateMultipartUploadRequest>;
using AbortMultipartUploadRequest = ExtendedRequest<Model::AbortMultipartUploadRequest>;
using UploadPartCopyRequest = ExtendedRequest<Model::UploadPartCopyRequest>;
using PutObjectTaggingRequest = ExtendedRequest<Model::PutObjectTaggingRequest>;

class DeleteObjectRequest : public ExtendedRequest<Model::DeleteObjectRequest>
{
public:
    bool RequestChecksumRequired() const override { return is_s3express_bucket; }
    bool ShouldComputeContentMd5() const override { return !is_s3express_bucket && checksum; }
};

class DeleteObjectsRequest : public ExtendedRequest<Model::DeleteObjectsRequest>
{
public:
    bool RequestChecksumRequired() const override { return is_s3express_bucket; }
    bool ShouldComputeContentMd5() const override { return !is_s3express_bucket && checksum; }
};

class ComposeObjectRequest : public ExtendedRequest<Aws::S3::S3Request>
{
public:
    const char * GetServiceRequestName() const override { return "ComposeObject"; }

    AWS_S3_API Aws::String SerializePayload() const override;

    AWS_S3_API void AddQueryStringParameters(Aws::Http::URI & uri) const override;

    AWS_S3_API Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;

    AWS_S3_API EndpointParameters GetEndpointContextParams() const override;

    const Aws::String & GetBucket() const;
    bool BucketHasBeenSet() const;
    void SetBucket(const Aws::String & value);
    void SetBucket(Aws::String && value);
    void SetBucket(const char* value);

    const Aws::String & GetKey() const;
    bool KeyHasBeenSet() const;
    void SetKey(const Aws::String & value);
    void SetKey(Aws::String && value);
    void SetKey(const char * value);

    void SetComponentNames(Strings component_names_);

    void SetContentType(Aws::String value);
private:
    Aws::String bucket;
    Aws::String key;
    Strings component_names;
    Aws::String content_type;
};

size_t getSDKAttemptNumber(const Aws::Http::HttpRequest & request);

size_t getClickhouseAttemptNumber(const Aws::AmazonWebServiceRequest & request);
size_t getClickhouseAttemptNumber(const Aws::Http::HttpRequest & request);
void setClickhouseAttemptNumber(Aws::AmazonWebServiceRequest & request, size_t attempt);

}

#endif
