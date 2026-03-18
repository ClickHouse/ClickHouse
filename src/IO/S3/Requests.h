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
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/ChecksumAlgorithm.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/core/utils/HashingUtils.h>

#include <base/defines.h>

namespace DB::S3
{

namespace Model = Aws::S3::Model;

/// Used only for S3Express
namespace RequestChecksum
{
inline void setPartChecksum(Model::CompletedPart & part, const std::string & checksum)
{
    part.SetChecksumCRC32(checksum);
}

inline void setRequestChecksum(Model::UploadPartRequest & req, const std::string & checksum)
{
    req.SetChecksumCRC32(checksum);
}

inline std::string calculateChecksum(Model::UploadPartRequest & req)
{
    chassert(req.GetChecksumAlgorithm() == Aws::S3::Model::ChecksumAlgorithm::CRC32);
    return Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateCRC32(*(req.GetBody())));
}

template <typename R>
inline void setChecksumAlgorithm(R & request)
{
    if constexpr (requires { request.SetChecksumAlgorithm(Model::ChecksumAlgorithm::CRC32); })
        request.SetChecksumAlgorithm(Model::ChecksumAlgorithm::CRC32);
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
        if (!is_s3express_bucket && !checksum)
            return "";
        return BaseRequest::GetChecksumAlgorithmName();
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
        RequestChecksum::setChecksumAlgorithm(*this);
    }

protected:
    mutable std::string region_override;
    mutable std::optional<S3::URI> uri_override;
    mutable ApiMode api_mode{ApiMode::AWS};
    mutable bool checksum = true;
    bool is_s3express_bucket = false;
};

class CopyObjectRequest : public ExtendedRequest<Model::CopyObjectRequest>
{
public:
    Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;
};

using HeadObjectRequest = ExtendedRequest<Model::HeadObjectRequest>;
using ListObjectsV2Request = ExtendedRequest<Model::ListObjectsV2Request>;
using ListObjectsRequest = ExtendedRequest<Model::ListObjectsRequest>;
using GetObjectRequest = ExtendedRequest<Model::GetObjectRequest>;

class UploadPartRequest : public ExtendedRequest<Model::UploadPartRequest>
{
public:
    void SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue) override;
};

class CompleteMultipartUploadRequest : public ExtendedRequest<Model::CompleteMultipartUploadRequest>
{
public:
    void SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue) override;
};

using CreateMultipartUploadRequest = ExtendedRequest<Model::CreateMultipartUploadRequest>;
using AbortMultipartUploadRequest = ExtendedRequest<Model::AbortMultipartUploadRequest>;
using UploadPartCopyRequest = ExtendedRequest<Model::UploadPartCopyRequest>;

using PutObjectRequest = ExtendedRequest<Model::PutObjectRequest>;
using DeleteObjectRequest = ExtendedRequest<Model::DeleteObjectRequest>;
using DeleteObjectsRequest = ExtendedRequest<Model::DeleteObjectsRequest>;


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

    void SetComponentNames(std::vector<Aws::String> component_names_);

    void SetContentType(Aws::String value);
private:
    Aws::String bucket;
    Aws::String key;
    std::vector<Aws::String> component_names;
    Aws::String content_type;
};

}

#endif
