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

namespace DB::S3
{

namespace Model = Aws::S3::Model;

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

    void setProviderType(ProviderType provider_type_) const
    {
        provider_type = provider_type_;
    }

protected:
    mutable std::string region_override;
    mutable std::optional<S3::URI> uri_override;
    mutable ProviderType provider_type{ProviderType::UNKNOWN};
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

using CreateMultipartUploadRequest = ExtendedRequest<Model::CreateMultipartUploadRequest>;
using CompleteMultipartUploadRequest = ExtendedRequest<Model::CompleteMultipartUploadRequest>;
using AbortMultipartUploadRequest = ExtendedRequest<Model::AbortMultipartUploadRequest>;
using UploadPartRequest = ExtendedRequest<Model::UploadPartRequest>;
using UploadPartCopyRequest = ExtendedRequest<Model::UploadPartCopyRequest>;

using PutObjectRequest = ExtendedRequest<Model::PutObjectRequest>;
using DeleteObjectRequest = ExtendedRequest<Model::DeleteObjectRequest>;
using DeleteObjectsRequest = ExtendedRequest<Model::DeleteObjectsRequest>;

}

#endif
