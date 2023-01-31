#pragma once

#include "config.h"

#if USE_AWS_S3

#include <IO/S3/URI.h>

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

class S3Client;

template <typename BaseRequest>
class RequestWithClient : public BaseRequest
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

protected:
    mutable std::string region_override;
    mutable std::optional<S3::URI> uri_override;
};

using HeadObjectRequest = RequestWithClient<Model::HeadObjectRequest>;
using ListObjectsV2Request = RequestWithClient<Model::ListObjectsV2Request>;
using ListObjectsRequest = RequestWithClient<Model::ListObjectsRequest>;
using GetObjectRequest = RequestWithClient<Model::GetObjectRequest>;

using CreateMultipartUploadRequest = RequestWithClient<Model::CreateMultipartUploadRequest>;
using CompleteMultipartUploadRequest = RequestWithClient<Model::CompleteMultipartUploadRequest>;
using AbortMultipartUploadRequest = RequestWithClient<Model::AbortMultipartUploadRequest>;
using UploadPartRequest = RequestWithClient<Model::UploadPartRequest>;
using UploadPartCopyRequest = RequestWithClient<Model::UploadPartCopyRequest>;

using PutObjectRequest = RequestWithClient<Model::PutObjectRequest>;
using CopyObjectRequest = RequestWithClient<Model::CopyObjectRequest>;
using DeleteObjectRequest = RequestWithClient<Model::DeleteObjectRequest>;
using DeleteObjectsRequest = RequestWithClient<Model::DeleteObjectsRequest>;

}

#endif
