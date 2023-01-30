#pragma once

#include "config.h"

#if USE_AWS_S3

#include <aws/core/endpoint/EndpointParameter.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>

#include <IO/S3/S3Client.h>

namespace DB::S3
{

template <typename BaseRequest>
class RequestWithClient : public BaseRequest
{
public:
    using BaseRequestType = BaseRequest;

    Aws::Endpoint::EndpointParameters GetEndpointContextParams() const override
    {
        auto params = BaseRequest::GetEndpointContextParams();
        if (s3_client)
        {
            auto bucket = BaseRequest::GetBucket();
            if (const auto * region = s3_client->getRegionForBucket(bucket); region != nullptr)
                params.emplace_back("Region", *region);

            if (const auto * uri = s3_client->getURIForBucket(bucket); uri != nullptr)
            {
                static const Aws::String AWS_S3_FORCE_PATH_STYLE = "ForcePathStyle";
                params.emplace_back(AWS_S3_FORCE_PATH_STYLE, !uri->is_virtual_hosted_style);
                params.emplace_back("Endpoint", uri->endpoint);
            }
        }

        return params;
    }

    void setClient(const S3Client * s3_client_) const
    {
        s3_client = s3_client_;
    }
protected:
    mutable const S3Client * s3_client = nullptr;
};

using HeadObjectRequest = RequestWithClient<Model::HeadObjectRequest>;
using ListObjectsV2Request = RequestWithClient<Model::ListObjectsV2Request>;
using GetObjectRequest = RequestWithClient<Model::GetObjectRequest>;
using CreateMultipartUploadRequest = RequestWithClient<Model::CreateMultipartUploadRequest>;
using CompleteMultipartUploadRequest = RequestWithClient<Model::CompleteMultipartUploadRequest>;
using PutObjectRequest = RequestWithClient<Model::PutObjectRequest>;
using UploadPartRequest = RequestWithClient<Model::UploadPartRequest>;
using DeleteObjectRequest = RequestWithClient<Model::DeleteObjectRequest>;
using DeleteObjectsRequest = RequestWithClient<Model::DeleteObjectsRequest>;

}

#endif
