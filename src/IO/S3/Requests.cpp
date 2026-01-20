#include <IO/S3/Requests.h>

#if USE_AWS_S3

#include <Common/logger_useful.h>
#include <aws/core/endpoint/EndpointParameter.h>
#include <aws/core/utils/xml/XmlSerializer.h>

namespace DB::S3
{

Aws::Http::HeaderValueCollection CopyObjectRequest::GetRequestSpecificHeaders() const
{
    auto headers = Model::CopyObjectRequest::GetRequestSpecificHeaders();
    if (api_mode != ApiMode::GCS)
        return headers;

    /// GCS supports same headers as S3 but with a prefix x-goog instead of x-amz
    /// we have to replace all the prefixes client set internally
    const auto replace_with_gcs_header = [&](const std::string & amz_header, const std::string & gcs_header)
    {
        if (const auto it = headers.find(amz_header); it != headers.end())
        {
            auto header_value = std::move(it->second);
            headers.erase(it);
            headers.emplace(gcs_header, std::move(header_value));
        }
    };

    replace_with_gcs_header("x-amz-copy-source", "x-goog-copy-source");
    replace_with_gcs_header("x-amz-metadata-directive", "x-goog-metadata-directive");
    replace_with_gcs_header("x-amz-storage-class", "x-goog-storage-class");

    /// replace all x-amz-meta- headers
    std::vector<std::pair<std::string, std::string>> new_meta_headers;
    for (auto it = headers.begin(); it != headers.end();)
    {
        if (it->first.starts_with("x-amz-meta-"))
        {
            auto value = std::move(it->second);
            auto header = "x-goog" + it->first.substr(/* x-amz */ 5);
            new_meta_headers.emplace_back(std::pair{std::move(header), std::move(value)});
            it = headers.erase(it);
        }
        else
            ++it;
    }

    for (auto & [header, value] : new_meta_headers)
        headers.emplace(std::move(header), std::move(value));

    return headers;
}

void CompleteMultipartUploadRequest::SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue)
{
    // S3's CompleteMultipartUpload doesn't support metadata headers so we skip adding them
    if (!headerName.starts_with("x-amz-meta-"))
        Model::CompleteMultipartUploadRequest::SetAdditionalCustomHeaderValue(headerName, headerValue);
}

void UploadPartRequest::SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue)
{
    // S3's UploadPart doesn't support metadata headers so we skip adding them
    if (!headerName.starts_with("x-amz-meta-"))
        Model::UploadPartRequest::SetAdditionalCustomHeaderValue(headerName, headerValue);
}

Aws::String ComposeObjectRequest::SerializePayload() const
{
    if (component_names.empty())
        return {};

    Aws::Utils::Xml::XmlDocument payload_doc = Aws::Utils::Xml::XmlDocument::CreateWithRootNode("ComposeRequest");
    auto root_node = payload_doc.GetRootElement();

    for (const auto & name : component_names)
    {
        auto component_node = root_node.CreateChildElement("Component");
        auto name_node = component_node.CreateChildElement("Name");
        name_node.SetText(name);
    }

    return payload_doc.ConvertToString();
}


void ComposeObjectRequest::AddQueryStringParameters(Aws::Http::URI & /*uri*/) const
{
}

Aws::Http::HeaderValueCollection ComposeObjectRequest::GetRequestSpecificHeaders() const
{
    if (content_type.empty())
        return {};

    return {Aws::Http::HeaderValuePair(Aws::Http::CONTENT_TYPE_HEADER, content_type)};
}

Aws::Endpoint::EndpointParameters ComposeObjectRequest::GetEndpointContextParams() const
{
    EndpointParameters parameters;
    if (BucketHasBeenSet())
        parameters.emplace_back("Bucket", GetBucket(), Aws::Endpoint::EndpointParameter::ParameterOrigin::OPERATION_CONTEXT);

    return parameters;
}

const Aws::String & ComposeObjectRequest::GetBucket() const
{
    return bucket;
}

bool ComposeObjectRequest::BucketHasBeenSet() const
{
    return !bucket.empty();
}

void ComposeObjectRequest::SetBucket(const Aws::String & value)
{
    bucket = value;
}

void ComposeObjectRequest::SetBucket(Aws::String && value)
{
    bucket = std::move(value);
}

void ComposeObjectRequest::SetBucket(const char * value)
{
    bucket.assign(value);
}

const Aws::String & ComposeObjectRequest::GetKey() const
{
    return key;
}

bool ComposeObjectRequest::KeyHasBeenSet() const
{
    return !key.empty();
}

void ComposeObjectRequest::SetKey(const Aws::String & value)
{
    key = value;
}

void ComposeObjectRequest::SetKey(Aws::String && value)
{
    key = std::move(value);
}

void ComposeObjectRequest::SetKey(const char * value)
{
    key.assign(value);
}

void ComposeObjectRequest::SetComponentNames(std::vector<Aws::String> component_names_)
{
    component_names = std::move(component_names_);
}

void ComposeObjectRequest::SetContentType(Aws::String value)
{
    content_type = std::move(value);
}

}

#endif
