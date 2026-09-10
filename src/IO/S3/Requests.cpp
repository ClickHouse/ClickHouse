#include <IO/S3/Requests.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <cctype>

#if USE_AWS_S3

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/Crypto/OpenSSLInitializer.h>
#include <IO/S3RequestSettings.h>
#include <aws/core/endpoint/EndpointParameter.h>
#include <aws/core/utils/xml/XmlSerializer.h>

#include <string_view>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_SETTING_VALUE;
}
}

namespace DB::S3RequestSetting
{
    extern const S3RequestSettingsString upload_checksum_algorithm;
}

namespace DB::S3
{

RequestChecksum::Algorithm RequestChecksum::getUploadChecksumAlgorithm(const S3RequestSettings & request_settings, bool is_s3express_bucket)
{
    /// An explicit setting always wins.
    const auto & name = request_settings[DB::S3RequestSetting::upload_checksum_algorithm].value;
    if (!name.empty())
    {
        const auto algorithm = tryParse(name);
        if (!algorithm)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Setting upload_checksum_algorithm has invalid value {} which only supports {}",
                name, supportedAlgorithms());

        /// `MD5` selects the SDK's `Content-MD5` path, which `S3Express` and FIPS reject.
        if (*algorithm == RequestChecksum::Algorithm::MD5)
        {
            if (is_s3express_bucket)
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting upload_checksum_algorithm cannot be MD5 for S3Express buckets, "
                    "which require a flexible checksum; use CRC32 or SHA256");
            if (OpenSSLInitializer::instance().isFIPSEnabled())
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Setting upload_checksum_algorithm cannot be MD5 when FIPS mode is enabled; use CRC32 or SHA256");
        }
        return *algorithm;
    }

    /// No explicit choice: pick a default for the environment.
    if (is_s3express_bucket)
        return RequestChecksum::Algorithm::CRC32; /// flexible checksum is mandatory, `Content-MD5` not accepted

    /// Default to the SDK's `Content-MD5` path. Under FIPS the SDK silently drops it, leaving the upload with no
    /// checksum header - the pre-flexible-checksum behavior, which is kept as the default because support for
    /// `x-amz-checksum-*` outside AWS is inconsistent. Set the setting explicitly to attach one.
    return RequestChecksum::Algorithm::MD5;
}

namespace
{

/// Translated in both directions; a name added here is renamed going out and recognised coming back.
/// These are the names Google's own migration guide maps.
///
/// The rename carries the value through untouched, which is right for a copy source, for COPY or
/// REPLACE, and for the user's own metadata. It is only half the story for the storage class: the two
/// clouds share no class name but STANDARD, so an S3 one renamed onto GCS answers 400
/// InvalidStorageClass. Nothing can send a GCS class today either, because `GetStorageClassForName`
/// knows only S3 names and maps the rest to NOT_SET. Making the setting work on GCS means passing the
/// user's string through instead of the enum, which is a change of its own.
constexpr std::pair<std::string_view, std::string_view> GCS_TRANSLATED_HEADERS[] = {
    {"x-amz-copy-source", "x-goog-copy-source"},
    {"x-amz-metadata-directive", "x-goog-metadata-directive"},
    {"x-amz-storage-class", "x-goog-storage-class"},
};

/// Object metadata is a family rather than one name, so it is matched by prefix.
constexpr std::string_view AMZ_META_PREFIX = "x-amz-meta-";
constexpr std::string_view GCS_META_PREFIX = "x-goog-meta-";

}

std::optional<std::string> translateHeaderNameFromGCS(const std::string & name)
{
    for (const auto & [amz_header, gcs_header] : GCS_TRANSLATED_HEADERS)
        if (equalsCaseInsensitive(name, gcs_header))
            return std::string(amz_header);

    /// HTTP/1.1 preserves the case a server sent, so match the prefix case-insensitively. The result
    /// is lower-cased whole: the SDK turns whatever follows the prefix into the metadata key of a
    /// case-sensitive map, and both S3 and GCS store user metadata keys lower-cased.
    if (name.size() > GCS_META_PREFIX.size()
        && equalsCaseInsensitive(std::string_view(name).substr(0, GCS_META_PREFIX.size()), GCS_META_PREFIX))
    {
        auto suffix = name.substr(GCS_META_PREFIX.size());
        std::transform(suffix.begin(), suffix.end(), suffix.begin(), [](unsigned char c) { return std::tolower(c); });
        return std::string(AMZ_META_PREFIX) + suffix;
    }

    return {};
}

void normalizeHeaderNames(HTTPHeaderEntries & headers)
{
    for (auto & header : headers)
        std::transform(header.name.begin(), header.name.end(), header.name.begin(),
                       [](unsigned char c) { return std::tolower(c); });
}

void translateHeadersToGCS(Aws::Http::HttpRequest & request)
{
    const auto before = request.GetHeaders();
    const auto after = translateHeadersToGCS(before);

    for (const auto & [name, _] : before)
        if (!after.contains(name))
            request.DeleteHeader(name.c_str());

    for (const auto & [name, value] : after)
        if (!before.contains(name))
            request.SetHeaderValue(name, value);
}

Aws::Http::HeaderValueCollection translateHeadersToGCS(Aws::Http::HeaderValueCollection headers)
{
    /// GCS supports same headers as S3 but with a prefix x-goog instead of x-amz
    /// we have to replace all the prefixes client set internally
    const auto replace_with_gcs_header = [&](std::string_view amz_header, std::string_view gcs_header)
    {
        if (const auto it = headers.find(std::string(amz_header)); it != headers.end())
        {
            auto header_value = std::move(it->second);
            headers.erase(it);
            headers.emplace(std::string(gcs_header), std::move(header_value));
        }
    };

    for (const auto & [amz_header, gcs_header] : GCS_TRANSLATED_HEADERS)
        replace_with_gcs_header(amz_header, gcs_header);

    /// replace all x-amz-meta- headers
    VectorWithMemoryTracking<std::pair<std::string, std::string>> new_meta_headers;
    for (auto it = headers.begin(); it != headers.end();)
    {
        if (it->first.starts_with(AMZ_META_PREFIX))
        {
            auto value = std::move(it->second);
            auto header = std::string(GCS_META_PREFIX) + it->first.substr(AMZ_META_PREFIX.size());
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

void HeadObjectRequest::SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue)
{
    // S3's HeadObject doesn't support `x-amz-server-side-encryption` headers so we skip adding them
    // Docs: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
    if (headerName != "x-amz-server-side-encryption")
        Model::HeadObjectRequest::SetAdditionalCustomHeaderValue(headerName, headerValue);
}

void CompleteMultipartUploadRequest::SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue)
{
    // S3's CompleteMultipartUpload doesn't support metadata headers so we skip adding them
    if (!headerName.starts_with("x-amz-meta-") && (headerName != "x-amz-server-side-encryption"))
        Model::CompleteMultipartUploadRequest::SetAdditionalCustomHeaderValue(headerName, headerValue);
}

void UploadPartRequest::SetAdditionalCustomHeaderValue(const Aws::String& headerName, const Aws::String& headerValue)
{
    // S3's UploadPart doesn't support metadata headers so we skip adding them
    if (!headerName.starts_with("x-amz-meta-") && (headerName != "x-amz-server-side-encryption"))
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

void ComposeObjectRequest::SetComponentNames(Strings component_names_)
{
    component_names = std::move(component_names_);
}

void ComposeObjectRequest::SetContentType(Aws::String value)
{
    content_type = std::move(value);
}


static size_t getAttemptFromInfo(const Aws::String & request_info)
{
    static auto key = Aws::String("attempt=");

    auto key_begin = request_info.find(key, 0);
    if (key_begin == Aws::String::npos)
        return 1;

    auto val_begin = key_begin + key.size();
    auto val_end = request_info.find(';', val_begin);
    if (val_end == Aws::String::npos)
        val_end = request_info.size();

    if (val_begin == val_end)
        return 1;

    auto value = request_info.substr(val_begin, val_end - val_begin);
    try
    {
        return std::stol(value, nullptr, 10);
    }
    catch (const std::exception &)
    {
        return 1;
    }
}

static String getOrEmpty(const Aws::Http::HeaderValueCollection & map, const String & key)
{
    auto it = map.find(key);
    if (it == map.end())
        return {};
    return it->second;
}

void setClickHouseAttemptNumber(Aws::AmazonWebServiceRequest & request, size_t attempt)
{
    request.SetAdditionalCustomHeaderValue("clickhouse-request", fmt::format("attempt={}", attempt));
}

size_t getClickHouseAttemptNumber(const Aws::AmazonWebServiceRequest & request)
{
    return getAttemptFromInfo(getOrEmpty(request.GetHeaders(), "clickhouse-request"));
}

size_t getClickHouseAttemptNumber(const Aws::Http::HttpRequest & request)
{
    return getAttemptFromInfo(getOrEmpty(request.GetHeaders(), "clickhouse-request"));
}

size_t getSDKAttemptNumber(const Aws::Http::HttpRequest & request)
{
       return getAttemptFromInfo(getOrEmpty(request.GetHeaders(), Aws::Http::SDK_REQUEST_HEADER));
}
}

#endif
