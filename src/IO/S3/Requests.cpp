#include <IO/S3/Requests.h>

#if USE_AWS_S3

#include <Common/logger_useful.h>

namespace DB::S3
{

Aws::Http::HeaderValueCollection CopyObjectRequest::GetRequestSpecificHeaders() const
{
    auto headers = Model::CopyObjectRequest::GetRequestSpecificHeaders();
    if (provider_type != ProviderType::GCS)
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

}

#endif
