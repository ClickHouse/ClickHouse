#include <IO/S3/GCSConditionalDialect.h>

#if USE_AWS_S3

#include <Common/Exception.h>
#include <aws/core/http/HttpRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include <algorithm>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::S3
{

namespace
{

bool isAllDigits(const std::string & s)
{
    return !s.empty() && std::all_of(s.begin(), s.end(), [](char c) { return c >= '0' && c <= '9'; });
}

std::string stripQuotes(const std::string & s)
{
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"')
        return s.substr(1, s.size() - 2);
    return s;
}

}

void applyGcsConditionalDialectToRequest(Aws::Http::HttpRequest & request)
{
    const auto query_params = request.GetUri().GetQueryStringParameters();
    const bool is_complete_multipart = request.GetMethod() == Aws::Http::HttpMethod::HTTP_POST
        && query_params.contains("uploadId") && !query_params.contains("partNumber");

    /// --- Conditional headers -> x-goog-if-generation-match ---
    std::optional<std::string> generation_match;
    if (request.HasHeader("if-none-match"))
    {
        const auto value = request.GetHeaderValue("if-none-match");
        if (value != "*")
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "GCS conditional dialect: If-None-Match with a value other than '*' has no GCS "
                "equivalent (got '{}') — refusing to silently change semantics", value);
        generation_match = "0";
        request.DeleteHeader("if-none-match");
    }
    if (request.HasHeader("if-match"))
    {
        const auto value = stripQuotes(request.GetHeaderValue("if-match"));
        if (!isAllDigits(value))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "GCS conditional dialect: If-Match value '{}' is not a generation number — an "
                "ETag-kind token leaked into a generation-dialect client (mixed-mode misconfiguration)",
                value);
        generation_match = value;
        request.DeleteHeader("if-match");
    }
    if (generation_match)
    {
        if (is_complete_multipart)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "GCS conditional dialect: a CONDITIONAL CompleteMultipartUpload was about to be sent. "
                "GCS silently ignores preconditions on CompleteMultipartUpload (measured 2026-07-03) — "
                "this would be silent data loss. Conditional writes must use the single-PUT path.");
        request.SetHeaderValue("x-goog-if-generation-match", *generation_match);
    }

    /// --- AWS auth artifacts: drop (the GCS-mode client re-authenticates after this call) ---
    for (const auto * header : {"authorization", "x-amz-date", "x-amz-content-sha256",
                                "x-amz-security-token", "x-amz-api-version"})
        request.DeleteHeader(header);

    /// --- Rename every remaining x-amz-* header to x-goog-* (mixing is rejected by GCS) ---
    std::vector<std::pair<std::string, std::string>> renamed;
    for (const auto & [name, value] : request.GetHeaders())
    {
        if (name.starts_with("x-amz-"))
            renamed.emplace_back("x-goog-" + name.substr(6), value);
    }
    for (const auto & [goog_name, value] : renamed)
    {
        request.DeleteHeader(("x-amz-" + goog_name.substr(7)).c_str());
        request.SetHeaderValue(goog_name.c_str(), value);
    }
}

std::optional<std::string> gcsGenerationETagOverride(const Poco::Net::HTTPResponse & response)
{
    if (!response.has("x-goog-generation"))
        return std::nullopt;
    return "\"" + response.get("x-goog-generation") + "\"";
}

}

#endif
