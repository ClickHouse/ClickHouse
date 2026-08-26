#include <IO/S3/GCSConditionalDialect.h>

#if USE_AWS_S3

#include <Common/Exception.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <Poco/Net/HTTPResponse.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB::S3
{

namespace
{

constexpr std::string_view AMZ_PREFIX = "x-amz-";
constexpr std::string_view GOOG_PREFIX = "x-goog-";
constexpr std::string_view AMZ_META_PREFIX = "x-amz-meta-";
constexpr std::string_view GOOG_META_PREFIX = "x-goog-meta-";

/// What both GCS authentication modes clear first: the SigV4 signature and the headers it was
/// computed over, which describe a canonical request neither GOOG4 nor Bearer authentication sends,
/// plus `x-amz-api-version`, which GCS rejects and which the SDK layer only removes when it
/// recognised the endpoint as GCS.
constexpr std::array AWS_HEADERS_CLEARED_BEFORE_GCS_AUTHENTICATION{
    "authorization", "x-amz-date", "x-amz-content-sha256", "x-amz-security-token", "x-amz-api-version"};

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

std::string toLower(std::string_view s)
{
    std::string out{s};
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

/// Rename `name` to its `x-goog-` counterpart, refusing to pick a winner when the target already
/// carries a different value.
void renameToGoogPrefix(Aws::Http::HttpRequest & request, const std::string & name)
{
    const std::string value = request.GetHeaderValue(name.c_str());
    const std::string goog_name = std::string{GOOG_PREFIX} + name.substr(AMZ_PREFIX.size());
    if (request.HasHeader(goog_name.c_str()) && request.GetHeaderValue(goog_name.c_str()) != value)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "GCS request adaptation: '{}' and '{}' carry different values, so renaming would silently "
            "discard one of them", name, goog_name);
    request.DeleteHeader(name.c_str());
    request.SetHeaderValue(goog_name.c_str(), value);
}

/// What GOOG4 authentication does with one `x-amz-*` request header.
enum class Goog4Disposition : uint8_t
{
    Rename,   /// GCS accepts the same semantics under the x-goog- prefix
    Consume,  /// meaningful only to the AWS SDK; drop it, the wire request is unaffected
    Reject,   /// GCS cannot honor it and dropping it would change what the request means
};

struct Goog4HeaderRule
{
    std::string_view name;   /// matched as a prefix when `is_prefix`, otherwise exactly
    bool is_prefix;
    Goog4Disposition disposition;
};

/// Every `x-amz-*` header ClickHouse's S3 requests can carry, with the reason for its fate. A header
/// absent from this table is rejected: this path deliberately signs a request whose prefixes are all
/// `x-goog-`, so guessing a translation or passing one through are both worse than an error naming
/// the header.
constexpr std::array GOOG4_HEADER_RULES{
    /// GCS object metadata is `x-goog-meta-*`; the storage class, copy source and metadata directive
    /// are the same headers under the other prefix.
    Goog4HeaderRule{AMZ_META_PREFIX, true, Goog4Disposition::Rename},
    Goog4HeaderRule{"x-amz-storage-class", false, Goog4Disposition::Rename},
    Goog4HeaderRule{"x-amz-copy-source", false, Goog4Disposition::Rename},
    Goog4HeaderRule{"x-amz-copy-source-range", false, Goog4Disposition::Rename},
    Goog4HeaderRule{"x-amz-metadata-directive", false, Goog4Disposition::Rename},

    /// Flexible checksums are an S3 protocol feature: the algorithm selector and the computed value
    /// mean nothing to the GCS XML API, and the body they describe is sent unchanged either way.
    Goog4HeaderRule{"x-amz-sdk-checksum-algorithm", false, Goog4Disposition::Consume},
    Goog4HeaderRule{"x-amz-checksum-", true, Goog4Disposition::Consume},

    /// These two announce `aws-chunked` body framing, which GCS cannot parse. Dropping them would
    /// leave the framed body on the wire described as a plain one, so refuse instead.
    Goog4HeaderRule{"x-amz-trailer", false, Goog4Disposition::Reject},
    Goog4HeaderRule{"x-amz-decoded-content-length", false, Goog4Disposition::Reject},
};

std::optional<Goog4Disposition> goog4DispositionFor(std::string_view name)
{
    for (const auto & rule : GOOG4_HEADER_RULES)
        if (!rule.is_prefix && name == rule.name)
            return rule.disposition;
    for (const auto & rule : GOOG4_HEADER_RULES)
        if (rule.is_prefix && name.starts_with(rule.name))
            return rule.disposition;
    return std::nullopt;
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
                "GCS native-conditional request: If-None-Match with a value other than '*' has no GCS "
                "equivalent (got '{}') — refusing to silently change semantics", value);
        generation_match = "0";
        request.DeleteHeader("if-none-match");
    }
    if (request.HasHeader("if-match"))
    {
        const auto value = stripQuotes(request.GetHeaderValue("if-match"));
        if (!isAllDigits(value))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "GCS native-conditional request: If-Match value '{}' is not a generation number, so it "
                "cannot name an incarnation on this backend", value);
        generation_match = value;
        request.DeleteHeader("if-match");
    }
    if (generation_match)
    {
        if (is_complete_multipart)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "GCS native-conditional request: a CONDITIONAL CompleteMultipartUpload was about to be sent. "
                "GCS silently ignores preconditions on CompleteMultipartUpload (measured 2026-07-03) — "
                "this would be silent data loss. Conditional writes must use the single-PUT path.");
        request.SetHeaderValue("x-goog-if-generation-match", *generation_match);
    }

    /// --- Object metadata: x-amz-meta-* -> x-goog-meta-*, the prefix GCS documents ---
    std::vector<std::string> meta_headers;
    for (const auto & header : request.GetHeaders())
    {
        if (toLower(header.first).starts_with(AMZ_META_PREFIX))
            meta_headers.push_back(header.first);
    }
    for (const auto & name : meta_headers)
        renameToGoogPrefix(request, name);
}

void prepareGcsRequestForOAuthAuthentication(Aws::Http::HttpRequest & request)
{
    for (const auto * header : AWS_HEADERS_CLEARED_BEFORE_GCS_AUTHENTICATION)
        request.DeleteHeader(header);
}

void prepareGcsRequestForGoog4Authentication(Aws::Http::HttpRequest & request)
{
    for (const auto * header : AWS_HEADERS_CLEARED_BEFORE_GCS_AUTHENTICATION)
        request.DeleteHeader(header);

    std::vector<std::string> remaining;
    for (const auto & header : request.GetHeaders())
    {
        if (toLower(header.first).starts_with(AMZ_PREFIX))
            remaining.push_back(header.first);
    }

    for (const auto & name : remaining)
    {
        const auto disposition = goog4DispositionFor(toLower(name));
        if (!disposition)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "GOOG4 authentication: header '{}' has no known GCS XML API counterpart, so it cannot be "
                "translated. Sending it unchanged would mix the x-amz- and x-goog- prefixes in one "
                "GOOG4-signed request, and whether GCS accepts that has not been established -- so it is "
                "refused rather than guessed at. Remove it from the disk configuration, or use an "
                "AWS-compatible endpoint.",
                name);

        switch (*disposition)
        {
            case Goog4Disposition::Rename:
                renameToGoogPrefix(request, name);
                break;
            case Goog4Disposition::Consume:
                request.DeleteHeader(name.c_str());
                break;
            case Goog4Disposition::Reject:
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "GOOG4 authentication: header '{}' announces aws-chunked body framing, which the GCS "
                    "XML API cannot parse. Dropping it would misdescribe the body already on the wire.",
                    name);
        }
    }
}

void applyGcsConditionalDialectToResponse(const Poco::Net::HTTPResponse & poco_response, Aws::Http::HttpResponse & sdk_response)
{
    /// Each value is passed as a NAMED LVALUE on purpose, because the two `AddHeader` overloads of
    /// `Aws::Http::Standard::StandardHttpResponse` are not equivalent: the `const Aws::String &` one
    /// assigns through `operator[]` and replaces an existing header, while the `Aws::String &&` one
    /// calls `emplace` and silently keeps the existing value. The caller's copy loop has already
    /// installed the server's own `etag` and every `x-amz-meta-*`, so passing a temporary here would
    /// no-op and leave the response unadapted.
    if (poco_response.has("x-goog-generation"))
    {
        const std::string quoted_generation = "\"" + poco_response.get("x-goog-generation") + "\"";
        sdk_response.AddHeader("ETag", quoted_generation);
    }

    for (const auto & [name, value] : poco_response)
    {
        const std::string lower_name = toLower(name);
        if (!lower_name.starts_with(GOOG_META_PREFIX))
            continue;

        const std::string amz_name = std::string{AMZ_META_PREFIX} + lower_name.substr(GOOG_META_PREFIX.size());
        if (poco_response.has(amz_name) && poco_response.get(amz_name) != value)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "GCS native-conditional response: '{}' and '{}' carry different values, so the object's "
                "attributes are ambiguous", name, amz_name);
        const std::string mapped_value = value;
        sdk_response.AddHeader(amz_name, mapped_value);
    }
}

}

#endif
