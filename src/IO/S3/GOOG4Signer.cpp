#include <IO/S3/GOOG4Signer.h>

#if USE_AWS_S3

#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpTypes.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/StringUtils.h>
#include <Common/Exception.h>
#include <base/hex.h>

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <fmt/format.h>
#include <map>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::S3
{

namespace
{

constexpr auto UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

std::string hmacSHA256(const std::string & key, const std::string & message)
{
    unsigned char out[SHA256_DIGEST_LENGTH];
    unsigned int out_len = 0;
    HMAC(EVP_sha256(),
         key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char *>(message.data()), message.size(),
         out, &out_len);
    return std::string(reinterpret_cast<char *>(out), out_len);
}

std::string sha256Hex(const std::string & data)
{
    unsigned char out[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(data.data()), data.size(), out);
    return hexString(out, SHA256_DIGEST_LENGTH);
}

}

void signRequestGOOG4(
    Aws::Http::HttpRequest & request,
    const Aws::Auth::AWSCredentials & credentials,
    std::chrono::system_clock::time_point now)
{
    const std::time_t now_t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_utc{};
    gmtime_r(&now_t, &tm_utc);
    const std::string timestamp = fmt::format(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        tm_utc.tm_year + 1900, tm_utc.tm_mon + 1, tm_utc.tm_mday,
        tm_utc.tm_hour, tm_utc.tm_min, tm_utc.tm_sec);
    const std::string datestamp = timestamp.substr(0, 8);

    request.SetHeaderValue("x-goog-date", timestamp);
    request.SetHeaderValue("x-goog-content-sha256", UNSIGNED_PAYLOAD);

    /// Canonical headers: `host` + every x-goog-* header, lowercase names, sorted.
    /// std::map keeps them sorted for us.
    std::map<std::string, std::string> signed_headers_map;
    for (const auto & [name, value] : request.GetHeaders())
    {
        std::string lower = Aws::Utils::StringUtils::ToLower(name.c_str());
        if (lower == "host" || lower.starts_with("x-goog-"))
            signed_headers_map.emplace(std::move(lower), value);
    }
    if (!signed_headers_map.contains("host"))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "GOOG4 signing requires a Host header on the request");

    std::string canonical_headers;
    std::string signed_headers;
    for (const auto & [name, value] : signed_headers_map)
    {
        canonical_headers += name + ":" + value + "\n";
        if (!signed_headers.empty())
            signed_headers += ";";
        signed_headers += name;
    }

    /// Canonical query string: URL-encoded key=value pairs sorted by key; a parameter without a
    /// value still gets a trailing `=` (e.g. `versioning=`).
    ///
    /// `Aws::Http::URI` has no ready-made helper for this: `CanonicalizeQueryString` only rewrites
    /// the query string when it already contains an `=`, so a bare flag like `?versioning` (no `=`)
    /// passes through unsorted and unencoded. `GetQueryStringParameters` doesn't help either — for
    /// a valueless flag it has no `=` to split on, so it treats the whole `key` as the `value` too
    /// (`versioning` becomes `versioning=versioning`, not `versioning=`). Parse the raw query string
    /// by hand instead, splitting each `key[=value]` pair on the first `=` with an empty value when
    /// absent, then URL-encode and join sorted `key=value` pairs with `&`.
    std::map<std::string, std::string> query_params;
    {
        const std::string raw_query = request.GetUri().GetQueryString();
        size_t pos = raw_query.empty() ? std::string::npos : 1; /// skip leading '?'
        while (pos != std::string::npos && pos < raw_query.size())
        {
            const size_t amp = raw_query.find('&', pos);
            const std::string pair = raw_query.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
            const size_t eq = pair.find('=');
            std::string key = eq == std::string::npos ? pair : pair.substr(0, eq);
            std::string value = eq == std::string::npos ? std::string() : pair.substr(eq + 1);
            query_params.emplace(
                Aws::Utils::StringUtils::URLDecode(key.c_str()),
                Aws::Utils::StringUtils::URLDecode(value.c_str()));
            pos = amp == std::string::npos ? std::string::npos : amp + 1;
        }
    }
    std::string canonical_query;
    for (const auto & [key, value] : query_params)
    {
        if (!canonical_query.empty())
            canonical_query += "&";
        canonical_query += Aws::Utils::StringUtils::URLEncode(key.c_str()) + "=" + Aws::Utils::StringUtils::URLEncode(value.c_str());
    }
    const std::string canonical_uri = request.GetUri().GetURLEncodedPath();

    const std::string method = Aws::Http::HttpMethodMapper::GetNameForHttpMethod(request.GetMethod());

    const std::string canonical_request = fmt::format(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method, canonical_uri, canonical_query, canonical_headers, signed_headers, UNSIGNED_PAYLOAD);

    const std::string scope = fmt::format("{}/auto/storage/goog4_request", datestamp);
    const std::string string_to_sign = fmt::format(
        "GOOG4-HMAC-SHA256\n{}\n{}\n{}", timestamp, scope, sha256Hex(canonical_request));

    std::string key = hmacSHA256("GOOG4" + credentials.GetAWSSecretKey(), datestamp);
    key = hmacSHA256(key, "auto");
    key = hmacSHA256(key, "storage");
    key = hmacSHA256(key, "goog4_request");
    const std::string signature = hexString(hmacSHA256(key, string_to_sign).data(), SHA256_DIGEST_LENGTH);

    request.SetHeaderValue("authorization", fmt::format(
        "GOOG4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        credentials.GetAWSAccessKeyId(), scope, signed_headers, signature));
}

}

#endif
