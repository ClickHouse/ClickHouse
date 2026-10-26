#include "config.h"

#include <Server/HTTP/AWSSignatureV4.h>

#include <Server/HTTP/HTTPServerRequest.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/Hex.h>
#include <Common/StringUtils.h>

#include <Poco/String.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/trim.hpp>

#if USE_SSL
#    include <Common/OpenSSLHelpers.h>
#    include <openssl/evp.h>
#endif

#include <algorithm>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int AUTHENTICATION_FAILED;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int NOT_IMPLEMENTED;
    extern const int OPENSSL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

constexpr std::string_view algorithm = "AWS4-HMAC-SHA256";
constexpr std::string_view unsigned_payload = "UNSIGNED-PAYLOAD";
constexpr std::string_view streaming_unsigned_payload_trailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
/// SHA-256 of the empty string.
constexpr std::string_view empty_payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// The maximum difference between the time of the request and the time of the server, as in S3.
constexpr time_t max_clock_skew_seconds = 15 * 60;
/// The maximum lifetime of a presigned URL, as in S3.
constexpr Int64 max_presigned_url_expiration_seconds = 7 * 24 * 3600;

[[noreturn]] void throwInvalid(std::string_view message)
{
    throw Exception(ErrorCodes::AUTHENTICATION_FAILED, "Invalid AWS Signature Version 4 authentication: {}", message);
}

String percentDecode(std::string_view str)
{
    String res;
    res.reserve(str.size());
    for (size_t i = 0; i < str.size(); ++i)
    {
        if (str[i] == '%')
        {
            if (i + 2 >= str.size() || !isHexDigit(str[i + 1]) || !isHexDigit(str[i + 2]))
                throwInvalid("malformed percent-encoding in the URL");
            res += static_cast<char>(unhex2(&str[i + 1]));
            i += 2;
        }
        else
            res += str[i];
    }
    return res;
}

/// URI encoding as specified by AWS: every byte except the unreserved characters is encoded as `%XY` with uppercase hex.
String awsURIEncode(std::string_view str)
{
    String res;
    res.reserve(str.size());
    for (char c : str)
    {
        if (isAlphaNumericASCII(c) || c == '-' || c == '_' || c == '.' || c == '~')
        {
            res += c;
        }
        else
        {
            res += '%';
            res += hexDigitUppercase(static_cast<unsigned char>(c) >> 4);
            res += hexDigitUppercase(static_cast<unsigned char>(c) & 0xF);
        }
    }
    return res;
}

#if USE_SSL
String sha256Hex(std::string_view data)
{
    String binary = encodeSHA256(data);
    String res(binary.size() * 2, '\0');
    encodeHexStringLower(reinterpret_cast<uint8_t *>(res.data()), reinterpret_cast<const uint8_t *>(binary.data()), binary.size());
    return res;
}
#else
String sha256Hex(std::string_view)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "AWS Signature Version 4 authentication requires ClickHouse to be built with SSL");
}
#endif

bool isLowercaseHexSHA256(std::string_view str)
{
    return str.size() == 64 && std::ranges::all_of(str, [](char c) { return isNumericASCII(c) || (c >= 'a' && c <= 'f'); });
}

/// Parses a timestamp in the ISO 8601 basic format used by AWS, e.g. `20130524T000000Z`.
time_t parseAMZDate(const String & str)
{
    if (str.size() != 16 || str[8] != 'T' || str[15] != 'Z'
        || !std::all_of(str.begin(), str.begin() + 8, isNumericASCII)
        || !std::all_of(str.begin() + 9, str.begin() + 15, isNumericASCII))
        throwInvalid(fmt::format("the date '{}' is not in the YYYYMMDD'T'HHMMSS'Z' format", str));

    auto num = [&](size_t pos, size_t len)
    {
        UInt32 res = 0;
        for (size_t i = pos; i < pos + len; ++i)
            res = res * 10 + (str[i] - '0');
        return res;
    };

    UInt32 year = num(0, 4);
    UInt32 month = num(4, 2);
    UInt32 day = num(6, 2);
    UInt32 hour = num(9, 2);
    UInt32 minute = num(11, 2);
    UInt32 second = num(13, 2);
    if (year < 1970 || year > 2200 || month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59)
        throwInvalid(fmt::format("the date '{}' is out of range", str));

    return DateLUT::instance("UTC").makeDateTime(
        static_cast<Int16>(year), static_cast<UInt8>(month), static_cast<UInt8>(day),
        static_cast<UInt8>(hour), static_cast<UInt8>(minute), static_cast<UInt8>(second));
}

std::string_view getQueryString(const HTTPServerRequest & request)
{
    std::string_view uri = request.getURI();
    auto pos = uri.find('?');
    if (pos == std::string_view::npos)
        return {};
    return uri.substr(pos + 1);
}

std::string_view getRawPath(const HTTPServerRequest & request)
{
    std::string_view uri = request.getURI();
    return uri.substr(0, uri.find('?'));
}

/// Decoded query parameters in the order of appearance, including the repeated ones.
std::vector<std::pair<String, String>> parseQueryString(std::string_view query)
{
    /// `HTMLForm`, which provides the parameters to the query, decodes a raw `+` as a space and removes a raw UTF-8 BOM,
    /// while in the signature they would mean themselves. Such a query string could be changed without changing
    /// the signature, so these characters must be percent-encoded, as AWS clients do.
    if (!std::ranges::all_of(query, [](char c) { return isPrintableASCII(c) && c != '+'; }))
        throwInvalid("the query string must not contain '+', control or non-ASCII characters, they have to be percent-encoded");

    std::vector<std::pair<String, String>> res;
    std::vector<std::string_view> parts;
    boost::split(parts, query, boost::is_any_of("&"));
    for (auto part : parts)
    {
        if (part.empty())
            continue;
        auto eq = part.find('=');
        if (eq == std::string_view::npos)
            res.emplace_back(percentDecode(part), "");
        else
            res.emplace_back(percentDecode(part.substr(0, eq)), percentDecode(part.substr(eq + 1)));
    }
    return res;
}

const String * findParameter(const std::vector<std::pair<String, String>> & params, std::string_view name)
{
    const String * res = nullptr;
    for (const auto & [key, value] : params)
    {
        if (key == name)
        {
            if (res)
                throwInvalid(fmt::format("the query parameter {} is specified more than once", name));
            res = &value;
        }
    }
    return res;
}

const String & getParameter(const std::vector<std::pair<String, String>> & params, std::string_view name)
{
    const auto * res = findParameter(params, name);
    if (!res)
        throwInvalid(fmt::format("the query parameter {} is missing", name));
    return *res;
}

/// The value trimmed, with sequential spaces replaced by one.
String canonicalizeHeaderValue(const String & header_value)
{
    String value;
    bool prev_space = false;
    for (char c : boost::algorithm::trim_copy(header_value))
    {
        bool space = c == ' ' || c == '\t';
        if (space && prev_space)
            continue;
        value += space ? ' ' : c;
        prev_space = space;
    }
    return value;
}

/// The canonical values of all the headers with this name, joined with commas.
std::optional<String> getCanonicalHeaderValue(const HTTPServerRequest & request, const String & name)
{
    std::optional<String> res;
    for (const auto & [header_name, header_value] : request)
    {
        if (Poco::icompare(header_name, name) != 0)
            continue;

        String value = canonicalizeHeaderValue(header_value);
        if (res)
            *res += "," + value;
        else
            res = std::move(value);
    }
    return res;
}

struct SignatureComponents
{
    String credential;
    String signed_headers;
    String signature;
    String amz_date;
};

SignatureComponents parseAuthorizationHeader(const HTTPServerRequest & request)
{
    String header = request.get("Authorization");
    if (!header.starts_with(algorithm) || header.size() <= algorithm.size() || header[algorithm.size()] != ' ')
        throwInvalid("the Authorization header does not start with AWS4-HMAC-SHA256");

    SignatureComponents res;
    std::vector<std::string_view> parts;
    std::string_view rest = std::string_view(header).substr(algorithm.size() + 1);
    boost::split(parts, rest, boost::is_any_of(","));
    for (auto part : parts)
    {
        String component = boost::algorithm::trim_copy(String(part));
        auto eq = component.find('=');
        if (eq == String::npos)
            throwInvalid(fmt::format("unexpected component '{}' in the Authorization header", component));
        String key = component.substr(0, eq);
        String value = component.substr(eq + 1);

        String * dest = nullptr;
        if (key == "Credential")
            dest = &res.credential;
        else if (key == "SignedHeaders")
            dest = &res.signed_headers;
        else if (key == "Signature")
            dest = &res.signature;
        else
            throwInvalid(fmt::format("unexpected component '{}' in the Authorization header", key));

        if (!dest->empty())
            throwInvalid(fmt::format("the component '{}' is specified more than once in the Authorization header", key));
        *dest = std::move(value);
    }

    if (res.credential.empty() || res.signed_headers.empty() || res.signature.empty())
        throwInvalid("the Authorization header must contain Credential, SignedHeaders and Signature");

    res.amz_date = request.get("X-Amz-Date", "");
    if (res.amz_date.empty())
        throwInvalid("the X-Amz-Date header is missing");

    return res;
}

/// Builds `<HTTPMethod>\n<CanonicalURI>\n<CanonicalQueryString>\n<CanonicalHeaders>\n<SignedHeaders>\n<HashedPayload>`.
String buildCanonicalRequest(
    const HTTPServerRequest & request,
    const std::vector<std::pair<String, String>> & params,
    bool presigned,
    const std::vector<String> & signed_headers,
    const String & signed_headers_str,
    const String & payload_hash)
{
    String res = request.getMethod();
    res += '\n';

    /// The path is encoded once (S3 does not apply the double encoding used by the other AWS services),
    /// and it is not normalized. Each component is decoded and encoded separately: `parseHTTPPath` splits the path
    /// by the literal slashes, and an encoded slash (`%2F`) is a part of a component, so they must not be confused.
    std::string_view raw_path = getRawPath(request);
    for (size_t begin = 0;;)
    {
        size_t end = raw_path.find('/', begin);
        res += awsURIEncode(percentDecode(raw_path.substr(begin, end == std::string_view::npos ? end : end - begin)));
        if (end == std::string_view::npos)
            break;
        res += '/';
        begin = end + 1;
    }
    res += '\n';

    std::vector<std::pair<String, String>> canonical_params;
    for (const auto & [key, value] : params)
    {
        if (presigned && key == "X-Amz-Signature")
            continue;
        canonical_params.emplace_back(awsURIEncode(key), awsURIEncode(value));
    }
    std::sort(canonical_params.begin(), canonical_params.end());
    for (size_t i = 0; i < canonical_params.size(); ++i)
    {
        if (i)
            res += '&';
        res += canonical_params[i].first;
        res += '=';
        res += canonical_params[i].second;
    }
    res += '\n';

    for (const auto & name : signed_headers)
    {
        auto value = getCanonicalHeaderValue(request, name);
        if (!value)
            throwInvalid(fmt::format("the signed header '{}' is missing", name));
        res += name;
        res += ':';
        res += *value;
        res += '\n';
    }
    res += '\n';

    res += signed_headers_str;
    res += '\n';
    res += payload_hash;
    return res;
}

#if USE_SSL
/// Verifies SHA-256 of the data read through it at the end of the data.
class PayloadHashVerifyingReadBuffer : public ReadBuffer
{
public:
    PayloadHashVerifyingReadBuffer(std::unique_ptr<ReadBuffer> in_, String expected_hash_)
        : ReadBuffer(nullptr, 0)
        , in(std::move(in_))
        , expected_hash(std::move(expected_hash_))
        , ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free)
    {
        if (!ctx || !EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr))
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot initialize SHA-256: {}", getOpenSSLErrors());
    }

private:
    bool nextImpl() override
    {
        /// The data of the nested buffer that is not handed out yet (it can be there before the first call) is used
        /// first, and the data that has been handed out is consumed.
        if (handed_out)
            in->position() = in->buffer().end();
        handed_out = true;

        if (!in->hasPendingData() && !in->next())
        {
            verify();
            return false;
        }

        if (!EVP_DigestUpdate(ctx.get(), in->position(), in->buffer().end() - in->position()))
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot calculate SHA-256: {}", getOpenSSLErrors());

        working_buffer = Buffer(in->position(), in->buffer().end());
        pos = working_buffer.begin();
        return true;
    }

    void verify()
    {
        if (verified)
            return;
        verified = true;

        unsigned char digest[EVP_MAX_MD_SIZE];
        unsigned int digest_size = 0;
        if (!EVP_DigestFinal_ex(ctx.get(), digest, &digest_size))
            throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot calculate SHA-256: {}", getOpenSSLErrors());

        String actual_hash(digest_size * 2, '\0');
        encodeHexStringLower(reinterpret_cast<uint8_t *>(actual_hash.data()), digest, digest_size);
        if (actual_hash != expected_hash)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The SHA-256 of the request body ({}) does not match the value of the x-amz-content-sha256 header ({})",
                actual_hash, expected_hash);
    }

    std::unique_ptr<ReadBuffer> in;
    String expected_hash;
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx;
    bool handed_out = false;
    bool verified = false;
};
#endif

/// Decodes the `aws-chunked` content encoding without chunk signatures:
///     <hex size>\r\n<data>\r\n ... 0\r\n<trailer headers>\r\n\r\n
/// The trailer headers (checksums of the data) are skipped.
/// See https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
class AWSChunkedReadBuffer : public ReadBuffer
{
public:
    AWSChunkedReadBuffer(std::unique_ptr<ReadBuffer> in_, std::optional<UInt64> decoded_content_length_)
        : ReadBuffer(nullptr, 0)
        , in(std::move(in_))
        , decoded_content_length(decoded_content_length_)
    {
    }

private:
    bool nextImpl() override
    {
        if (finished)
            return false;

        if (remaining_in_chunk == 0)
        {
            if (!first_chunk)
                assertString("\r\n", *in);
            first_chunk = false;

            String line = readLine();
            /// Chunk extensions (`;chunk-signature=...`) are not expected for unsigned payloads.
            if (line.empty() || line.size() > 16 || !std::ranges::all_of(line, isHexDigit))
                throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Invalid chunk size '{}' in the aws-chunked request body", line);
            remaining_in_chunk = 0;
            for (char c : line)
                remaining_in_chunk = remaining_in_chunk * 16 + unhex(c);

            if (remaining_in_chunk == 0)
            {
                /// Trailer headers until an empty line.
                while (!readLine().empty())
                    ;
                finished = true;
                if (decoded_content_length && *decoded_content_length != total_decoded)
                    throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
                        "The size of the aws-chunked request body ({}) does not match the x-amz-decoded-content-length header ({})",
                        total_decoded, *decoded_content_length);
                return false;
            }
        }

        if (in->eof())
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Unexpected end of the aws-chunked request body");

        size_t size = std::min<UInt64>(remaining_in_chunk, in->available());
        working_buffer = Buffer(in->position(), in->position() + size);
        pos = working_buffer.begin();
        in->position() += size;
        remaining_in_chunk -= size;
        total_decoded += size;
        return true;
    }

    /// Reads a line terminated by CRLF, without the terminator.
    String readLine()
    {
        String line;
        while (true)
        {
            if (in->eof())
                throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Unexpected end of the aws-chunked request body");
            char c = *in->position();
            ++in->position();
            if (c == '\r')
            {
                assertChar('\n', *in);
                return line;
            }
            line += c;
            if (line.size() > 4096)
                throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Too long line in the aws-chunked request body");
        }
    }

    std::unique_ptr<ReadBuffer> in;
    std::optional<UInt64> decoded_content_length;
    UInt64 remaining_in_chunk = 0;
    UInt64 total_decoded = 0;
    bool first_chunk = true;
    bool finished = false;
};

}


bool isAWSSignatureV4Request(const HTTPServerRequest & request)
{
    if (request.has("Authorization") && request.get("Authorization").starts_with(algorithm))
        return true;

    /// A cheap check without parsing the whole query string.
    auto query = getQueryString(request);
    return query.starts_with("X-Amz-Algorithm=") || query.contains("&X-Amz-Algorithm=");
}

bool isAWSSignatureV4QueryParameter(const String & name)
{
    return (name.size() >= 6 && Poco::icompare(name.substr(0, 6), "x-amz-") == 0) || name == "x-id";
}

AWSSignatureV4Request parseAWSSignatureV4Request(const HTTPServerRequest & request, time_t now)
{
    /// `HTMLForm` and `Poco::URI`, which provide the path and the parameters to the query, treat a raw `#` as the beginning
    /// of a fragment and drop the rest, while in the signature it would mean itself. Then a signed `%23` could be replaced
    /// with `#` to remove the signed parameters following it from the execution, without changing the signature.
    if (request.getURI().contains('#'))
        throwInvalid("the request target must not contain '#', it has to be percent-encoded");

    auto params = parseQueryString(getQueryString(request));

    /// The parameters are sorted in the canonical request, so their order is not signed, while the query takes the first
    /// value of a repeated parameter (e.g. `query`) and the settings take the last one. A repeated parameter could
    /// change its meaning by reordering without changing the signature, so the repetitions are not allowed. The names are
    /// compared case-insensitively, as `HTMLForm` looks them up.
    {
        std::unordered_set<String> seen_params;
        for (const auto & [key, value] : params)
        {
            if (!seen_params.insert(Poco::toLower(key)).second)
                throwInvalid(fmt::format("the query parameter '{}' is specified more than once", key));
        }
    }
    bool has_authorization_header = request.has("Authorization");
    bool presigned = findParameter(params, "X-Amz-Algorithm") != nullptr;

    if (has_authorization_header && presigned)
        throwInvalid("it is not allowed to use the Authorization header and the presigned URL parameters simultaneously");

    SignatureComponents components;
    String payload_hash;
    if (presigned)
    {
        if (getParameter(params, "X-Amz-Algorithm") != algorithm)
            throwInvalid(fmt::format("only the {} algorithm is supported", algorithm));
        components.credential = getParameter(params, "X-Amz-Credential");
        components.signed_headers = getParameter(params, "X-Amz-SignedHeaders");
        components.signature = getParameter(params, "X-Amz-Signature");
        components.amz_date = getParameter(params, "X-Amz-Date");
        payload_hash = String(unsigned_payload);
    }
    else
    {
        components = parseAuthorizationHeader(request);
        payload_hash = getAWSSignatureV4PayloadHash(request);
    }

    if (payload_hash != unsigned_payload && payload_hash != streaming_unsigned_payload_trailer && !isLowercaseHexSHA256(payload_hash))
    {
        if (payload_hash.starts_with("STREAMING-"))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "The payload signing mode '{}' is not supported. Supported modes are: UNSIGNED-PAYLOAD, {}, "
                "or a SHA-256 of the whole payload", payload_hash, streaming_unsigned_payload_trailer);
        throwInvalid(fmt::format("invalid value of the x-amz-content-sha256 header: '{}'", payload_hash));
    }

    if (!isLowercaseHexSHA256(components.signature))
        throwInvalid("the signature must consist of 64 lowercase hex digits");

    /// Credential: <access key ID>/<date>/<region>/<service>/aws4_request.
    /// The access key ID (the user name) may contain slashes, so the components are taken from the end.
    std::vector<String> scope;
    boost::split(scope, components.credential, boost::is_any_of("/"));
    if (scope.size() < 5 || scope.back() != "aws4_request")
        throwInvalid("the credential must be in the form <access key>/<date>/<region>/<service>/aws4_request");

    AWSSignatureV4Request res;
    res.service = scope[scope.size() - 2];
    res.region = scope[scope.size() - 3];
    res.date = scope[scope.size() - 4];
    auto access_key_end = components.credential.size() - res.date.size() - res.region.size() - res.service.size() - strlen("/aws4_request") - 3;
    res.access_key_id = components.credential.substr(0, access_key_end);
    res.signature = components.signature;
    res.payload_hash = payload_hash;

    if (res.access_key_id.empty())
        throwInvalid("the access key is empty");
    if (res.service != "s3")
        throwInvalid(fmt::format("the service in the credential scope must be 's3', got '{}'", res.service));

    /// Validate the time.
    time_t request_time = parseAMZDate(components.amz_date);
    if (components.amz_date.substr(0, 8) != res.date)
        throwInvalid(fmt::format("the date in the credential scope ({}) does not match the date of the request ({})", res.date, components.amz_date));

    if (request_time > now + max_clock_skew_seconds)
        throwInvalid(fmt::format("the request time {} is too far in the future", components.amz_date));

    if (presigned)
    {
        const String & expires_str = getParameter(params, "X-Amz-Expires");
        Int64 expires = 0;
        if (!tryParse(expires, expires_str) || expires < 1 || expires > max_presigned_url_expiration_seconds)
            throwInvalid(fmt::format("X-Amz-Expires must be a number of seconds from 1 to {}", max_presigned_url_expiration_seconds));
        if (now > request_time + expires)
            throwInvalid("the presigned URL has expired");
    }
    else if (request_time + max_clock_skew_seconds < now)
    {
        throwInvalid(fmt::format("the request time {} is too far in the past", components.amz_date));
    }

    /// Validate the signed headers.
    std::vector<String> signed_headers;
    boost::split(signed_headers, components.signed_headers, boost::is_any_of(";"));
    for (const auto & name : signed_headers)
    {
        if (name.empty() || std::ranges::any_of(name, [](char c) { return isUpperAlphaASCII(c); }))
            throwInvalid(fmt::format("invalid list of signed headers: '{}'", components.signed_headers));
    }
    if (!std::ranges::is_sorted(signed_headers) || std::ranges::adjacent_find(signed_headers) != signed_headers.end())
        throwInvalid(fmt::format("the list of signed headers must be sorted and must not contain duplicates: '{}'", components.signed_headers));
    if (!std::ranges::binary_search(signed_headers, String("host")))
        throwInvalid("the host header must be signed");

    /// The headers which change the meaning of the request must be signed, otherwise they could be added to a
    /// presigned URL by anyone who has got it.
    /// `Content-Type` and `Content-Encoding` change the interpretation of the body. With a presigned URL the body
    /// is not signed and is accepted only as the data of an upload into a table, and `multipart/form-data` is not
    /// accepted at all (see `HTTPHandler::processQuery`), so it is not required for them there.
    /// The values of such headers are used as is, so they must be already canonical: otherwise different values,
    /// e.g. database names with a different number of spaces, or two headers instead of one with a comma, would
    /// have the same signature.
    std::unordered_set<String> seen_headers;
    for (const auto & [header_name, header_value] : request)
    {
        String lower_name = Poco::toLower(header_name);
        bool must_be_signed = lower_name.starts_with("x-clickhouse-")
            || (!presigned && (lower_name == "x-amz-content-sha256" || lower_name == "x-amz-date"
                || lower_name == "content-type" || lower_name == "content-encoding"));
        if (!must_be_signed)
            continue;
        if (!std::ranges::binary_search(signed_headers, lower_name))
            throwInvalid(fmt::format("the header '{}' must be signed", header_name));
        if (!seen_headers.insert(lower_name).second)
            throwInvalid(fmt::format("the header '{}' is specified more than once", header_name));
        if (canonicalizeHeaderValue(header_value) != header_value)
            throwInvalid(fmt::format(
                "the value of the header '{}' must not contain tabs, sequential spaces, leading or trailing whitespace", header_name));
    }

    String canonical_request = buildCanonicalRequest(request, params, presigned, signed_headers, components.signed_headers, payload_hash);

    res.string_to_sign = fmt::format("{}\n{}\n{}/{}/{}/aws4_request\n{}",
        algorithm, components.amz_date, res.date, res.region, res.service, sha256Hex(canonical_request));

    return res;
}

String getAWSSignatureV4PayloadHash(const HTTPServerRequest & request)
{
    if (!request.has("Authorization"))
        return String(unsigned_payload);

    /// S3 clients always send this header. Generic SigV4 clients may omit it, in which case only an empty body is allowed.
    return request.get("X-Amz-Content-Sha256", String(empty_payload_hash));
}

bool isAWSSignatureV4PayloadSigned(const String & payload_hash)
{
    return isLowercaseHexSHA256(payload_hash);
}

std::unique_ptr<ReadBuffer> wrapAWSSignatureV4RequestBody(
    std::unique_ptr<ReadBuffer> body, const String & payload_hash, const HTTPServerRequest & request)
{
    if (payload_hash == unsigned_payload)
        return body;

    if (payload_hash == streaming_unsigned_payload_trailer)
    {
        std::optional<UInt64> decoded_content_length;
        if (request.has("X-Amz-Decoded-Content-Length"))
            decoded_content_length = parse<UInt64>(request.get("X-Amz-Decoded-Content-Length"));
        return std::make_unique<AWSChunkedReadBuffer>(std::move(body), decoded_content_length);
    }

#if USE_SSL
    return std::make_unique<PayloadHashVerifyingReadBuffer>(std::move(body), payload_hash);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "AWS Signature Version 4 authentication requires ClickHouse to be built with SSL");
#endif
}

bool isAWSRangeFromBeginning(const String & range)
{
    std::string_view value = range;
    if (!value.starts_with("bytes=0-"))
        return false;
    value.remove_prefix(strlen("bytes=0-"));
    return std::ranges::all_of(value, isNumericASCII);
}

String removeAWSChunkedContentEncoding(const String & content_encoding)
{
    std::vector<String> encodings;
    boost::split(encodings, content_encoding, boost::is_any_of(","));
    String res;
    for (auto & encoding : encodings)
    {
        boost::algorithm::trim(encoding);
        if (encoding.empty() || Poco::icompare(encoding, "aws-chunked") == 0)
            continue;
        if (!res.empty())
            res += ",";
        res += encoding;
    }
    return res;
}

}
