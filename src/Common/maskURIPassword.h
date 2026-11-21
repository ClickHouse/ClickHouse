#pragma once

#include <Common/re2.h>

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <utility>


namespace DB
{

/** Hides the secrets of an Azure connection string as the Azure SDK reads it (`Azure::Storage::_internal::ParseConnectionString`,
  * `contrib/azure/sdk/storage/azure-storage-common/src/storage_credential.cpp`): a key runs to '=' and its value to ';', keys
  * match exactly, and the last copy of a repeated key wins. Splitting at every ';' first can only hide more than the SDK reads.
  *   - secret keys:   `AccountName=a;AccountKey=K;SharedAccessSignature=S` -> `AccountName=a;AccountKey=[HIDDEN];SharedAccessSignature=[HIDDEN]`
  *   - repeated keys: `AccountKey=K1;AccountKey=K2` -> `AccountKey=[HIDDEN];AccountKey=[HIDDEN]`
  *   - an endpoint is a url, so a query in it is a SAS: `BlobEndpoint=https://h/?sig=S` or `BlobEndpoint=https://u@h/` -> `BlobEndpoint=[HIDDEN]`
  *   - the whole string is hidden when a '?', '#' or '@' is still shown, or when it has one and holds a url outside the pair
  *     values (':' or '/' in a key or in a part without '='; no key has them): `https://h/?sig=S` or `https://u:p=s@h/` -> `[HIDDEN]`
  * Returns whether anything was hidden.
  */
inline bool maskAzureConnectionStringSecrets(std::string & str)
{
    static constexpr std::array<std::string_view, 2> secret_keys = {"AccountKey", "SharedAccessSignature"};
    auto is_secret_key = [](std::string_view key) { return std::ranges::find(secret_keys, key) != secret_keys.end(); };

    const bool has_url_delimiters = str.find_first_of("?#@") != std::string::npos;
    bool url_outside_values = false;
    std::string result;
    size_t copied = 0;
    bool masked = false;
    for (size_t part_begin = 0; part_begin <= str.length();)
    {
        const size_t part_end = std::min(str.find(';', part_begin), str.length());
        const std::string_view part = std::string_view(str).substr(part_begin, part_end - part_begin);
        const size_t equals = part.find('=');
        if (part.substr(0, equals).find_first_of(":/") != std::string_view::npos)
            url_outside_values = true;
        if (equals != std::string_view::npos
            && (is_secret_key(part.substr(0, equals)) || part.substr(equals + 1).find_first_of("?@") != std::string_view::npos))
        {
            const size_t value_begin = part_begin + equals + 1;
            result.append(str, copied, value_begin - copied);
            result.append("[HIDDEN]");
            copied = part_end;
            masked = true;
        }
        part_begin = part_end + 1;
    }

    if (masked)
    {
        result.append(str, copied, std::string::npos);
        str = std::move(result);
    }

    if (str.find_first_of("?#@") != std::string::npos || (url_outside_values && has_url_delimiters))
    {
        str = "[HIDDEN]";
        return true;
    }
    return masked;
}

inline bool maskURIPassword(std::string * uri)
{
    return RE2::Replace(uri, R"(([^:]+://[^:]*):([^@]*)@(.*))", "\\1:[HIDDEN]@\\3");
}

/** Mask the values of the query parameters that carry credentials in a presigned URL, so that
  * `...?X-Amz-Signature=abc&foo=1` becomes `...?X-Amz-Signature=[HIDDEN]&foo=1`. Every occurrence
  * is masked. Returns whether anything was masked.
  *
  * Mirrors the regular expression
  * `([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|X-Amz-[A-Za-z0-9\-]*|X-Goog-[A-Za-z0-9\-]*)=)[^&#]*`
  * that `FunctionSecretArgumentsFinder` applies to a presigned URL, and the parameter set mirrors
  * `BackupInfo::removeCredentialsFromS3URL`. Matching is case-sensitive, as in the expression.
  */
inline bool maskPresignedURLParameters(std::string & url)
{
    static constexpr std::array<std::string_view, 4> exact_names = {"AWSAccessKeyId", "Signature", "Expires", "GoogleAccessId"};
    static constexpr std::array<std::string_view, 2> prefixes = {"X-Amz-", "X-Goog-"};

    auto is_secret_parameter = [](std::string_view name)
    {
        for (auto exact : exact_names)
            if (name == exact)
                return true;

        for (auto prefix : prefixes)
        {
            if (!name.starts_with(prefix))
                continue;
            /// `[A-Za-z0-9\-]*` - the rest of the name, possibly empty.
            bool rest_matches = true;
            for (char c : name.substr(prefix.length()))
                if (!(('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || c == '-'))
                    rest_matches = false;
            if (rest_matches)
                return true;
        }

        return false;
    };

    static constexpr std::string_view REPLACEMENT = "[HIDDEN]";

    /// Built in one pass rather than replacing in place: a replacement of a different length shifts
    /// the rest of the string, which is quadratic in the number of masked parameters. A setting value
    /// reaches this from the logging path and its length is chosen by whoever set the setting.
    std::string result;
    size_t copied = 0;

    for (size_t position = url.find_first_of("?&"); position != std::string::npos;
         position = url.find_first_of("?&", position + 1))
    {
        size_t name_begin = position + 1;

        /// Bounded by the next separator instead of scanning on to the next '=', which is quadratic
        /// on a string of separators. None of the names above contains one, so a name that runs into
        /// a separator is not one of them anyway.
        size_t name_end = url.find_first_of("=?&", name_begin);
        if (name_end == std::string::npos)
            break;
        if (url[name_end] != '=')
        {
            /// Rescan from the separator itself, so it is not skipped.
            position = name_end - 1;
            continue;
        }

        if (!is_secret_parameter(std::string_view(url).substr(name_begin, name_end - name_begin)))
            continue;

        /// `[^&#]*` - the value.
        size_t value_begin = name_end + 1;
        size_t value_end = url.find_first_of("&#", value_begin);
        if (value_end == std::string::npos)
            value_end = url.length();

        result.append(url, copied, value_begin - copied);
        result.append(REPLACEMENT);
        copied = value_end;

        /// Continue after the value, not inside it.
        position = value_end - 1;
    }

    if (copied == 0)
        return false;

    result.append(url, copied, std::string::npos);
    url = std::move(result);
    return true;
}

/** Mask both credential carriers of a URL: the userinfo (`user:password@`) and the values of the
  * presigned-URL query parameters. Returns whether anything was masked.
  *
  * This is the masking to use for a whole HTTP/S3 URL. Scanning for a presigned parameter is only
  * sound on a value that is one whole URL (see `maskPresignedURLParameters`), so the argument must be
  * one URL, not a URL embedded in a longer string.
  */
inline bool maskURICredentials(std::string & url)
{
    /// This release has no `maskURIUserinfo`; this is the regular expression it replaced. The userinfo
    /// is taken greedily up to the last at-sign before the path, so a password that itself contains an
    /// at-sign is masked whole.
    static const re2::RE2 userinfo(R"(^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]+@)");
    bool masked = RE2::Replace(&url, userinfo, "\\1[HIDDEN]@");
    masked |= maskPresignedURLParameters(url);
    return masked;
}

}
