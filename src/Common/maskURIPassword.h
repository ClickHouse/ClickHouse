#pragma once

#include <Common/re2.h>

#include <array>
#include <string>
#include <string_view>


namespace DB
{

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

}
