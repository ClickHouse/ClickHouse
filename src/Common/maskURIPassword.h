#pragma once

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <utility>
#include <vector>


namespace DB
{

/** Mask the value of `<key>=<value>` in a connection string, where the value runs up to the next
  * ';' or to the end of the string. Only the first occurrence is masked, and nothing is masked if
  * the key is absent.
  *
  * This replaces the regular expressions `AccountKey=.*?(;|$)` and `SharedAccessSignature=.*?(;|$)`
  * that used to be applied here, so that masking a secret does not require a regex engine.
  */
inline bool maskConnectionStringKey(std::string & str, std::string_view key_with_eq)
{
    size_t key_position = str.find(key_with_eq);
    if (key_position == std::string::npos)
        return false;

    size_t value_begin = key_position + key_with_eq.length();
    size_t value_end = str.find(';', value_begin);
    if (value_end == std::string::npos)
        value_end = str.length();

    str.replace(value_begin, value_end - value_begin, "[HIDDEN]");
    return true;
}

/** The range `[begin, end)` of the password in a URI of the form `scheme://user:password@host`, or
  * `{npos, npos}` if there is none. Only the first such occurrence is found.
  *
  * This used to be the regular expression `([^:]+://[^:]*):([^@]*)@(.*)` rewritten to
  * `\1:[HIDDEN]@\3` - a whole regex engine carried for one substitution. The scan below reproduces
  * that expression exactly; `src/Common/tests/gtest_mask_uri_password.cpp` checks it against re2.
  *
  * Reading the expression: `[^:]+` is the scheme, so a match can only begin right after the
  * preceding colon (or at the start of the string); `[^:]*` then runs up to the colon that opens
  * the password; and `[^@]*` runs up to the '@' that closes it. If either is missing, the match
  * fails at this `://` and the next one is tried.
  */
inline std::pair<size_t, size_t> findURIPasswordRange(std::string_view uri)
{
    static constexpr std::string_view SEPARATOR = "://";

    for (size_t separator = uri.find(SEPARATOR); separator != std::string_view::npos;
         separator = uri.find(SEPARATOR, separator + SEPARATOR.length()))
    {
        /// `[^:]+` - at least one non-colon character in front of the separator.
        size_t preceding_colon = separator ? uri.find_last_of(':', separator - 1) : std::string_view::npos;
        size_t scheme_begin = (preceding_colon == std::string_view::npos) ? 0 : preceding_colon + 1;
        if (scheme_begin >= separator)
            continue;

        /// `[^:]*:` - the colon that opens the password.
        size_t password_begin = uri.find(':', separator + SEPARATOR.length());
        if (password_begin == std::string_view::npos)
            continue;
        ++password_begin;

        /// `[^@]*@` - the at sign that closes it.
        size_t password_end = uri.find('@', password_begin);
        if (password_end == std::string_view::npos)
            continue;

        return {password_begin, password_end};
    }

    return {std::string_view::npos, std::string_view::npos};
}

/** Replace the password in a URI of the form `scheme://user:password@host` with `[HIDDEN]`.
  * Returns whether anything was masked. Only the first such occurrence is masked.
  */
inline bool maskURIPassword(std::string * uri)
{
    auto [password_begin, password_end] = findURIPasswordRange(*uri);
    if (password_begin == std::string::npos)
        return false;

    uri->replace(password_begin, password_end - password_begin, "[HIDDEN]");
    return true;
}

/// Hides the secret option values and the URI password of a MongoDB connection string or option list; returns whether any was hidden.
/// As the driver reads them, an option name is case-insensitive and not percent-decoded, and a value runs to the next '&'.
inline bool maskMongoDBConnectionString(std::string & str)
{
    static constexpr std::array<std::string_view, 2> secret_options = {"tlscertificatekeyfilepassword", "sslclientcertificatekeypassword"};
    /// The properties the driver accepts besides `AWS_SESSION_TOKEN`; their names are case-insensitive too.
    static constexpr std::array<std::string_view, 6> public_properties
        = {"service_name", "canonicalize_host_name", "service_realm", "service_host", "environment", "token_resource"};

    auto equals = [](std::string_view name, std::string_view lowercase_name)
    {
        return std::equal(name.begin(), name.end(), lowercase_name.begin(), lowercase_name.end(),
            [](char c, char expected) { return (('A' <= c && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c) == expected; });
    };
    auto is_one_of = [&](std::string_view name, const auto & lowercase_names)
    {
        return std::any_of(lowercase_names.begin(), lowercase_names.end(), [&](std::string_view expected) { return equals(name, expected); });
    };

    std::vector<std::pair<size_t, size_t>> hidden;

    /// An option starts at the beginning of the string or right after a '?' or a '&'.
    for (size_t name_begin = 0; name_begin < str.length();)
    {
        size_t name_end = str.find_first_of("=?&", name_begin);
        if (name_end == std::string::npos)
            break;
        if (str[name_end] != '=')
        {
            name_begin = name_end + 1;
            continue;
        }

        size_t value_begin = name_end + 1;
        std::string_view name = std::string_view(str).substr(name_begin, name_end - name_begin);
        bool is_property_list = equals(name, "authmechanismproperties");
        bool is_secret = is_property_list || is_one_of(name, secret_options);
        /// Any other value also ends at a '?', so that one inside it, or inside the path, does not hide a later secret.
        size_t value_end = is_secret ? str.find('&', value_begin) : str.find_first_of("?&", value_begin);
        if (value_end == std::string::npos)
            value_end = str.length();

        /// A list of `name:value` separated by ',', which the driver splits after percent-decoding it.
        std::string_view value = std::string_view(str).substr(value_begin, value_end - value_begin);
        if (is_property_list && !value.contains('%'))
        {
            for (size_t entry_begin = 0; entry_begin < value.length();)
            {
                size_t entry_end = std::min(value.find(',', entry_begin), value.length());
                std::string_view entry = value.substr(entry_begin, entry_end - entry_begin);
                size_t colon = std::min(entry.find(':'), entry.length());
                if (!entry.empty() && !is_one_of(entry.substr(0, colon), public_properties))
                    hidden.emplace_back(value_begin + entry_begin + (colon < entry.length() ? colon + 1 : 0), value_begin + entry_end);
                entry_begin = entry_end + 1;
            }
        }
        else if (is_secret)
        {
            hidden.emplace_back(value_begin, value_end);
        }
        name_begin = value_end + 1;
    }

    if (auto [password_begin, password_end] = findURIPasswordRange(str); password_begin != std::string::npos)
        hidden.emplace_back(password_begin, password_end);

    if (hidden.empty())
        return false;

    std::sort(hidden.begin(), hidden.end());

    std::string result;
    size_t copied = 0;
    for (size_t i = 0; i < hidden.size();)
    {
        auto [range_begin, range_end] = hidden[i];
        for (++i; i < hidden.size() && hidden[i].first <= range_end; ++i)
            range_end = std::max(range_end, hidden[i].second);

        result.append(str, copied, range_begin - copied);
        result.append("[HIDDEN]");
        copied = range_end;
    }

    result.append(str, copied, std::string::npos);
    str = std::move(result);
    return true;
}

/** The offset just past the `://` of a value that starts with an RFC 3986 scheme, `npos` otherwise.
  */
inline size_t findURIAuthority(std::string_view uri)
{
    static constexpr std::string_view SEPARATOR = "://";

    /// `^[a-zA-Z][a-zA-Z0-9+.-]*` - the scheme. The character classes are spelled out rather than
    /// taken from `<cctype>`, which depends on the locale.
    auto is_letter = [](char c) { return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z'); };
    auto is_letter_or_digit = [&](char c) { return is_letter(c) || ('0' <= c && c <= '9'); };

    if (uri.empty() || !is_letter(uri[0]))
        return std::string_view::npos;

    size_t scheme_end = 1;
    while (scheme_end < uri.length()
           && (is_letter_or_digit(uri[scheme_end]) || uri[scheme_end] == '+' || uri[scheme_end] == '.' || uri[scheme_end] == '-'))
        ++scheme_end;

    if (uri.compare(scheme_end, SEPARATOR.length(), SEPARATOR) != 0)
        return std::string_view::npos;

    return scheme_end + SEPARATOR.length();
}

/** Mask the userinfo part of a URL: `scheme://anything@rest` becomes `scheme://[HIDDEN]@rest`.
  * Returns whether anything was masked.
  *
  * This used to be the regular expression `^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]+@` rewritten to
  * `\1[HIDDEN]@`. Only a match at the start of the string counts, and the userinfo is taken
  * greedily up to the last '@' before the path, so a password that itself contains an at-sign is
  * masked whole. `src/Common/tests/gtest_mask_uri_password.cpp` checks this against re2.
  */
inline bool maskURIUserinfo(std::string & url)
{
    size_t authority_begin = findURIAuthority(url);
    if (authority_begin == std::string::npos)
        return false;

    /// `[^/?#]+@` - the userinfo, greedy, so it ends at the last '@' before the path.
    size_t authority_end = url.find_first_of("/?#", authority_begin);
    if (authority_end == std::string::npos)
        authority_end = url.length();

    size_t at_sign = url.rfind('@', authority_end == 0 ? 0 : authority_end - 1);
    if (at_sign == std::string::npos || at_sign < authority_begin || at_sign >= authority_end || at_sign == authority_begin)
        return false;

    url.replace(authority_begin, at_sign - authority_begin, "[HIDDEN]");
    return true;
}

/// Query parameters can contain arbitrary credentials, not only presigned S3 parameters.
inline bool maskURIQuery(std::string & url)
{
    const auto query = url.find('?');
    if (query == std::string::npos)
        return false;
    url.replace(query + 1, std::string::npos, "[HIDDEN]");
    return true;
}

/** Mask the values of the query parameters that carry credentials in a presigned URL, so that
  * `...?X-Amz-Signature=abc&foo=1` becomes `...?X-Amz-Signature=[HIDDEN]&foo=1`. Every occurrence
  * is masked. Returns whether anything was masked.
  *
  * This used to be the regular expression
  * `([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|X-Amz-[A-Za-z0-9\-]*|X-Goog-[A-Za-z0-9\-]*)=)[^&#]*`
  * rewritten to `\1[HIDDEN]` globally. The parameter set mirrors
  * `BackupInfo::removeCredentialsFromS3URL`. Matching is case-sensitive, as in the expression, and
  * `src/Common/tests/gtest_mask_uri_password.cpp` checks this against re2.
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
