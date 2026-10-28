#pragma once

#include <Common/HiddenSecret.h>
#include <Common/maskSensitiveQueryParameters.h>

#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#include <utility>
#include <vector>


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
            result.append(HIDDEN_SECRET);
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
        str = HIDDEN_SECRET;
        return true;
    }
    return masked;
}

/// Whether `authority` is `host [ ":" port ]` of RFC 3986, sections 3.2.2 and 3.2.3.
inline bool isURIHostAndPort(std::string_view authority)
{
    auto is_hex_digit = [](char c) { return ('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F'); };
    auto is_reg_name_char = [](char c)
    {
        return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || std::string_view("-._~!$&'()*+,;=").contains(c);
    };

    size_t host_end = 0;
    if (authority.starts_with('['))
    {
        host_end = authority.find(']');
        if (host_end == std::string_view::npos)
            return false;
        ++host_end;
    }
    else
    {
        for (; host_end < authority.length() && authority[host_end] != ':'; ++host_end)
        {
            if (authority[host_end] == '%')
            {
                if (host_end + 2 >= authority.length() || !is_hex_digit(authority[host_end + 1]) || !is_hex_digit(authority[host_end + 2]))
                    return false;
                host_end += 2;
            }
            else if (!is_reg_name_char(authority[host_end]))
            {
                return false;
            }
        }
    }

    std::string_view port = authority.substr(host_end);
    if (port.empty())
        return true;
    return port[0] == ':' && std::all_of(port.begin() + 1, port.end(), [](char c) { return '0' <= c && c <= '9'; });
}

/// Where RFC 3986, section 3.2, ends the authority of a URI. `Poco::URI` ends it there too.
inline constexpr std::string_view URI_AUTHORITY_TERMINATORS = "/?#";

/// The range `[begin, end)` of the password of the first URI in `text` that carries one, or `{npos, npos}`.
/// RFC 3986: the authority ends at the first '/', '?' or '#', and the password is its userinfo after the first ':'.
/// Input that breaks the grammar is masked more, never less:
/// - a raw '@' in the password: the userinfo runs to the last '@' of the authority (`u:pw@1@host` hides `pw@1`);
/// - an authority cut short by a raw '/', '?' or '#' in the password: it runs to the last '@' (`u:pa/ss@host`).
/// `authority_terminators` are where the consumer of the URI ends its userinfo. By default only '/', as the MongoDB
/// driver does, since the consumer of a text is not always known; pass `URI_AUTHORITY_TERMINATORS` for `Poco::URI`.
inline std::pair<size_t, size_t> findURIPasswordRange(std::string_view text, std::string_view authority_terminators = "/")
{
    static constexpr std::string_view SEPARATOR = "://";

    for (size_t separator = text.find(SEPARATOR); separator != std::string_view::npos;
         separator = text.find(SEPARATOR, separator + SEPARATOR.length()))
    {
        size_t authority_begin = separator + SEPARATOR.length();
        size_t userinfo_bound = std::min(text.find_first_of(authority_terminators, authority_begin), text.length());
        std::string_view bounded = text.substr(authority_begin, userinfo_bound - authority_begin);

        if (size_t at_sign = bounded.rfind('@'); at_sign != std::string_view::npos)
        {
            if (size_t colon = bounded.find(':'); colon < at_sign)
                return {authority_begin + colon + 1, authority_begin + at_sign};
            continue;
        }

        size_t authority_end = std::min(text.find_first_of(URI_AUTHORITY_TERMINATORS, authority_begin), text.length());
        std::string_view authority = text.substr(authority_begin, authority_end - authority_begin);
        size_t colon = authority.find(':');

        if (colon == std::string_view::npos || isURIHostAndPort(authority))
            continue;

        if (size_t at_sign = text.rfind('@'); at_sign != std::string_view::npos && at_sign > authority_end)
            return {authority_begin + colon + 1, at_sign};
    }

    return {std::string_view::npos, std::string_view::npos};
}

/// Replace the password `findURIPasswordRange` finds with `[HIDDEN]`. Returns whether anything was masked.
inline bool maskURIPassword(std::string * uri, std::string_view authority_terminators = "/")
{
    auto [password_begin, password_end] = findURIPasswordRange(*uri, authority_terminators);
    if (password_begin == std::string::npos)
        return false;

    uri->replace(password_begin, password_end - password_begin, HIDDEN_SECRET);
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
        result.append(HIDDEN_SECRET);
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

    url.replace(authority_begin, at_sign - authority_begin, HIDDEN_SECRET);
    return true;
}

/// Mask the values of the query parameters that carry credentials in a presigned URL, so that
/// `...?X-Amz-Signature=abc&foo=1` becomes `...?X-Amz-Signature=[HIDDEN]&foo=1`. Every occurrence
/// is masked. Returns whether anything was masked.
///
/// This used to be the regular expression
/// `([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|sig|(?i:X-Amz-|X-Goog-)[^=?&]*)=)[^&#]*`
/// rewritten to `\1[HIDDEN]` globally; `src/Common/tests/gtest_mask_uri_password.cpp` checks this against re2.
/// A name is also matched percent-decoded, as the storage endpoint reads it: `?%73ig=` is `?sig=`.
/// `sig` is the signature of an Azure SAS.
inline bool maskPresignedURLParameters(std::string & url)
{
    auto is_secret_parameter = [](std::string_view raw_name)
    {
        const std::string name = decodeQueryComponent(raw_name);
        return name == "sig" || isPresignedURLParameterName(name);
    };

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
        result.append(HIDDEN_SECRET);
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
    bool masked = maskURIUserinfo(url);
    masked |= maskPresignedURLParameters(url);
    return masked;
}

}
