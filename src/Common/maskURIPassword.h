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


static constexpr bool is_letter(const char c) { return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z'); }
static constexpr bool is_digit(const char c) { return '0' <= c && c <= '9'; }
static constexpr bool is_letter_or_digit(const char c) { return is_letter(c) || is_digit(c); }

static constexpr std::string_view url_legal_name_chars = "-._~!$&'()*+,;=";

/// Whether `authority` is `host [ ":" port ]` of RFC 3986, sections 3.2.2 and 3.2.3.
static constexpr bool isURIHostAndPort(const std::string& authority, const size_t offset, const size_t limit)
{
    auto is_hex_digit = [](const char c) { return is_digit(c) || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F'); };
    auto is_reg_name_char = [](const char c)
    {
        return is_letter_or_digit(c) || url_legal_name_chars.contains(c);
    };

    size_t host_end = offset;
    if (authority[host_end] =='[')
    {
        host_end = authority.find(']', host_end + 1);
        if (host_end == std::string_view::npos)
            return false;
        ++host_end;
    }
    else
    {
        for (; host_end < limit && authority[host_end] != ':'; ++host_end)
        {
            if (authority[host_end] == '%')
            {
                if (host_end + 2 >= limit || !is_hex_digit(authority[host_end + 1]) || !is_hex_digit(authority[host_end + 2]))
                    return false;
                host_end += 2;
            }
            else if (!is_reg_name_char(authority[host_end]))
            {
                return false;
            }
        }
    }

    return host_end == limit
        || (authority[host_end] == ':'
            && std::all_of(
                std::ranges::next(authority.cbegin(), host_end + 1), std::ranges::next(authority.cbegin(), limit), is_digit));
}

/// Where RFC 3986, section 3.2, ends the authority of a URI. `Poco::URI` ends it there too.
inline constexpr std::string_view URI_AUTHORITY_TERMINATORS = "/?#";

static constexpr std::string_view SCHEMA_SEPARATOR = "://";

/// The range `[begin, end)` of the password of the first URI in `text` that carries one, or `{npos, npos}`.
/// RFC 3986: the authority ends at the first '/', '?' or '#', and the password is its userinfo after the first ':'.
/// Input that breaks the grammar is masked more, never less:
/// - a raw '@' in the password: the userinfo runs to the last '@' of the authority (`u:pw@1@host` hides `pw@1`);
/// - an authority cut short by a raw '/', '?' or '#' in the password: it runs to the last '@' (`u:pa/ss@host`).
/// `authority_terminators` are where the consumer of the URI ends its userinfo.
static constexpr std::pair<size_t, size_t> findURIPasswordRange(const std::string & text)
{
    for (size_t separator = text.find(SCHEMA_SEPARATOR); separator != std::string_view::npos;
         separator = text.find(SCHEMA_SEPARATOR, separator + SCHEMA_SEPARATOR.length()))
    {
        const size_t authority_begin = separator + SCHEMA_SEPARATOR.length();
        const size_t userinfo_bound = std::min(text.find('/', authority_begin), text.length() - 1);

        if (const size_t at_sign = text.rfind('@', userinfo_bound); at_sign != std::string_view::npos)
        {
            if (const size_t colon = text.find(':', authority_begin); colon < at_sign)
                return {authority_begin + colon + 1, authority_begin + at_sign};
            continue;
        }

        const size_t authority_end = std::min(text.find_first_of(URI_AUTHORITY_TERMINATORS, authority_begin), text.length());
        auto authority = std::string_view(text).substr(authority_begin, authority_end - authority_begin);
        const size_t colon = authority.find(':');

        if (colon == std::string_view::npos || isURIHostAndPort(text, authority_begin, authority_end))
            continue;

        if (size_t at_sign = text.rfind('@'); at_sign != std::string_view::npos && at_sign > authority_end)
            return {authority_begin + colon + 1, at_sign};
    }

    return {std::string_view::npos, std::string_view::npos};
}
/// `^[a-zA-Z][a-zA-Z0-9+.-]*` - the scheme. The character classes are spelled out rather than
/// taken from `<cctype>`, which depends on the locale.
static constexpr size_t skip_valid_schema_chars(const std::string & uri, size_t offset, const size_t limit)
{
    if (offset == limit)
        return offset;
    if (!is_letter(uri[offset]))
        return offset;
    ++offset;
    while (offset < limit - 3 && (is_letter_or_digit(uri[offset]) || uri[offset] == '+' || uri[offset] == '.' || uri[offset] == '-'))
        ++offset;
    return offset;
};

/// Replace the passwords found in userinfo components in the given string with `[HIDDEN]`. Returns whether anything was masked.
/// In contrast to rfc-3986, userinfo components without a colon are interpreted as a password and masked whole to account for URIs
/// like http://token@host.
inline bool maskURIPassword(std::string & uri, std::string_view authority_terminators = "/")
{
    const auto original_size = uri.size();
    size_t uncopied_offset = 0;
    std::string result;
    auto parse_userinfo_and_authority = [&](size_t offset) -> size_t
    {
        const size_t initial_offset = offset;
        size_t last_at = std::string_view::npos;
        size_t first_colon = std::string_view::npos;
        while (offset < original_size)
        {
            if (const char c = uri[offset]; c == '@')
                last_at = offset;
            else if (c == ':' && first_colon == std::string_view::npos)
                first_colon = offset;
            else if (authority_terminators.contains(c))
            {
                if (first_colon == std::string_view::npos
                    || (last_at != std::string::npos && first_colon < last_at)
                    || isURIHostAndPort(uri, initial_offset, offset))
                    break;
            }
            else if (c == '|' && first_colon == std::string_view::npos)
                break;
            offset++;
        }
        if (last_at != std::string_view::npos)
        {
            result.append(uri, uncopied_offset, (first_colon == std::string::npos ? initial_offset - 1 : first_colon) - uncopied_offset + 1)
                .append(HIDDEN_SECRET);
            uncopied_offset = last_at;
        }
        return offset;
    };
    auto skip_past_next_schema = [&](size_t offset) -> size_t
    {
        while (offset < original_size - 3)
        {
            const size_t initial_offset = offset;
            offset = skip_valid_schema_chars(uri, offset, original_size);
            if (offset > initial_offset && offset + 3 < original_size
                && uri.compare(offset, SCHEMA_SEPARATOR.length(), SCHEMA_SEPARATOR) == 0)
                return parse_userinfo_and_authority(offset + SCHEMA_SEPARATOR.length());
            ++offset;
        }
        return original_size;
    };
    auto parse_schema_from_root = [&](size_t offset) -> size_t
    {
        // a // at the start of a uri is resolved relative to the schema of the base, interpret offset + 2 as start of authority
        if (offset < original_size - 2 && uri.compare(offset, 2, "//") == 0)
            return parse_userinfo_and_authority(offset + 2);
        const size_t initial_offset = offset;
        if (offset < original_size - 3)
        {
            offset = skip_valid_schema_chars(uri, offset, original_size);
            if (offset + 3 >= original_size)
            {
                return original_size;
            }
            const char c = uri[offset];
            if (c == ':' && uri.compare(offset + 1, 2, "//") == 0)
                return parse_userinfo_and_authority(offset + 3);
            if (c == ':' || c == '@')
                return parse_userinfo_and_authority(initial_offset);
            if (c == '|')
                return offset + 1;
            if (!url_legal_name_chars.contains(c))
                return skip_past_next_schema(offset + 1);
        }
        return parse_userinfo_and_authority(initial_offset);
    };
    auto parse_query = [&](size_t offset) -> size_t
    {
        while (offset < original_size)
        {
            const auto next_param_val = uri.find_first_of("=|", offset);
            if (next_param_val == std::string_view::npos)
                return original_size;
            if (uri[next_param_val] == '|')
                return next_param_val + 1;
            offset = next_param_val + 1;
            const size_t initial_offset = offset;
            offset = skip_valid_schema_chars(uri, offset, original_size);
            if (offset > initial_offset && uri.compare(offset, 3, SCHEMA_SEPARATOR) == 0)
                offset = parse_userinfo_and_authority(offset + 3);
            offset++;
        }
        return original_size;
    };
    auto parse_path = [&](const size_t offset) -> size_t
    {
        const auto query_or_next_url = uri.find_first_of("?|", offset);
        if (query_or_next_url == std::string_view::npos)
            return skip_past_next_schema(offset);
        if (uri[query_or_next_url] == '?')
            return parse_query(query_or_next_url + 1);
        return query_or_next_url + 1;
    };
    auto parse_after_authority = [&](const size_t offset) -> size_t
    {
        switch (const char c = uri[offset]; c)
        {
            case '/': return parse_path(offset + 1);
            case '?': return parse_query(offset + 1);
            case '|': return offset + 1;
            default: return url_legal_name_chars.contains(c) ? original_size : skip_past_next_schema(offset + 1);
        }
    };
    for (size_t i = 0; i < original_size;)
    {
        i = parse_schema_from_root(i);
        if (i < original_size)
            i = parse_after_authority(i);
    }
    if (uncopied_offset == 0)
        return false;
    result.append(uri, uncopied_offset);
    uri = result;
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
inline size_t findURIAuthority(const std::string & uri)
{
    const size_t scheme_end = skip_valid_schema_chars(uri, 0, uri.length());
    if (scheme_end == 0)
        return std::string_view::npos;
    if (uri.compare(scheme_end, SCHEMA_SEPARATOR.length(), SCHEMA_SEPARATOR) != 0)
        return std::string_view::npos;

    return scheme_end + SCHEMA_SEPARATOR.length();
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
    const size_t authority_begin = findURIAuthority(url);
    if (authority_begin == std::string::npos)
        return false;

    /// `[^/?#]+@` - the userinfo, greedy, so it ends at the last '@' before the path.
    size_t authority_end = url.find_first_of(URI_AUTHORITY_TERMINATORS, authority_begin);
    if (authority_end == std::string::npos)
        authority_end = url.length();

    size_t at_sign = url.rfind('@', authority_end == 0 ? 0 : authority_end - 1);
    if (at_sign == std::string::npos || at_sign < authority_begin || at_sign >= authority_end || at_sign == authority_begin)
        return false;

    url.replace(authority_begin, at_sign - authority_begin, HIDDEN_SECRET);
    return true;
}

/// Mask the values of the query parameters that carry credentials in a presigned URL, so that
/// `...?X-Amz-Signature=abc&foo=1` becomes `...?X-Amz-Signature=[HIDDEN]&foo=1`, and of the
/// `connection_string` parameter of an ODBC/JDBC bridge request. Every occurrence is masked.
/// Returns whether anything was masked.
///
/// Apart from `connection_string`, this used to be the regular expression
/// `([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|sig|(?i:X-Amz-|X-Goog-)[^=?&]*)=)[^&#]*`
/// rewritten to `\1[HIDDEN]` globally; `src/Common/tests/gtest_mask_uri_password.cpp` checks this against re2.
/// A name is also matched percent-decoded, as the storage endpoint reads it: `?%73ig=` is `?sig=`.
/// `sig` is the signature of an Azure SAS.
inline bool maskPresignedURLParameters(std::string & url)
{
    auto is_secret_parameter = [](std::string_view raw_name)
    {
        const std::string name = decodeQueryComponent(raw_name);
        return name == "sig" || name == "connection_string" || isPresignedURLParameterName(name);
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
