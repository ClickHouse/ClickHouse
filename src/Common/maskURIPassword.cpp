#include <Common/HiddenSecret.h>
#include <Common/maskSensitiveQueryParameters.h>
#include <Common/maskURIPassword.h>
#include <Common/StringUtils.h>

/// Whether `authority` is `host [ ":" port ]` of RFC 3986, sections 3.2.2 and 3.2.3.
static constexpr bool isURIHostAndPort(std::string_view authority)
{
    auto is_reg_name_char = [](char c)
    {
        return isAlphaNumericASCII(c) || std::string_view("-._~!$&'()*+,;=").contains(c);
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
                if (host_end + 2 >= authority.length() || !isHexDigit(authority[host_end + 1]) || !isHexDigit(authority[host_end + 2]))
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
    return port[0] == ':' && std::all_of(port.begin() + 1, port.end(), isNumericASCII);
}

/// The range `[begin, end)` of the password of the first URI in `text` that carries one, or `{npos, npos}`.
/// RFC 3986: the authority ends at the first '/', '?' or '#', and the password is its userinfo after the first ':'.
/// Input that breaks the grammar is masked more, never less:
/// - a raw '@' in the password: the userinfo runs to the last '@' of the authority (`u:pw@1@host` hides `pw@1`);
/// - an authority cut short by a raw '/', '?' or '#' in the password: it runs to the last '@' (`u:pa/ss@host`).
/// `authority_terminators` are where the consumer of the URI ends its userinfo. By default only '/', as the MongoDB
/// driver does, since the consumer of a text is not always known; pass `URI_AUTHORITY_TERMINATORS` for `Poco::URI`.
static constexpr std::pair<size_t, size_t> findURIPasswordRange(std::string_view text, std::string_view authority_terminators = "/")
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

        size_t authority_end = std::min(text.find_first_of(DB::URI_AUTHORITY_TERMINATORS, authority_begin), text.length());
        std::string_view authority = text.substr(authority_begin, authority_end - authority_begin);
        size_t colon = authority.find(':');

        if (colon == std::string_view::npos || isURIHostAndPort(authority))
            continue;

        if (size_t at_sign = text.rfind('@'); at_sign != std::string_view::npos && at_sign > authority_end)
            return {authority_begin + colon + 1, at_sign};
    }

    return {std::string_view::npos, std::string_view::npos};
}

bool DB::maskAzureConnectionStringSecrets(std::string & str)
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

bool DB::maskURIPassword(std::string & uri, std::string_view authority_terminators)
{
    auto [password_begin, password_end] = findURIPasswordRange(uri, authority_terminators);
    if (password_begin == std::string::npos)
        return false;

    uri.replace(password_begin, password_end - password_begin, HIDDEN_SECRET);
    return true;
}

bool DB::maskMongoDBConnectionString(std::string & str)
{
    static constexpr std::array<std::string_view, 2> secret_options = {"tlscertificatekeyfilepassword", "sslclientcertificatekeypassword"};
    /// The properties the driver accepts besides `AWS_SESSION_TOKEN`; their names are case-insensitive too.
    static constexpr std::array<std::string_view, 6> public_properties
        = {"service_name", "canonicalize_host_name", "service_realm", "service_host", "environment", "token_resource"};

    auto equals = [](std::string_view name, std::string_view lowercase_name)
    {
        return std::ranges::equal(name, lowercase_name,
            [](char c, char expected) { return toLowerASCII(c) == expected; });
    };
    auto is_one_of = [&](std::string_view name, const auto & lowercase_names)
    {
        return std::any_of(
            lowercase_names.begin(), lowercase_names.end(), [&](std::string_view expected) { return equals(name, expected); });
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

    std::ranges::sort(hidden);

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

size_t DB::findURIAuthority(std::string_view uri)
{
    static constexpr std::string_view SEPARATOR = "://";

    if (uri.empty() || !isAlphaASCII(uri[0]))
        return std::string_view::npos;

    size_t scheme_end = 1;
    while (scheme_end < uri.length()
           && (isAlphaNumericASCII(uri[scheme_end]) || uri[scheme_end] == '+' || uri[scheme_end] == '.' || uri[scheme_end] == '-'))
        ++scheme_end;

    if (uri.compare(scheme_end, SEPARATOR.length(), SEPARATOR) != 0)
        return std::string_view::npos;

    return scheme_end + SEPARATOR.length();
}

bool DB::maskURIUserinfo(std::string & url)
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

bool DB::maskPresignedURLParameters(std::string & url)
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

    for (size_t position = url.find_first_of("?&"); position != std::string::npos; position = url.find_first_of("?&", position + 1))
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
