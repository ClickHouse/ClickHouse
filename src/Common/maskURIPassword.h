#pragma once

#include <array>
#include <string>
#include <string_view>


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

/** Replace the password in a URI of the form `scheme://user:password@host` with `[HIDDEN]`.
  * Returns whether anything was masked. Only the first such occurrence is masked.
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
inline bool maskURIPassword(std::string * uri)
{
    static constexpr std::string_view SEPARATOR = "://";

    for (size_t separator = uri->find(SEPARATOR); separator != std::string::npos;
         separator = uri->find(SEPARATOR, separator + SEPARATOR.length()))
    {
        /// `[^:]+` - at least one non-colon character in front of the separator.
        size_t preceding_colon = separator ? uri->find_last_of(':', separator - 1) : std::string::npos;
        size_t scheme_begin = (preceding_colon == std::string::npos) ? 0 : preceding_colon + 1;
        if (scheme_begin >= separator)
            continue;

        /// `[^:]*:` - the colon that opens the password.
        size_t password_begin = uri->find(':', separator + SEPARATOR.length());
        if (password_begin == std::string::npos)
            continue;
        ++password_begin;

        /// `[^@]*@` - the at sign that closes it.
        size_t password_end = uri->find('@', password_begin);
        if (password_end == std::string::npos)
            continue;

        uri->replace(password_begin, password_end - password_begin, "[HIDDEN]");
        return true;
    }

    return false;
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

inline bool matchesPresignedURLCredentialName(std::string_view name)
{
    static constexpr std::array<std::string_view, 5> exact_names = {"AWSAccessKeyId", "Signature", "Expires", "GoogleAccessId", "sig"};
    static constexpr std::array<std::string_view, 2> prefixes = {"X-Amz-", "X-Goog-"};

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
}

/// The storage endpoint percent-decodes a parameter name before authenticating, so `?%73ig=`
/// authenticates exactly like `?sig=`; a single pass, as the endpoint's is, so `?%2573ig=` is not a
/// credential there either. `+` is a space to `HTMLForm::readQuery`, not to the storage client.
inline bool isPresignedURLSecretParameterName(std::string_view name)
{
    if (matchesPresignedURLCredentialName(name))
        return true;

    if (!name.contains('%'))
        return false;

    auto hex_digit = [](char c)
    {
        if ('0' <= c && c <= '9')
            return c - '0';
        if ('a' <= c && c <= 'f')
            return c - 'a' + 10;
        if ('A' <= c && c <= 'F')
            return c - 'A' + 10;
        return -1;
    };

    std::string decoded;
    decoded.reserve(name.length());

    for (size_t i = 0; i < name.length(); ++i)
    {
        int high = -1;
        int low = -1;
        if (name[i] == '%' && i + 2 < name.length())
        {
            high = hex_digit(name[i + 1]);
            low = hex_digit(name[i + 2]);
        }

        /// A malformed escape is copied through: this feeds the decision below, never the output.
        if (high < 0 || low < 0)
        {
            decoded.push_back(name[i]);
            continue;
        }

        decoded.push_back(static_cast<char>(high * 16 + low));
        i += 2;
    }

    return matchesPresignedURLCredentialName(decoded);
}

/** Mask the values of the query parameters that carry credentials in a presigned URL, so that
  * `...?X-Amz-Signature=abc&foo=1` becomes `...?X-Amz-Signature=[HIDDEN]&foo=1`. Every occurrence
  * is masked. Returns whether anything was masked.
  *
  * This used to be the regular expression
  * `([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|sig|X-Amz-[A-Za-z0-9\-]*|X-Goog-[A-Za-z0-9\-]*)=)[^&#]*`
  * rewritten to `\1[HIDDEN]` globally. Matching is case-sensitive, as in the expression, and equal
  * to it for names with no percent escape; the scan also classifies the decoded name.
  * `src/Common/tests/gtest_mask_uri_password.cpp` checks both, against re2 and against a table.
  *
  * `sig` is the signature of an Azure shared access signature; the rest of a SAS (`sv`, `sp`, `se`,
  * `sr`, ...) states what that signature grants and stays visible. `BackupInfo::removeCredentialsFromS3URL`
  * keeps the old set: it REMOVES parameters from a locator that must stay openable on restore.
  */
inline bool maskPresignedURLParameters(std::string & url)
{
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

        if (!isPresignedURLSecretParameterName(std::string_view(url).substr(name_begin, name_end - name_begin)))
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
