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
    static constexpr std::string_view SEPARATOR = "://";

    /// `^[a-zA-Z][a-zA-Z0-9+.-]*` - the scheme. The character classes are spelled out rather than
    /// taken from `<cctype>`, which depends on the locale.
    auto is_letter = [](char c) { return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z'); };
    auto is_letter_or_digit = [&](char c) { return is_letter(c) || ('0' <= c && c <= '9'); };

    if (url.empty() || !is_letter(url[0]))
        return false;

    size_t scheme_end = 1;
    while (scheme_end < url.length()
           && (is_letter_or_digit(url[scheme_end]) || url[scheme_end] == '+' || url[scheme_end] == '.' || url[scheme_end] == '-'))
        ++scheme_end;

    if (url.compare(scheme_end, SEPARATOR.length(), SEPARATOR) != 0)
        return false;

    /// `[^/?#]+@` - the userinfo, greedy, so it ends at the last '@' before the path.
    size_t authority_begin = scheme_end + SEPARATOR.length();
    size_t authority_end = url.find_first_of("/?#", authority_begin);
    if (authority_end == std::string::npos)
        authority_end = url.length();

    size_t at_sign = url.rfind('@', authority_end == 0 ? 0 : authority_end - 1);
    if (at_sign == std::string::npos || at_sign < authority_begin || at_sign >= authority_end || at_sign == authority_begin)
        return false;

    url.replace(authority_begin, at_sign - authority_begin, "[HIDDEN]");
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

    bool masked = false;
    for (size_t position = url.find_first_of("?&"); position != std::string::npos;
         position = url.find_first_of("?&", position + 1))
    {
        size_t name_begin = position + 1;
        size_t equal_sign = url.find('=', name_begin);
        if (equal_sign == std::string::npos)
            break;

        if (!is_secret_parameter(std::string_view(url).substr(name_begin, equal_sign - name_begin)))
            continue;

        /// `[^&#]*` - the value.
        size_t value_begin = equal_sign + 1;
        size_t value_end = url.find_first_of("&#", value_begin);
        if (value_end == std::string::npos)
            value_end = url.length();

        static constexpr std::string_view REPLACEMENT = "[HIDDEN]";
        url.replace(value_begin, value_end - value_begin, REPLACEMENT);
        masked = true;
        /// Continue after the replacement, not inside it.
        position = value_begin + REPLACEMENT.length() - 1;
    }

    return masked;
}

}
