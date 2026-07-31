#pragma once

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

/** Mask the userinfo part of a URI: `scheme://user:password@host` becomes `scheme://[HIDDEN]@host`.
  * Returns whether anything was masked.
  *
  * This replaces the regular expression `^([a-zA-Z][a-zA-Z0-9+.-]*://)[^/?#]+@` rewritten to
  * `\1[HIDDEN]@`, so that masking a secret does not require a regex engine;
  * `src/Common/tests/gtest_mask_uri_password.cpp` checks it against re2.
  *
  * Reading the expression: the match is anchored at the start of the string, `[a-zA-Z][a-zA-Z0-9+.-]*`
  * is the scheme, and the greedy `[^/?#]+` runs to the last at sign before the path, query, or
  * fragment, so a userinfo whose password itself contains an at sign is masked whole, not just up
  * to the first one.
  */
inline bool maskURIUserInfo(std::string & url)
{
    const auto is_alpha = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); };
    const auto is_scheme_char = [&](char c)
    {
        return is_alpha(c) || (c >= '0' && c <= '9') || c == '+' || c == '.' || c == '-';
    };

    if (url.empty() || !is_alpha(url[0]))
        return false;

    size_t separator = 1;
    while (separator < url.size() && is_scheme_char(url[separator]))
        ++separator;

    if (url.compare(separator, 3, "://") != 0)
        return false;

    size_t userinfo_begin = separator + 3;
    size_t authority_end = url.find_first_of("/?#", userinfo_begin);
    if (authority_end == std::string::npos)
        authority_end = url.size();
    if (authority_end == userinfo_begin)
        return false;

    /// `[^/?#]+` is greedy, so the at sign that closes the match is the last one before the path.
    size_t at_sign = url.rfind('@', authority_end - 1);
    if (at_sign == std::string::npos || at_sign <= userinfo_begin)
        return false;

    url.replace(userinfo_begin, at_sign - userinfo_begin, "[HIDDEN]");
    return true;
}

/** Mask the values of presigned-URL credential query parameters:
  * `?X-Amz-Signature=abc&Expires=1` becomes `?X-Amz-Signature=[HIDDEN]&Expires=[HIDDEN]`.
  * Returns whether anything was masked.
  *
  * This replaces the regular expression
  * `([?&](?:AWSAccessKeyId|Signature|Expires|GoogleAccessId|X-Amz-[A-Za-z0-9\-]*|X-Goog-[A-Za-z0-9\-]*)=)[^&#]*`
  * applied globally and rewritten to `\1[HIDDEN]`, so that masking a secret does not require a
  * regex engine; `src/Common/tests/gtest_mask_uri_password.cpp` checks it against re2.
  */
inline bool maskURIPresignedCredentials(std::string & url)
{
    static constexpr std::string_view HIDDEN = "[HIDDEN]";
    static constexpr std::string_view EXACT_KEYS[] = {"AWSAccessKeyId", "Signature", "Expires", "GoogleAccessId"};
    static constexpr std::string_view PREFIX_KEYS[] = {"X-Amz-", "X-Goog-"};

    const auto is_key_char = [](char c)
    {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-';
    };

    bool changed = false;
    size_t pos = 0;
    while ((pos = url.find_first_of("?&", pos)) != std::string::npos)
    {
        size_t key_begin = pos + 1;
        ++pos;

        size_t value_begin = std::string::npos;
        for (const auto & key : EXACT_KEYS)
        {
            if (url.compare(key_begin, key.size(), key) == 0
                && key_begin + key.size() < url.size() && url[key_begin + key.size()] == '=')
            {
                value_begin = key_begin + key.size() + 1;
                break;
            }
        }
        if (value_begin == std::string::npos)
        {
            for (const auto & prefix : PREFIX_KEYS)
            {
                if (url.compare(key_begin, prefix.size(), prefix) != 0)
                    continue;
                size_t key_end = key_begin + prefix.size();
                while (key_end < url.size() && is_key_char(url[key_end]))
                    ++key_end;
                if (key_end < url.size() && url[key_end] == '=')
                {
                    value_begin = key_end + 1;
                    break;
                }
            }
        }

        if (value_begin == std::string::npos)
            continue;

        size_t value_end = url.find_first_of("&#", value_begin);
        if (value_end == std::string::npos)
            value_end = url.size();

        url.replace(value_begin, value_end - value_begin, HIDDEN);
        changed = true;
        pos = value_begin + HIDDEN.size();
    }

    return changed;
}

}
