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

}
