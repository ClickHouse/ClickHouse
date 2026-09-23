#include <Common/HTTPHeaderFilter.h>
#include <Common/StringUtils.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <Poco/String.h>
#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void HTTPHeaderFilter::checkHeaders(HTTPHeaderEntries & entries) const
{
    std::lock_guard guard(mutex);

    for (const auto & entry : entries)
    {
        /// A header name must be an RFC 7230 token: non-empty and built only from tchar bytes
        /// (letters, digits and "!#$%&'*+-.^_`|~"). A value must not contain CR or LF.
        const auto is_tchar = [](char c)
        {
            return isAlphaNumericASCII(c)
                || c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\''
                || c == '*' || c == '+' || c == '-' || c == '.' || c == '^' || c == '_'
                || c == '`' || c == '|' || c == '~';
        };
        if (entry.name.empty() || !std::all_of(entry.name.begin(), entry.name.end(), is_tchar)
            || entry.value.contains('\r') || entry.value.contains('\n'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" has invalid character", entry.name);

        /// Header names are case-insensitive (RFC 7230 3.2); the forbidden set is stored lower-cased.
        const std::string lower_name = Poco::toLower(entry.name);

        if (forbidden_headers.contains(lower_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                    "see <http_forbid_headers>", entry.name);

        /// Match against the original-case name so an inline (?-i) scope stays case-sensitive.
        for (const auto & header_regex : forbidden_headers_regexp)
            if (re2::RE2::FullMatch(entry.name, *header_regex))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                        "see <http_forbid_headers>", entry.name);
    }
}

void HTTPHeaderFilter::checkHeaders(NormalizedHTTPHeaderEntries & entries) const
{
    /// The check only validates the entries; it does not modify them, so the container's
    /// lower-case invariant is preserved.
    checkHeaders(entries.entries);
}

void HTTPHeaderFilter::setValuesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard guard(mutex);

    forbidden_headers.clear();
    forbidden_headers_regexp.clear();

    if (config.has("http_forbid_headers"))
    {
        std::vector<std::string> keys;
        config.keys("http_forbid_headers", keys);

        for (const auto & key : keys)
        {
            if (startsWith(key, "header_regexp"))
            {
                const std::string pattern = config.getString("http_forbid_headers." + key);
                /// Case insensitivity must come from RE2 options, not from lower-casing the
                /// pattern string (that would corrupt metacharacters such as \D or [A-Z]).
                re2::RE2::Options options;
                options.set_case_sensitive(false);
                options.set_log_errors(false);
                auto regexp = std::make_shared<const re2::RE2>(pattern, options);
                if (!regexp->ok())
                {
                    /// Keep the existing behaviour of not aborting config load on a bad pattern,
                    /// but surface it: an uncompilable pattern silently forbids nothing.
                    LOG_WARNING(
                        getLogger("HTTPHeaderFilter"),
                        "Ignoring invalid <http_forbid_headers> regexp \"{}\": {}",
                        pattern, regexp->error());
                    continue;
                }
                forbidden_headers_regexp.push_back(std::move(regexp));
            }
            else if (startsWith(key, "header"))
            {
                /// Stored lower-cased so the case-insensitive lookup in checkHeaders works.
                forbidden_headers.insert(Poco::toLower(config.getString("http_forbid_headers." + key)));
            }
        }
    }
}

}
