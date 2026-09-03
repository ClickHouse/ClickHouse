#include <Common/HTTPHeaderFilter.h>
#include <Common/StringUtils.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// RFC 9110 (5.6.2) token characters. A valid HTTP field name is a token, so any
/// character outside this set (for example ':') is not allowed in a header name.
bool isHTTPTokenChar(char c)
{
    return isAlphaNumericASCII(c)
        || c == '!' || c == '#' || c == '$' || c == '%' || c == '&' || c == '\''
        || c == '*' || c == '+' || c == '-' || c == '.' || c == '^' || c == '_'
        || c == '`' || c == '|' || c == '~';
}

}

void HTTPHeaderFilter::checkAndNormalizeHeaders(HTTPHeaderEntries & entries) const
{
    std::lock_guard guard(mutex);

    for (auto & entry : entries)
    {
        /// A header name must be a non-empty RFC 9110 (5.6.2) token. The original bytes are
        /// validated, so a caller that sends its own copy of the entries cannot emit a name this
        /// filter did not accept. This rejects control characters (including CR and LF),
        /// whitespace and delimiters such as ':' — a name like "Cookie:x" would otherwise pass
        /// the filter yet let a peer parse a forbidden "Cookie" header from it, because a field
        /// name ends at the first ':'.
        if (entry.name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header name cannot be empty");
        for (char c : entry.name)
            if (!isHTTPTokenChar(c))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" has an invalid character in its name", entry.name);

        /// A bare CR or LF in a value terminates the header line, so a value carrying one could
        /// smuggle a second header into the request (request/response splitting).
        if (entry.value.contains('\n') || entry.value.contains('\r'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" has an invalid character in its value", entry.name);

        /// HTTP header names are case-insensitive (RFC 7230 3.2). The exact-set
        /// entries are stored lower-cased, so lower-case the name for that lookup.
        const std::string lower_name = Poco::toLower(entry.name);

        if (forbidden_headers.contains(lower_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                    "see <http_forbid_headers>", entry.name);

        /// Match the regexp against the original-case name: patterns are compiled
        /// case-insensitive by default, but an inline (?-i) scope must see the real
        /// case (lower-casing here would stop existing (?-i) configs from matching).
        for (const auto & header_regex : forbidden_headers_regexp)
            if (re2::RE2::FullMatch(entry.name, *header_regex))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                        "see <http_forbid_headers>", entry.name);
    }
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
                /// Stored lower-cased so the case-insensitive lookup in checkAndNormalizeHeaders works.
                forbidden_headers.insert(Poco::toLower(config.getString("http_forbid_headers." + key)));
            }
        }
    }
}

}
