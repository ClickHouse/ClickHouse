#include <Common/HTTPHeaderFilter.h>
#include <Common/StringUtils.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <Poco/String.h>
#include <algorithm>
#include <cctype>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void HTTPHeaderFilter::checkAndNormalizeHeaders(HTTPHeaderEntries & entries) const
{
    std::lock_guard guard(mutex);

    for (auto & entry : entries)
    {
        /// A bare CR or LF in a header name or value terminates the header line, so a header
        /// carrying one could smuggle a second header into the request (request/response splitting).
        if (entry.name.contains('\n') || entry.value.contains('\n')
            || entry.name.contains('\r') || entry.value.contains('\r'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" has invalid character", entry.name);
        /// Strip whitespace and control characters from header name for validation
        std::string & normalized_name = entry.name;
        normalized_name.erase(
            std::remove_if(
                normalized_name.begin(),
                normalized_name.end(),
                [](char c) { return std::iscntrl(static_cast<unsigned char>(c)) || std::isspace(static_cast<unsigned char>(c)); }),
            normalized_name.end());

        /// HTTP header names are case-insensitive (RFC 7230 3.2). Both the exact set and the
        /// regexps match the lower-cased name, so a rule cannot be bypassed by changing the case
        /// of a header, and a pattern cannot opt out of that with an inline (?-i) scope.
        const std::string lower_name = Poco::toLower(normalized_name);

        if (forbidden_headers.contains(lower_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                    "see <http_forbid_headers>", entry.name);

        for (const auto & header_regex : forbidden_headers_regexp)
            if (re2::RE2::FullMatch(lower_name, *header_regex))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                        "see <http_forbid_headers>", entry.name);
    }
}

void HTTPHeaderFilter::checkAndNormalizeHeaders(NormalizedHTTPHeaderEntries & entries) const
{
    checkAndNormalizeHeaders(entries.entries);

    /// The check only removes characters from a name, which cannot introduce an upper-case letter.
    /// Restore the invariant anyway, so that it does not rest on that argument.
    for (auto & entry : entries.entries)
        Poco::toLowerInPlace(entry.name);
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
                /// The name is matched in lower case, so a case-sensitive scope can only weaken the
                /// rule: an upper-case letter inside it never matches anything.
                if (pattern.contains("(?-i"))
                    LOG_WARNING(
                        getLogger("HTTPHeaderFilter"),
                        "<http_forbid_headers> regexp \"{}\" turns off case-insensitive matching. A header name is "
                        "matched in lower case, so an upper-case letter in that scope forbids nothing. "
                        "Write the pattern in lower case.",
                        pattern);
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
