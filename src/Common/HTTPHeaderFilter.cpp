#include <Common/HTTPHeaderFilter.h>
#include <Common/StringUtils.h>
#include <Common/Exception.h>
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

        /// HTTP header names are case-insensitive (RFC 7230 3.2), so both checks match in lower case.
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

    /// The check edits a name in place, so re-apply the invariant.
    for (auto & entry : entries.entries)
        Poco::toLowerInPlace(entry.name);
}

void HTTPHeaderFilter::setValuesFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::unordered_set<std::string> new_forbidden_headers;
    std::vector<std::shared_ptr<const re2::RE2>> new_forbidden_headers_regexp;

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
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "<http_forbid_headers> regexp \"{}\" does not compile: {}",
                        pattern, regexp->error());
                if (pattern.contains("(?-i"))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "<http_forbid_headers> regexp \"{}\" disables case-insensitive matching with an inline "
                        "(?-i) scope. A header name is matched in lower case, so the scope cannot be honoured. "
                        "Remove it and write the pattern in lower case.",
                        pattern);
                new_forbidden_headers_regexp.push_back(std::move(regexp));
            }
            else if (startsWith(key, "header"))
            {
                /// Stored lower-cased so the case-insensitive lookup in checkAndNormalizeHeaders works.
                new_forbidden_headers.insert(Poco::toLower(config.getString("http_forbid_headers." + key)));
            }
        }
    }

    /// Built first, then swapped: a rejected config leaves the running blocklist intact.
    std::lock_guard guard(mutex);
    forbidden_headers = std::move(new_forbidden_headers);
    forbidden_headers_regexp = std::move(new_forbidden_headers_regexp);
}

}
