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

HTTPHeaderEntries HTTPHeaderFilter::checkAndNormalizeHeaders(HTTPHeaderEntries entries) const
{
    std::lock_guard guard(mutex);

    for (auto & entry : entries)
    {
        /// Keep the user-written name for error messages: normalization mutates entry.name below.
        const std::string reported_name = entry.name;

        /// Reject the characters that let a name inject or forge a header on the wire, checked on
        /// the original bytes so a caller that validates and then sends its own copy of the entries
        /// cannot emit them: CR and LF terminate the header line (request/response splitting), and
        /// ':' ends the field name, so a name like "Cookie:x" would otherwise pass this filter yet
        /// let a peer parse a forbidden "Cookie" header from it.
        if (entry.name.contains('\r') || entry.name.contains('\n') || entry.name.contains(':'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" has an invalid character in its name", reported_name);

        /// A bare CR or LF in a value terminates the header line, so a value carrying one could
        /// smuggle a second header into the request (request/response splitting).
        if (entry.value.contains('\n') || entry.value.contains('\r'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" has an invalid character in its value", reported_name);

        /// Strip the remaining whitespace and control characters from the name before the
        /// <http_forbid_headers> lookup: it canonicalises names like "Coo\tkie" so they cannot
        /// evade the forbidden set, and preserves the historical behaviour for stored objects,
        /// whose header names are re-validated on every ATTACH.
        std::string & normalized_name = entry.name;
        normalized_name.erase(
            std::remove_if(
                normalized_name.begin(),
                normalized_name.end(),
                [](char c) { return std::iscntrl(static_cast<unsigned char>(c)) || std::isspace(static_cast<unsigned char>(c)); }),
            normalized_name.end());

        /// A name that is only whitespace/control normalizes to "" here and is not rejected: an
        /// empty name cannot forge a forbidden header or split the request, and rejecting it would
        /// fail ATTACH of a table stored with such a name.
        /// HTTP header names are case-insensitive (RFC 7230 3.2). The exact-set
        /// entries are stored lower-cased, so lower-case the name for that lookup.
        const std::string lower_name = Poco::toLower(normalized_name);

        if (forbidden_headers.contains(lower_name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                    "see <http_forbid_headers>", reported_name);

        /// Match the regexp against the original-case name: patterns are compiled
        /// case-insensitive by default, but an inline (?-i) scope must see the real
        /// case (lower-casing here would stop existing (?-i) configs from matching).
        for (const auto & header_regex : forbidden_headers_regexp)
            if (re2::RE2::FullMatch(normalized_name, *header_regex))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HTTP header \"{}\" is forbidden in configuration file, "
                                                        "see <http_forbid_headers>", reported_name);
    }

    return entries;
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
