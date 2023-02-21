#pragma once
#include <Parsers/IAST_fwd.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/quoteString.h>
#include <unordered_set>
#include <string_view>
#include <fmt/format.h>
#include <regex>

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB
{

/// Helper function to get named collection for table engine.
/// Table engines have collection name as first argument of ast and other arguments are key-value overrides.
MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(ASTs asts, bool throw_unknown_collection = true);
/// Helper function to get named collection for dictionary source.
/// Dictionaries have collection name as name argument of dict configuration and other arguments are overrides.
MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

HTTPHeaderEntries getHeadersFromNamedCollection(const NamedCollection & collection);

template <typename RequiredKeys = std::unordered_set<std::string>,
          typename OptionalKeys = std::unordered_set<std::string>>
void validateNamedCollection(
    const NamedCollection & collection,
    const RequiredKeys & required_keys,
    const OptionalKeys & optional_keys,
    const std::vector<std::regex> & optional_regex_keys = {})
{
    NamedCollection::Keys keys = collection.getKeys();
    auto required_keys_copy = required_keys;

    for (const auto & key : keys)
    {
        if (required_keys_copy.contains(key))
        {
            required_keys_copy.erase(key);
            continue;
        }

        if (optional_keys.contains(key))
            continue;

        auto match = std::find_if(
            optional_regex_keys.begin(), optional_regex_keys.end(),
            [&](const std::regex & regex) { return std::regex_search(key, regex); })
            != optional_regex_keys.end();

        if (!match)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unexpected key {} in named collection. Required keys: {}, optional keys: {}",
                backQuoteIfNeed(key), fmt::join(required_keys, ", "), fmt::join(optional_keys, ", "));
        }
    }

    if (!required_keys_copy.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Required keys ({}) are not specified. All required keys: {}, optional keys: {}",
            fmt::join(required_keys_copy, ", "), fmt::join(required_keys, ", "), fmt::join(optional_keys, ", "));
    }
}

}
