#pragma once
#include <Parsers/IAST_fwd.h>
#include <IO/HTTPHeaderEntries.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/quoteString.h>
#include <Common/re2.h>
#include <unordered_set>
#include <string_view>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Helper function to get named collection for table engine.
/// Table engines have collection name as first argument of ast and other arguments are key-value overrides.
MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(
    ASTs asts, ContextPtr context, bool throw_unknown_collection = true, std::vector<std::pair<std::string, ASTPtr>> * complex_args = nullptr);

/// Helper function to get named collection for dictionary source.
/// Dictionaries have collection name as name argument of dict configuration and other arguments are overrides.
MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context);

/// Parses asts as key value pairs and returns a map of them.
/// If key or value cannot be parsed as literal or interpreted
/// as constant expression throws an exception.
std::map<String, Field> getParamsMapFromAST(ASTs asts, ContextPtr context);

HTTPHeaderEntries getHeadersFromNamedCollection(const NamedCollection & collection);

struct ExternalDatabaseEqualKeysSet
{
    static constexpr std::array<std::pair<std::string_view, std::string_view>, 5> equal_keys{
        std::pair{"username", "user"}, std::pair{"database", "db"}, std::pair{"hostname", "host"}, std::pair{"addresses_expr", "host"}, std::pair{"addresses_expr", "hostname"}};
};
struct MongoDBEqualKeysSet
{
    static constexpr std::array<std::pair<std::string_view, std::string_view>, 4> equal_keys{
        std::pair{"username", "user"}, std::pair{"database", "db"}, std::pair{"hostname", "host"}, std::pair{"table", "collection"}};
};
struct RedisEqualKeysSet
{
    static constexpr std::array<std::pair<std::string_view, std::string_view>, 4> equal_keys{std::pair{"hostname", "host"}};
};

template <typename EqualKeys> struct NamedCollectionValidateKey
{
    NamedCollectionValidateKey() = default;
    NamedCollectionValidateKey(const char * value_) : value(value_) {} /// NOLINT(google-explicit-constructor)
    NamedCollectionValidateKey(std::string_view value_) : value(value_) {} /// NOLINT(google-explicit-constructor)
    NamedCollectionValidateKey(const String & value_) : value(value_) {} /// NOLINT(google-explicit-constructor)

    std::string_view value;

    bool operator==(const auto & other) const
    {
        if (value == other.value)
            return true;

        for (const auto & equal : EqualKeys::equal_keys)
        {
            if (((equal.first == value) && (equal.second == other.value)) || ((equal.first == other.value) && (equal.second == value)))
            {
                return true;
            }
        }
        return false;
    }

    bool operator<(const auto & other) const
    {
        std::string_view canonical_self = value;
        std::string_view canonical_other = other.value;
        for (const auto & equal : EqualKeys::equal_keys)
        {
            if ((equal.first == value) || (equal.second == value))
                canonical_self = std::max(canonical_self, std::max(equal.first, equal.second));
            if ((equal.first == other.value) || (equal.second == other.value))
                canonical_other = std::max(canonical_other, std::max(equal.first, equal.second));
        }

        return canonical_self < canonical_other;
    }
};

template <typename T>
std::ostream & operator << (std::ostream & ostr, const NamedCollectionValidateKey<T> & key)
{
    ostr << key.value;
    return ostr;
}

template <class keys_cmp> using ValidateKeysMultiset = std::multiset<NamedCollectionValidateKey<keys_cmp>, std::less<NamedCollectionValidateKey<keys_cmp>>>;
using ValidateKeysSet = std::multiset<std::string_view>;

template <typename Keys = ValidateKeysSet>
void validateNamedCollection(
    const NamedCollection & collection,
    const Keys & required_keys,
    const Keys & optional_keys,
    const std::vector<std::shared_ptr<re2::RE2>> & optional_regex_keys = {})
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
        {
            continue;
        }

        if (required_keys.contains(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate key {} in named collection", key);

        auto match = std::find_if(
            optional_regex_keys.begin(), optional_regex_keys.end(),
            [&](const std::shared_ptr<re2::RE2> & regex) { return re2::RE2::PartialMatch(key, *regex); })
            != optional_regex_keys.end();

        if (!match)
        {
             throw Exception(
                 ErrorCodes::BAD_ARGUMENTS,
                 "Unexpected key `{}` in named collection. Required keys: {}, optional keys: {}",
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

template <typename T>
struct fmt::formatter<DB::NamedCollectionValidateKey<T>>
{
    constexpr static auto parse(format_parse_context & context)
    {
        return context.begin();
    }

    template <typename FormatContext>
    auto format(const DB::NamedCollectionValidateKey<T> & elem, FormatContext & context) const
    {
        return fmt::format_to(context.out(), "{}", elem.value);
    }
};
