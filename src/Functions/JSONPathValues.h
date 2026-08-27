#pragma once

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/re2.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <base/types.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <optional>

namespace DB::ErrorCodes
{
extern const int CANNOT_COMPILE_REGEXP;
}

namespace DB::JSONPathValues
{

class PathMatcher
{
public:
    PathMatcher(
        VectorWithMemoryTracking<String> include_paths_,
        VectorWithMemoryTracking<String> include_path_regexps_,
        VectorWithMemoryTracking<String> skip_paths_,
        VectorWithMemoryTracking<String> skip_path_regexps_)
        : include_paths(normalizePaths(std::move(include_paths_)))
        , include_path_regexps(normalizeStrings(std::move(include_path_regexps_)))
        , skip_paths(normalizePaths(std::move(skip_paths_)))
        , skip_path_regexps(normalizeStrings(std::move(skip_path_regexps_)))
    {
        compileRegexps(include_path_regexps, include_regexps);
        compileRegexps(skip_path_regexps, skip_regexps);
    }

    bool shouldIndex(std::string_view path) const
    {
        if (matchesAnyPathOrSubtree(path, skip_paths) || matchesAnyRegexp(path, skip_regexps))
            return false;

        return !hasIncludeFilter()
            || matchesAnyPathOrSubtree(path, include_paths)
            || matchesAnyRegexp(path, include_regexps);
    }

    bool shouldVisit(std::string_view path) const
    {
        if (matchesAnyPathOrSubtree(path, skip_paths))
            return false;

        /// An arbitrary include or skip regexp can match a descendant differently from its ancestor.
        return !hasIncludeFilter()
            || matchesAnyPathOrSubtree(path, include_paths)
            || matchesAnyAncestor(path, include_paths)
            || !include_regexps.empty();
    }

    const VectorWithMemoryTracking<String> & getIncludePaths() const { return include_paths; }
    const VectorWithMemoryTracking<String> & getIncludePathRegexps() const { return include_path_regexps; }
    const VectorWithMemoryTracking<String> & getSkipPaths() const { return skip_paths; }
    const VectorWithMemoryTracking<String> & getSkipPathRegexps() const { return skip_path_regexps; }

private:
    using Regexps = VectorWithMemoryTracking<std::unique_ptr<re2::RE2>>;

    bool hasIncludeFilter() const { return !include_paths.empty() || !include_regexps.empty(); }

    static VectorWithMemoryTracking<String> normalizePaths(VectorWithMemoryTracking<String> paths)
    {
        std::sort(paths.begin(), paths.end());
        paths.erase(std::unique(paths.begin(), paths.end()), paths.end());

        VectorWithMemoryTracking<String> result;
        result.reserve(paths.size());
        for (auto & path : paths)
        {
            if (!matchesAnyPathOrSubtree(path, result))
                result.emplace_back(std::move(path));
        }
        return result;
    }

    static VectorWithMemoryTracking<String> normalizeStrings(VectorWithMemoryTracking<String> values)
    {
        std::sort(values.begin(), values.end());
        values.erase(std::unique(values.begin(), values.end()), values.end());
        return values;
    }

    static bool matchesAnyPathOrSubtree(std::string_view path, const VectorWithMemoryTracking<String> & paths)
    {
        while (true)
        {
            const auto it = std::lower_bound(
                paths.begin(),
                paths.end(),
                path,
                [](const String & lhs, std::string_view rhs) { return lhs.compare(rhs) < 0; });
            if (it != paths.end() && *it == path)
                return true;

            const auto separator = path.find_last_of(".[");
            if (separator == std::string_view::npos)
                return false;
            path = path.substr(0, separator);
        }
    }

    static bool matchesAnyAncestor(std::string_view path, const VectorWithMemoryTracking<String> & paths)
    {
        String descendant_prefix(path);
        descendant_prefix += '.';
        auto it = std::lower_bound(paths.begin(), paths.end(), descendant_prefix);
        if (it != paths.end() && it->starts_with(descendant_prefix))
            return true;

        descendant_prefix.back() = '[';
        it = std::lower_bound(paths.begin(), paths.end(), descendant_prefix);
        return it != paths.end() && it->starts_with(descendant_prefix);
    }

    static bool matchesAnyRegexp(std::string_view path, const Regexps & regexps)
    {
        return std::any_of(
            regexps.begin(),
            regexps.end(),
            [&](const auto & regexp) { return re2::RE2::PartialMatch(path, *regexp); });
    }

    static void compileRegexps(const VectorWithMemoryTracking<String> & regexp_strings, Regexps & regexps)
    {
        regexps.reserve(regexp_strings.size());
        for (const auto & regexp_string : regexp_strings)
        {
            auto regexp = std::make_unique<re2::RE2>(regexp_string, regexpOptions());
            if (!regexp->ok())
                throw Exception(
                    DB::ErrorCodes::CANNOT_COMPILE_REGEXP,
                    "Invalid regexp '{}': {}",
                    regexp_string,
                    regexp->error());
            regexps.emplace_back(std::move(regexp));
        }
    }

    static re2::RE2::Options regexpOptions()
    {
        re2::RE2::Options options;
        options.set_log_errors(false);
        return options;
    }

    VectorWithMemoryTracking<String> include_paths;
    VectorWithMemoryTracking<String> include_path_regexps;
    VectorWithMemoryTracking<String> skip_paths;
    VectorWithMemoryTracking<String> skip_path_regexps;
    Regexps include_regexps;
    Regexps skip_regexps;
};

inline constexpr UInt64 DEFAULT_MAX_TOKEN_BYTES = 1024;
inline constexpr UInt64 MAX_TOKEN_BYTES = 1024 * 1024;
inline constexpr UInt64 TOKEN_FORMAT_VERSION = 1;
inline constexpr size_t VALUE_HASH_BYTES = 8;
inline constexpr UInt64 VALUE_HASH_KEY0 = 0;
inline constexpr UInt64 VALUE_HASH_KEY1 = 0;

inline bool isValidMaxTokenBytes(UInt64 value)
{
    return value > 0 && value <= MAX_TOKEN_BYTES;
}

struct IndexConfiguration
{
    UInt64 token_format_version;
    UInt64 max_token_bytes;
    std::shared_ptr<const PathMatcher> path_matcher;
};

/// `jsonPathValues` tokens are persisted as text-index dictionary keys. Path, type, and map-key
/// components use an order-preserving encoding: zero bytes become `00 01`, and `00 00` terminates
/// the component. Complete values store their full text serialization. Truncated values store the
/// longest fitting text prefix followed by the little-endian 64-bit SipHash-2-4 with a zero key.
/// Scalar descendants of `Array(JSON)` use their `[]` path, binary `Array(T)` type, and scalar
/// kinds. The type/kind pair distinguishes them from legal literal array paths with the same text,
/// which use array-element kinds.
/// The text-index part header stores `TOKEN_FORMAT_VERSION`. Kind values are append-only.
enum class Kind : UInt8
{
    ScalarComplete = 1,
    ScalarTruncated = 2,
    ArrayElementComplete = 3,
    ArrayElementTruncated = 4,
    MapEntryComplete = 5,
    MapEntryTruncated = 6,
    DynamicValidation = 7,
};

using ValueHash = std::array<char, VALUE_HASH_BYTES>;

inline ValueHash valueHashFromUInt64(UInt64 hash)
{
    ValueHash result;
    for (size_t i = 0; i != result.size(); ++i)
        result[i] = static_cast<char>(hash >> (i * 8));
    return result;
}

inline ValueHash hashValue(std::string_view value)
{
    SipHash sip_hash(VALUE_HASH_KEY0, VALUE_HASH_KEY1);
    sip_hash.update(value);
    return valueHashFromUInt64(sip_hash.get64());
}

inline size_t getTruncatedValuePrefixSize(size_t value_capacity)
{
    return value_capacity >= VALUE_HASH_BYTES ? value_capacity - VALUE_HASH_BYTES : 0;
}

/// Appends the order-preserving escaped component (zero bytes become `00 01`) followed by
/// the `00 00` terminator. Returns false when the escaped component would not fit into
/// `max_token_bytes` (each byte is checked with room for its potential escape).
inline bool appendEscapedComponentBounded(String & out, std::string_view value, size_t max_token_bytes)
{
    for (const char byte : value)
    {
        if (out.size() + 2 > max_token_bytes)
            return false;
        out.push_back(byte);
        if (byte == 0)
            out.push_back(1);
    }
    if (out.size() + 2 > max_token_bytes)
        return false;
    out.append("\0\0", 2);
    return true;
}

inline void appendEscapedComponent(String & out, std::string_view value)
{
    appendEscapedComponentBounded(out, value, std::numeric_limits<size_t>::max());
}

inline void encodePathTypePrefix(String & result, std::string_view path, std::string_view binary_type)
{
    result.clear();
    result.reserve(path.size() + binary_type.size() + 4);
    appendEscapedComponent(result, path);
    appendEscapedComponent(result, binary_type);
}

inline String encodePathTypePrefix(std::string_view path, const DataTypePtr & type)
{
    String result;
    encodePathTypePrefix(result, path, type ? encodeDataType(type) : String{});
    return result;
}

inline std::optional<std::string_view> tryGetCompleteScalarValue(std::string_view token)
{
    size_t position = 0;
    auto skipComponent = [&]()
    {
        while (position < token.size())
        {
            if (token[position++] != 0)
                continue;
            if (position >= token.size())
                return false;
            const char escaped = token[position++];
            if (escaped == 0)
                return true;
            if (escaped != 1)
                return false;
        }
        return false;
    };

    if (!skipComponent())
        return std::nullopt;
    if (!skipComponent() || position >= token.size())
        return std::nullopt;
    if (static_cast<Kind>(token[position++]) != Kind::ScalarComplete)
        return std::nullopt;
    return token.substr(position);
}

struct EncodedValue
{
    String token;
    bool complete;
};

struct EncodedValueInfo
{
    bool complete;
};

inline std::optional<EncodedValueInfo> encodeValueTo(
    String & result,
    std::string_view prefix,
    std::string_view value,
    size_t max_token_bytes,
    bool value_is_complete = true,
    Kind complete_kind = Kind::ScalarComplete,
    Kind truncated_kind = Kind::ScalarTruncated,
    std::optional<ValueHash> value_hash = std::nullopt)
{
    if (prefix.size() + 1 > max_token_bytes)
        return std::nullopt;

    const size_t value_capacity = max_token_bytes - prefix.size() - 1;
    const bool complete = value_is_complete && value.size() <= value_capacity;
    if (!complete && value_capacity < VALUE_HASH_BYTES)
        return std::nullopt;

    const size_t value_prefix_size
        = complete ? value.size() : std::min(value.size(), getTruncatedValuePrefixSize(value_capacity));
    result.assign(prefix);
    result.push_back(static_cast<char>(complete ? complete_kind : truncated_kind));
    result.append(value.substr(0, value_prefix_size));

    if (!complete)
    {
        if (!value_hash)
            value_hash = hashValue(value);
        result.append(value_hash->data(), value_hash->size());
    }

    return EncodedValueInfo{complete};
}

inline std::optional<EncodedValue> encodeValue(
    String prefix,
    std::string_view value,
    size_t max_token_bytes,
    bool value_is_complete = true,
    Kind complete_kind = Kind::ScalarComplete,
    Kind truncated_kind = Kind::ScalarTruncated,
    std::optional<ValueHash> value_hash = std::nullopt)
{
    String result;
    const auto info = encodeValueTo(
        result,
        prefix,
        value,
        max_token_bytes,
        value_is_complete,
        complete_kind,
        truncated_kind,
        std::move(value_hash));
    if (!info)
        return std::nullopt;
    return EncodedValue{std::move(result), info->complete};
}

inline std::optional<EncodedValue> encodeValue(
    std::string_view path,
    const DataTypePtr & type,
    std::string_view value,
    size_t max_token_bytes,
    bool value_is_complete = true,
    Kind complete_kind = Kind::ScalarComplete,
    Kind truncated_kind = Kind::ScalarTruncated,
    std::optional<ValueHash> value_hash = std::nullopt)
{
    return encodeValue(
        encodePathTypePrefix(path, type),
        value,
        max_token_bytes,
        value_is_complete,
        complete_kind,
        truncated_kind,
        std::move(value_hash));
}

inline std::optional<EncodedValueInfo> encodeMapEntryTo(
    String & result,
    std::string_view prefix,
    std::string_view key,
    std::string_view value,
    size_t max_token_bytes,
    bool value_is_complete = true,
    std::optional<ValueHash> value_hash = std::nullopt)
{
    result.assign(prefix);
    if (result.size() + 1 > max_token_bytes)
        return std::nullopt;

    const size_t kind_position = result.size();
    result.push_back(0);
    if (!appendEscapedComponentBounded(result, key, max_token_bytes))
        return std::nullopt;

    const size_t value_capacity = max_token_bytes - result.size();
    const bool complete = value_is_complete && value.size() <= value_capacity;
    if (!complete && value_capacity < VALUE_HASH_BYTES)
        return std::nullopt;

    const size_t value_prefix_size
        = complete ? value.size() : std::min(value.size(), getTruncatedValuePrefixSize(value_capacity));
    result[kind_position] = static_cast<char>(complete ? Kind::MapEntryComplete : Kind::MapEntryTruncated);
    result.append(value.substr(0, value_prefix_size));
    if (!complete)
    {
        if (!value_hash)
            value_hash = hashValue(value);
        result.append(value_hash->data(), value_hash->size());
    }
    return EncodedValueInfo{complete};
}

inline std::optional<EncodedValue> encodeMapEntry(
    String prefix,
    std::string_view key,
    std::string_view value,
    size_t max_token_bytes,
    bool value_is_complete = true,
    std::optional<ValueHash> value_hash = std::nullopt)
{
    String result;
    const auto info = encodeMapEntryTo(
        result, prefix, key, value, max_token_bytes, value_is_complete, std::move(value_hash));
    if (!info)
        return std::nullopt;
    return EncodedValue{std::move(result), info->complete};
}

inline std::optional<String> encodeMapEntryPrefix(
    std::string_view prefix, std::string_view key, Kind kind, size_t max_token_bytes)
{
    String result(prefix);
    if (result.size() + 1 > max_token_bytes)
        return std::nullopt;
    result.push_back(static_cast<char>(kind));
    if (!appendEscapedComponentBounded(result, key, max_token_bytes))
        return std::nullopt;
    return result;
}

inline std::optional<String> encodeDynamicValidation(std::string_view path, size_t max_token_bytes)
{
    String result = encodePathTypePrefix(path, nullptr);
    if (result.size() + 1 > max_token_bytes)
        return std::nullopt;
    result.push_back(static_cast<char>(Kind::DynamicValidation));
    return result;
}

}
