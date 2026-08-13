#pragma once

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/re2.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <base/types.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

namespace DB::ErrorCodes
{
extern const int CANNOT_COMPILE_REGEXP;
}

namespace DB::JSONPathValues
{

class PathMatcher
{
public:
    PathMatcher(std::vector<String> skip_paths_, std::vector<String> skip_path_regexps_)
        : skip_path_regexps(std::move(skip_path_regexps_))
    {
        std::sort(skip_paths_.begin(), skip_paths_.end());
        skip_paths_.erase(std::unique(skip_paths_.begin(), skip_paths_.end()), skip_paths_.end());
        for (auto & path : skip_paths_)
        {
            if (skip_paths.empty() || !matchesPathOrSubtree(path, skip_paths.back()))
                skip_paths.emplace_back(std::move(path));
        }

        std::sort(skip_path_regexps.begin(), skip_path_regexps.end());
        skip_path_regexps.erase(
            std::unique(skip_path_regexps.begin(), skip_path_regexps.end()),
            skip_path_regexps.end());
        regexps.reserve(skip_path_regexps.size());
        for (const auto & regexp_string : skip_path_regexps)
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

    bool shouldSkip(std::string_view path) const
    {
        const auto it = std::upper_bound(skip_paths.begin(), skip_paths.end(), path);
        if (it != skip_paths.begin() && matchesPathOrSubtree(path, *std::prev(it)))
            return true;

        for (const auto & regexp : regexps)
        {
            if (re2::RE2::PartialMatch(path, *regexp))
                return true;
        }
        return false;
    }

    const std::vector<String> & getSkipPaths() const { return skip_paths; }
    const std::vector<String> & getSkipPathRegexps() const { return skip_path_regexps; }

private:
    static bool matchesPathOrSubtree(std::string_view path, std::string_view prefix)
    {
        if (!path.starts_with(prefix))
            return false;
        auto suffix = path.substr(prefix.size());
        if (suffix.empty() || suffix.starts_with('.'))
            return true;
        while (suffix.starts_with("[]"))
            suffix.remove_prefix(2);
        return suffix.empty() || suffix.starts_with('.');
    }

    static re2::RE2::Options regexpOptions()
    {
        re2::RE2::Options options;
        options.set_log_errors(false);
        return options;
    }

    std::vector<String> skip_paths;
    std::vector<String> skip_path_regexps;
    std::vector<std::unique_ptr<re2::RE2>> regexps;
};

inline constexpr UInt64 DEFAULT_MAX_TOKEN_BYTES = 1024;
inline constexpr UInt64 MAX_TOKEN_BYTES = 1024 * 1024;
inline constexpr size_t VALUE_HASH_BYTES = 8;
inline constexpr UInt64 VALUE_HASH_KEY0 = 0;
inline constexpr UInt64 VALUE_HASH_KEY1 = 0;

inline bool isValidMaxTokenBytes(UInt64 value)
{
    return value > 0 && value <= MAX_TOKEN_BYTES;
}

/// `jsonPathValues` tokens are persisted as text-index dictionary keys. Path, type, and map-key
/// components use an order-preserving encoding: zero bytes become `00 01`, and `00 00` terminates
/// the component. Complete values store their full text serialization. Truncated values store the
/// longest fitting text prefix followed by the little-endian 64-bit SipHash-2-4 with a zero key.
/// Scalar descendants of `Array(JSON)` use their `[]` path, binary `Array(T)` type, and scalar
/// kinds. The type/kind pair distinguishes them from legal literal array paths with the same text,
/// which use array-element kinds.
/// There is deliberately no format-version byte: this tokenizer is unpublished and incompatible
/// changes require rebuilding its materialized indexes. Kind values are append-only.
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

inline String encodePathPrefix(std::string_view path)
{
    String result;
    result.reserve(path.size() + 2);
    appendEscapedComponent(result, path);
    return result;
}

struct DecodedToken
{
    std::string_view encoded_path;
    std::string_view encoded_binary_type;
    Kind kind;
    std::optional<std::string_view> encoded_map_key;
    std::string_view value;
};

inline std::optional<DecodedToken> tryDecodeToken(std::string_view token)
{
    size_t position = 0;
    auto readComponent = [&]() -> std::optional<std::string_view>
    {
        const size_t begin = position;
        while (position < token.size())
        {
            if (token[position++] != 0)
                continue;
            if (position >= token.size())
                return std::nullopt;
            const char escaped = token[position++];
            if (escaped == 0)
            {
                return token.substr(begin, position - begin - 2);
            }
            if (escaped != 1)
                return std::nullopt;
        }
        return std::nullopt;
    };

    const auto path = readComponent();
    const auto binary_type = readComponent();
    if (!path || !binary_type || position >= token.size())
        return std::nullopt;

    const UInt8 kind = static_cast<UInt8>(token[position++]);
    if (kind < static_cast<UInt8>(Kind::ScalarComplete)
        || kind > static_cast<UInt8>(Kind::DynamicValidation))
        return std::nullopt;

    const auto decoded_kind = static_cast<Kind>(kind);
    std::optional<std::string_view> map_key;
    if (decoded_kind == Kind::MapEntryComplete || decoded_kind == Kind::MapEntryTruncated)
    {
        map_key = readComponent();
        if (!map_key)
            return std::nullopt;
    }

    const auto value = token.substr(position);
    if ((decoded_kind == Kind::ScalarTruncated
            || decoded_kind == Kind::ArrayElementTruncated
            || decoded_kind == Kind::MapEntryTruncated)
        && value.size() < VALUE_HASH_BYTES)
        return std::nullopt;
    if (decoded_kind == Kind::DynamicValidation && !value.empty())
        return std::nullopt;
    if (decoded_kind == Kind::DynamicValidation && !binary_type->empty())
        return std::nullopt;

    return DecodedToken{*path, *binary_type, decoded_kind, map_key, value};
}

inline std::optional<String> tryDecodeComponent(std::string_view encoded)
{
    String result;
    result.reserve(encoded.size());
    for (size_t position = 0; position < encoded.size(); ++position)
    {
        const char byte = encoded[position];
        if (byte != 0)
        {
            result.push_back(byte);
            continue;
        }
        if (++position >= encoded.size() || encoded[position] != 1)
            return std::nullopt;
        result.push_back(0);
    }
    return result;
}

struct EncodedValue
{
    String token;
    bool complete;
    size_t value_prefix_size;
};

struct EncodedValueInfo
{
    bool complete;
    size_t value_prefix_size;
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
    result.append(value.data(), value_prefix_size);

    if (!complete)
    {
        if (!value_hash)
            value_hash = hashValue(value);
        result.append(value_hash->data(), value_hash->size());
    }

    return EncodedValueInfo{complete, value_prefix_size};
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
    return EncodedValue{std::move(result), info->complete, info->value_prefix_size};
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
    result.append(value.data(), value_prefix_size);
    if (!complete)
    {
        if (!value_hash)
            value_hash = hashValue(value);
        result.append(value_hash->data(), value_hash->size());
    }
    return EncodedValueInfo{complete, value_prefix_size};
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
    return EncodedValue{std::move(result), info->complete, info->value_prefix_size};
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
