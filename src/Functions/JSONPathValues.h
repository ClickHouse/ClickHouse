#pragma once

#include <Common/SipHash.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <base/types.h>

#include <array>
#include <optional>

namespace DB::JSONPathValues
{

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

inline void appendEscapedComponent(String & out, std::string_view value)
{
    for (const char byte : value)
    {
        out.push_back(byte);
        if (byte == 0)
            out.push_back(1);
    }
    out.append("\0\0", 2);
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

inline std::optional<Kind> tryGetKind(std::string_view token)
{
    const auto decoded = tryDecodeToken(token);
    if (!decoded)
        return std::nullopt;
    return decoded->kind;
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
    for (const char byte : key)
    {
        if (result.size() + 2 > max_token_bytes)
            return std::nullopt;
        result.push_back(byte);
        if (byte == 0)
            result.push_back(1);
    }
    if (result.size() + 2 > max_token_bytes)
        return std::nullopt;
    result.append("\0\0", 2);

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
    for (const char byte : key)
    {
        if (result.size() + 2 > max_token_bytes)
            return std::nullopt;
        result.push_back(byte);
        if (byte == 0)
            result.push_back(1);
    }
    if (result.size() + 2 > max_token_bytes)
        return std::nullopt;
    result.append("\0\0", 2);
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
