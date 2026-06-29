#pragma once

#include <optional>
#include <string_view>

namespace DB
{

/// Query cache supports only Redis standalone mode, so there is no Redis Cluster hash tag in keys.
inline constexpr std::string_view QUERY_RESULT_CACHE_REDIS_KEY_PREFIX = "ch:qcache:";
inline constexpr std::string_view QUERY_RESULT_CACHE_REDIS_GENERATION_KEY_PREFIX = "ch:qcache:__generation__:";

namespace QueryResultCacheRedisKeyUtils
{

inline bool isDecimalNumber(std::string_view value)
{
    if (value.empty())
        return false;

    for (const char c : value)
    {
        if (c < '0' || c > '9')
            return false;
    }

    return true;
}

inline bool isLowerHexString(std::string_view value)
{
    if (value.empty())
        return false;

    for (const char c : value)
    {
        if ((c < '0' || c > '9') && (c < 'a' || c > 'f'))
            return false;
    }

    return true;
}

/// Parse the tag from a query cache Redis key using the exact key layout.
/// Accepts both data keys and their `:lock` counterparts.
inline std::optional<std::string_view> tryGetTagFromRedisKey(std::string_view redis_key)
{
    if (redis_key.ends_with(":lock"))
        redis_key.remove_suffix(std::string_view(":lock").size());

    if (!redis_key.starts_with(QUERY_RESULT_CACHE_REDIS_KEY_PREFIX))
        return std::nullopt;

    redis_key.remove_prefix(QUERY_RESULT_CACHE_REDIS_KEY_PREFIX.size());

    if (!redis_key.starts_with('v'))
        return std::nullopt;

    const size_t global_generation_end = redis_key.find(":t");
    if (global_generation_end == std::string_view::npos || !isDecimalNumber(redis_key.substr(1, global_generation_end - 1)))
        return std::nullopt;

    const size_t tag_generation_begin = global_generation_end + 2;
    const size_t tag_generation_end = redis_key.find(':', tag_generation_begin);
    if (tag_generation_end == std::string_view::npos || !isDecimalNumber(redis_key.substr(tag_generation_begin, tag_generation_end - tag_generation_begin)))
        return std::nullopt;

    const size_t tag_begin = tag_generation_end + 1;
    constexpr size_t redis_hash_size = 32;

    size_t tag_end = std::string_view::npos;
    constexpr std::string_view shared_scope_marker = ":shared:";
    constexpr std::string_view private_scope_marker = ":private:";

    if (redis_key.size() >= shared_scope_marker.size() + redis_hash_size)
    {
        const size_t shared_scope_marker_pos = redis_key.size() - (shared_scope_marker.size() + redis_hash_size);
        const std::string_view shared_ast_hash = redis_key.substr(redis_key.size() - redis_hash_size);
        if (redis_key.substr(shared_scope_marker_pos, shared_scope_marker.size()) == shared_scope_marker
            && isLowerHexString(shared_ast_hash))
        {
            tag_end = shared_scope_marker_pos;
        }
    }

    if (tag_end == std::string_view::npos)
    {
        constexpr size_t private_scope_hash_size = 32;
        if (redis_key.size() < private_scope_marker.size() + private_scope_hash_size + 1 + redis_hash_size)
            return std::nullopt;

        const size_t private_ast_hash_pos = redis_key.size() - redis_hash_size;
        const size_t private_scope_hash_pos = private_ast_hash_pos - 1 - private_scope_hash_size;
        if (redis_key[private_ast_hash_pos - 1] != ':')
            return std::nullopt;
        if (private_scope_hash_pos < private_scope_marker.size())
            return std::nullopt;

        const size_t private_scope_marker_pos = private_scope_hash_pos - private_scope_marker.size();
        const std::string_view private_scope_hash = redis_key.substr(private_scope_hash_pos, private_scope_hash_size);
        const std::string_view private_ast_hash = redis_key.substr(private_ast_hash_pos, redis_hash_size);
        if (redis_key.substr(private_scope_marker_pos, private_scope_marker.size()) != private_scope_marker
            || !isLowerHexString(private_scope_hash)
            || !isLowerHexString(private_ast_hash))
            return std::nullopt;

        tag_end = private_scope_marker_pos;
    }

    if (tag_end < tag_begin)
        return std::nullopt;

    return redis_key.substr(tag_begin, tag_end - tag_begin);
}

inline bool hasTag(std::string_view redis_key, std::string_view tag)
{
    const auto parsed_tag = tryGetTagFromRedisKey(redis_key);
    return parsed_tag.has_value() && *parsed_tag == tag;
}

}

}
