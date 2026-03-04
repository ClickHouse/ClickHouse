#pragma once

#include <base/types.h>

#include <chrono>
#include <optional>
#include <utility>
#include <vector>

namespace DB
{

/// Abstract interface for a remote (external) cache backend.
///
/// Template parameters:
///   TKey   - must provide `encodeToRedisKey()` returning String (used as the remote key string),
///            as well as `serializeTo(WriteBuffer &)` / `static deserializeFrom(ReadBuffer &)`.
///   TValue - must provide `serializeTo(WriteBuffer &, const Block & header)` /
///            `static deserializeFrom(ReadBuffer &, const Block & header)`.
///
/// Error handling contract:
///   - `getWithKey` and `set`: must never throw; swallow all exceptions and log them.
///   - `remove`, `clearByTag`, `clear`: may throw on failure.
///   - `dump` and `count`: may throw on failure.
template <typename TKey, typename TValue>
class IRemoteCacheBackend
{
public:
    virtual ~IRemoteCacheBackend() = default;

    /// Read entry by key. Returns nullopt on miss or on any error (silently degraded).
    virtual std::optional<std::pair<TKey, TValue>> getWithKey(const TKey & key) = 0;

    /// Write entry only if the key does not already exist, with the given TTL.
    /// Failures are logged and silently discarded (must not throw).
    virtual void set(const TKey & key, const TValue & value, std::chrono::milliseconds ttl) = 0;

    /// Delete a single entry by key. Throws on failure.
    virtual void remove(const TKey & key) = 0;

    /// Delete all entries whose Redis key starts with the given tag prefix. Throws on failure.
    virtual void clearByTag(const String & tag) = 0;

    /// Delete all entries in the backend (FLUSHDB). Throws on failure.
    virtual void clear() = 0;

    /// Return up to max_keys entries (used for system.query_cache). Throws on failure.
    virtual std::vector<std::pair<TKey, TValue>> dump(size_t max_keys) = 0;

    /// Return the total number of entries in the backend. Throws on failure.
    virtual size_t count() = 0;
};

}
