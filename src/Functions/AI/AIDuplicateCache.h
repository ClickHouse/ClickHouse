#pragma once

#include <Common/CacheBase.h>
#include <Common/SipHash.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Types.h>

#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>


namespace DB
{

class QueryStatus;

/// Outcome of one provider request, shared by every row in the query that had the same key.
/// `Pending` means the owner has not published yet. `QuotaSkip` is not stored in the cache:
/// it only wakes waiters, then the key is removed.
enum class AIReuseKind : uint8_t
{
    Pending,
    Success,
    ProviderFailure,
    QuotaSkip,
};

struct AIReuseEntry
{
    std::mutex mutex;
    std::condition_variable cv;
    AIReuseKind kind = AIReuseKind::Pending;
    String text;
    VectorWithMemoryTracking<Float32> embedding;
    std::exception_ptr exception;
    /// Byte weight known before the body arrives (for example `dimensions * sizeof(Float32)`).
    /// `CacheBase` records weight at insertion, so a later `set` refreshes it once the body is known.
    size_t reserved_bytes = 0;
};

using AIReuseEntryPtr = std::shared_ptr<AIReuseEntry>;

/// Snapshot safe to copy. `AIReuseEntry` itself owns a mutex.
struct AIReuseOutcome
{
    AIReuseKind kind = AIReuseKind::Pending;
    String text;
    VectorWithMemoryTracking<Float32> embedding;
    std::exception_ptr exception;
};

struct AIReuseKey
{
    UInt128 hash;

    bool operator==(const AIReuseKey & rhs) const { return hash == rhs.hash; }
};

struct AIReuseKeyHash
{
    size_t operator()(const AIReuseKey & key) const noexcept
    {
        SipHash sip;
        sip.update(key.hash);
        return static_cast<size_t>(sip.get64());
    }
};

struct AIReuseWeight
{
    size_t operator()(const AIReuseEntry & entry) const
    {
        const size_t bytes = sizeof(AIReuseEntry) + entry.text.size() + entry.embedding.size() * sizeof(Float32);
        return bytes > entry.reserved_bytes ? bytes : entry.reserved_bytes;
    }
};


/// Per-query LRU cache of duplicate AI-function inputs. One instance lives on the query context
/// and is destroyed with it. `getOrSet` only allocates the placeholder; the HTTP call happens
/// after the claim returns, so a waiter blocks on `AIReuseEntry` and not inside `CacheBase`.
///
/// A waiter must not block on an AI request pool thread. Only the owner may perform the HTTP call.
/// A pool full of waiters deadlocks if the owner also needs a pool thread.
class AIDuplicateCache : public CacheBase<AIReuseKey, AIReuseEntry, AIReuseKeyHash, AIReuseWeight>
{
public:
    using Base = CacheBase<AIReuseKey, AIReuseEntry, AIReuseKeyHash, AIReuseWeight>;

    explicit AIDuplicateCache(size_t max_size_in_bytes);

    /// `owned` means this caller must compute the value and publish it. False means the entry is
    /// already stored or another caller is computing it; `wait` until `kind` leaves `Pending`.
    std::pair<AIReuseEntryPtr, bool> claim(const AIReuseKey & key, size_t reserved_bytes);

    void publishSuccessText(const AIReuseKey & key, const AIReuseEntryPtr & entry, String text);
    void publishSuccessEmbedding(const AIReuseKey & key, const AIReuseEntryPtr & entry, VectorWithMemoryTracking<Float32> embedding);
    void publishProviderFailure(const AIReuseKey & key, const AIReuseEntryPtr & entry, std::exception_ptr exception);
    /// Wakes waiters, then drops the key so a quota skip does not occupy the cache.
    void publishQuotaSkip(const AIReuseKey & key, const AIReuseEntryPtr & entry);

    /// Blocks while the entry is `Pending`. Between waits, checks query cancellation and the time limit.
    AIReuseOutcome wait(const AIReuseEntryPtr & entry, QueryStatus * process_list) const;
};


/// Constant prefix of one chat-function call. Hashed once per block, then combined with each row's text.
UInt128 hashAIChatPrefix(
    std::string_view provider,
    std::string_view endpoint,
    std::string_view api_version,
    std::string_view api_key,
    std::string_view model,
    float temperature,
    UInt64 max_tokens,
    std::string_view system_prompt,
    std::string_view response_format);

UInt128 hashAIEmbeddingPrefix(
    std::string_view provider,
    std::string_view endpoint,
    std::string_view api_version,
    std::string_view api_key,
    std::string_view model,
    UInt64 dimensions);

AIReuseKey hashAIReuseKey(UInt128 prefix, std::string_view text);

/// Bytes to reserve for an embedding of `dimensions` floats before the vector arrives.
/// Zero when `dimensions` is 0 or `dimensions * sizeof(Float32)` does not fit in `size_t`.
size_t aiEmbeddingReservedBytes(UInt64 dimensions);

}
