#include <Functions/AI/AIDuplicateCache.h>

#include <Common/Exception.h>
#include <Interpreters/ProcessList.h>

#include <chrono>
#include <cstring>
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

void publishKind(
    AIReuseEntry & entry,
    AIReuseKind kind,
    String text,
    VectorWithMemoryTracking<Float32> embedding,
    std::exception_ptr exception)
{
    {
        std::lock_guard lock(entry.mutex);
        entry.text = std::move(text);
        entry.embedding = std::move(embedding);
        entry.exception = std::move(exception);
        entry.kind = kind;
    }
    entry.cv.notify_all();
}

/// Length-prefix each string so concatenated fields cannot collide at a boundary.
void updateDelimited(SipHash & hash, std::string_view value)
{
    const UInt64 size = value.size();
    hash.update(size);
    hash.update(value);
}

}


AIDuplicateCache::AIDuplicateCache(size_t max_size_in_bytes)
    : Base(
        "LRU",
        CurrentMetrics::end(),
        CurrentMetrics::end(),
        max_size_in_bytes,
        NO_MAX_COUNT,
        DEFAULT_SIZE_RATIO)
{
    if (max_size_in_bytes == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "'ai_function_reuse_identical_max_size_bytes' must be positive when 'ai_function_reuse_identical' is enabled");
}

std::pair<AIReuseEntryPtr, bool> AIDuplicateCache::claim(const AIReuseKey & key, size_t reserved_bytes)
{
    /// `load_func` only allocates the placeholder. The owner performs the provider call after
    /// `claim` returns, and waiters block in `wait`, not on an AI request pool thread.
    return getOrSet(key, [&]
    {
        auto created = std::make_shared<AIReuseEntry>();
        created->reserved_bytes = reserved_bytes;
        return created;
    });
}

void AIDuplicateCache::publishSuccessText(const AIReuseKey & key, const AIReuseEntryPtr & entry, String text)
{
    publishKind(*entry, AIReuseKind::Success, std::move(text), {}, nullptr);
    /// Refresh the weight now that the text size is known. Eviction may drop the entry; waiters
    /// already hold `entry` and still observe the published outcome.
    set(key, entry);
}

void AIDuplicateCache::publishSuccessEmbedding(
    const AIReuseKey & key, const AIReuseEntryPtr & entry, VectorWithMemoryTracking<Float32> embedding)
{
    publishKind(*entry, AIReuseKind::Success, {}, std::move(embedding), nullptr);
    set(key, entry);
}

void AIDuplicateCache::publishProviderFailure(
    const AIReuseKey & key, const AIReuseEntryPtr & entry, std::exception_ptr exception)
{
    publishKind(*entry, AIReuseKind::ProviderFailure, {}, {}, std::move(exception));
    set(key, entry);
}

void AIDuplicateCache::publishQuotaSkip(const AIReuseKey & key, const AIReuseEntryPtr & entry)
{
    publishKind(*entry, AIReuseKind::QuotaSkip, {}, {}, nullptr);
    remove(key);
}

AIReuseOutcome AIDuplicateCache::wait(const AIReuseEntryPtr & entry, QueryStatus * process_list) const
{
    while (true)
    {
        if (process_list)
            process_list->checkTimeLimit();

        std::unique_lock lock(entry->mutex);
        if (entry->kind != AIReuseKind::Pending)
        {
            AIReuseOutcome out;
            out.kind = entry->kind;
            out.text = entry->text;
            out.embedding = entry->embedding;
            out.exception = entry->exception;
            return out;
        }

        entry->cv.wait_for(lock, std::chrono::milliseconds(100), [&] { return entry->kind != AIReuseKind::Pending; });
    }
}

UInt128 hashAIChatPrefix(
    std::string_view provider,
    std::string_view endpoint,
    std::string_view api_version,
    std::string_view api_key,
    std::string_view model,
    float temperature,
    UInt64 max_tokens,
    std::string_view system_prompt,
    std::string_view response_format)
{
    SipHash hash;
    const UInt8 tag = 'c';
    hash.update(tag);
    updateDelimited(hash, provider);
    updateDelimited(hash, endpoint);
    updateDelimited(hash, api_version);
    updateDelimited(hash, api_key);
    updateDelimited(hash, model);

    UInt32 temperature_bits = 0;
    static_assert(sizeof(temperature_bits) == sizeof(temperature));
    memcpy(&temperature_bits, &temperature, sizeof(temperature_bits));
    hash.update(temperature_bits);
    hash.update(max_tokens);
    updateDelimited(hash, system_prompt);
    updateDelimited(hash, response_format);
    return hash.get128();
}

UInt128 hashAIEmbeddingPrefix(
    std::string_view provider,
    std::string_view endpoint,
    std::string_view api_version,
    std::string_view api_key,
    std::string_view model,
    UInt64 dimensions)
{
    SipHash hash;
    const UInt8 tag = 'e';
    hash.update(tag);
    updateDelimited(hash, provider);
    updateDelimited(hash, endpoint);
    updateDelimited(hash, api_version);
    updateDelimited(hash, api_key);
    updateDelimited(hash, model);
    hash.update(dimensions);
    return hash.get128();
}

AIReuseKey hashAIReuseKey(UInt128 prefix, std::string_view text)
{
    SipHash hash;
    hash.update(prefix);
    updateDelimited(hash, text);
    return AIReuseKey{hash.get128()};
}

size_t aiEmbeddingReservedBytes(UInt64 dimensions)
{
    if (dimensions == 0)
        return 0;

    constexpr size_t float_bytes = sizeof(Float32);
    if (dimensions > std::numeric_limits<size_t>::max() / float_bytes)
        return 0;

    return static_cast<size_t>(dimensions) * float_bytes;
}

}
