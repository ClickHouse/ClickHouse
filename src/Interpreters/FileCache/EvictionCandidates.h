#pragma once
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/CacheUsage.h>
#include <Interpreters/FileCache/FileCacheOriginInfo.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <deque>

namespace DB
{

/// Eviction info:
/// - contains information about how much size/elements is needed to be evicted
/// - holds "space holders", for space which was already available
///   and will now be "held" as reserved, while we are evicting remaining space.
/// If releaseHoldSpace() is not called,
/// held space will be automatically released in destructor of HoldSpacePtr.
struct QueueEvictionInfo;
using QueueEvictionInfoPtr = std::unique_ptr<QueueEvictionInfo>;

struct QueueEvictionInfo
{
    explicit QueueEvictionInfo(
        const std::string & description_,
        const FileCacheOriginInfo::UserID & user_id_)
        : description(description_), user_id(user_id_) {}

    const std::string description;
    const FileCacheOriginInfo::UserID user_id;

    size_t size_to_evict = 0;
    size_t elements_to_evict = 0;
    IFileCachePriority::HoldSpacePtr hold_space;

    /// Overwrite the eviction target with `other`'s. No accumulation.
    void setEvictTarget(const QueueEvictionInfo & other);

    /// Merge `other_hold` into ours so all reservations stay live until
    /// release. No-op if null.
    void absorbHoldSpace(IFileCachePriority::HoldSpacePtr other_hold);

    std::string toString() const;
    /// Whether actual eviction is needed to be done.
    bool requiresEviction() const { return size_to_evict || elements_to_evict; }
    /// Release hold space if still hold.
    void releaseHoldSpace(const CacheStateGuard::Lock & lock);
};
using QueueID = size_t;

class EvictionInfo;
using EvictionInfoPtr = std::unique_ptr<EvictionInfo>;

/// Aggregated eviction info:
/// - contains QueueEvictionInfo per queue_id
/// - aggregates all methods among all QueueEvictionInfo's.
class EvictionInfo : public absl::flat_hash_map<QueueID, QueueEvictionInfoPtr>, private boost::noncopyable
{
public:
    EvictionInfo() = default;
    /// Creates eviction info from a single QueueEvictionInfo.
    /// More infos can be added via add() method.
    explicit EvictionInfo(QueueID queue_id, QueueEvictionInfoPtr info);

    /// Release hold spaces (in the base map) before the `kept_alive_cache_usage`
    /// pins (members, destroyed first) drop — otherwise `~HoldSpace` could release
    /// into a per-user priority a concurrent `cache_usage.snapshot` already erased.
    ~EvictionInfo() { clear(); }

    /// Get eviction info by queue id.
    const QueueEvictionInfo & get(const QueueID & queue_id) const;
    /// Add eviction info under the queue_id.
    /// Throws exception if eviction info with the same queue_id already exists.
    void add(EvictionInfoPtr && info);
    void addOrUpdate(EvictionInfoPtr && info);

    size_t getSizeToEvict() const { return size_to_evict; }
    size_t getElementsToEvict() const { return elements_to_evict; }
    size_t getHoldSize() const { return hold_size; }
    size_t getHoldElements() const { return hold_elements; }
    /// Whether actual eviction is needed to be done.
    bool requiresEviction() const { return size_to_evict || elements_to_evict; }
    /// Whether we "hold" some space.
    bool hasHoldSpace() const { return hold_size || hold_elements; }
    /// Release hold space if still hold.
    void releaseHoldSpace(const CacheStateGuard::Lock & lock);

    std::string toString() const;

    /// Keep `usage` alive so a concurrent `cache_usage.snapshot` cannot destroy
    /// the user's per-user priority while we hold raw pointers into it.
    /// `shared_ptr` value dedupes: same user across iterations is stored once.
    void addCacheUsage(CacheUsagePtr usage) { kept_alive_cache_usage.insert(std::move(usage)); }

    /// Take over `other`'s pins. `add`/`addOrUpdate` must call this when merging:
    /// otherwise the source info's pins die with it while the merged entries
    /// still hold raw pointers into the pinned per-client priorities.
    void takeKeptAliveCacheUsage(EvictionInfo & other)
    {
        kept_alive_cache_usage.merge(other.kept_alive_cache_usage);
    }

private:
    /// On existing queue: replace target + merge holds if `replace_if_exists`, else throw.
    void addImpl(const QueueID & queue_id, QueueEvictionInfoPtr info, bool replace_if_exists);

    size_t size_to_evict = 0; /// Total size to evict among all eviction infos.
    size_t elements_to_evict = 0; /// Total elements to evict among all eviction infos.
    size_t hold_size = 0;     /// Total hold size among all eviction infos.
    size_t hold_elements = 0; /// Total hold elements among all eviction infos.

    absl::flat_hash_set<CacheUsagePtr> kept_alive_cache_usage;
};

class EvictionCandidates : private boost::noncopyable
{
public:
    using AfterEvictWriteCallback = std::function<void(const CachePriorityGuard::WriteLock & lk)>;
    using AfterEvictStateCallback = std::function<void(const CacheStateGuard::Lock & lk)>;

    explicit EvictionCandidates(IFileCachePriority::OnEvictCallback on_evict_callback_);
    ~EvictionCandidates();

    /// Total number of eviction candidates.
    size_t size() const { return candidates_size; }
    /// Total size in bytes of all eviction candidates.
    size_t bytes() const { return candidates_bytes; }

    auto begin(this auto && self) { return self.candidates.begin(); }
    auto end(this auto&& self) { return self.candidates.end(); }

    /// Add a new eviction candidate.
    void add(const FileSegmentMetadataPtr & candidate, LockedKey & locked_key);
    /// Append a callback to run after eviction. "write" func mutates the
    /// priority queue structure; "state" func mutates size/element counters.
    /// Overcommit retries can register multiple callbacks per pass; all run
    /// sequentially under one lock in `afterEvictWrite` / `afterEvictState`.
    void addAfterEvictWriteCallback(AfterEvictWriteCallback && func) { after_evict_write_callbacks.push_back(std::move(func)); }
    void addAfterEvictStateCallback(AfterEvictStateCallback && func) { after_evict_state_callbacks.push_back(std::move(func)); }

    /// Evict all candidates, which were added before via add().
    void evict();
    /// Execute "after eviction callbacks".
    /// "write" callback must be executed before "state" callback.
    void afterEvictWrite(const CachePriorityGuard::WriteLock & lock);
    void afterEvictState(const CacheStateGuard::Lock & lock);

    /// Whether calling afterEvictWrite() is required.
    /// (Can be used to avoid taking write lock)
    bool requiresAfterEvictWrite() const { return !after_evict_write_callbacks.empty(); }
    /// Whether calling afterEvictState() is required.
    /// (Can be used to avoid taking state lock)
    bool requiresAfterEvictState() const { return !after_evict_state_callbacks.empty() || !queue_entries_to_invalidate.empty(); }

    /// Used only for dynamic cache resize,
    /// allows to remove queue entries in advance.
    void removeQueueEntries(const CachePriorityGuard::WriteLock &);

    struct KeyCandidates
    {
        KeyMetadataPtr key_metadata;
        std::vector<FileSegmentMetadataPtr> candidates;
        std::vector<std::string> error_messages;
    };
    /// Get eviction candidates which failed to be evicted during evict().
    struct FailedCandidates
    {
        std::vector<KeyCandidates> failed_candidates_per_key;
        size_t total_cache_size = 0;
        size_t total_cache_elements = 0;

        size_t size() const { return failed_candidates_per_key.size(); }

        std::string getFirstErrorMessage() const;
    };

    FailedCandidates getFailedCandidates() const { return failed_candidates; }

    /// Get the original queue type of a candidate saved during removeQueueEntries.
    /// Returns None if not found (e.g., if removeQueueEntries was not called).
    IFileCachePriority::QueueEntryType getOriginalQueueType(const FileSegmentMetadata * candidate) const
    {
        auto it = original_queue_types.find(candidate);
        return it != original_queue_types.end() ? it->second : IFileCachePriority::QueueEntryType::None;
    }

private:
    absl::flat_hash_map<FileCacheKey, KeyCandidates, std::hash<FileCacheKey>> candidates;
    size_t candidates_size = 0;
    size_t candidates_bytes = 0;
    FailedCandidates failed_candidates;

    /// Saved original queue type per candidate, populated in removeQueueEntries.
    std::unordered_map<const FileSegmentMetadata *, IFileCachePriority::QueueEntryType> original_queue_types;

    std::vector<AfterEvictWriteCallback> after_evict_write_callbacks;
    std::vector<AfterEvictStateCallback> after_evict_state_callbacks;

    std::vector<IFileCachePriority::IteratorPtr> queue_entries_to_invalidate;
    bool removed_queue_entries = false;

    IFileCachePriority::HoldSpacePtr hold_space;

    IFileCachePriority::OnEvictCallback on_evict_callback;

    LoggerPtr log;
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
