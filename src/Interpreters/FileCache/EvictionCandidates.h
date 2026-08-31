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
    void addAfterEvictWriteCallback(CachePriorityGuard * guard, AfterEvictWriteCallback && callback);
    void addAfterEvictStateCallback(AfterEvictStateCallback && callback);

    /// Remove the candidates' files from the filesystem and from the cache metadata.
    /// Takes no priority or state lock, only the per-key and per-file-segment locks.
    void evict();

    /// Run the callbacks registered above. `afterEvictWrite` must run before `afterEvictState`.
    void afterEvictWrite();
    void afterEvictState(const CacheStateGuard::Lock & lock);

    /// Whether calling afterEvictWrite() is required.
    /// (Can be used to avoid taking write lock)
    bool requiresAfterEvictWrite() const { return !after_evict_write_callbacks.empty(); }
    /// Whether calling afterEvictState() is required.
    /// (Can be used to avoid taking state lock)
    bool requiresAfterEvictState() const { return !after_evict_state_callbacks.empty() || !queue_entries_to_invalidate.empty(); }

    /// Used only for dynamic cache resize,
    /// allows to remove queue entries in advance.
    void removeQueueEntries();

    /// Undo `removeQueueEntries`, restoring entries to the queue they came from.
    /// Used by dynamic resize when it cannot go through with the new limits.
    void restoreQueueEntries(IFileCachePriority & priority, const CacheStateGuard::Lock & state_lock);

    /// Same as `restoreQueueEntries`, but only for the candidates which failed to be evicted in `evict`.
    void restoreFailedQueueEntries(IFileCachePriority & priority, const CacheStateGuard::Lock & state_lock);

    struct KeyCandidates
    {
        /// Both members are required: the restore paths lock `guard` and then `key_metadata`,
        /// so there is no valid state in which either is unset.
        KeyCandidates(KeyMetadataPtr key_metadata_, CachePriorityGuard * guard_)
            : key_metadata(std::move(key_metadata_)), guard(guard_)
        {
            chassert(key_metadata);
            chassert(guard);
        }

        KeyMetadataPtr key_metadata;
        /// Guard of the queue holding these entries; all candidates of one key share it.
        CachePriorityGuard * guard;
        std::vector<FileSegmentMetadataPtr> candidates;
        std::vector<std::string> error_messages;
    };
    struct FailedCandidates
    {
        std::vector<KeyCandidates> failed_candidates_per_key;
        size_t total_cache_size = 0;
        size_t total_cache_elements = 0;

        size_t size() const { return failed_candidates_per_key.size(); }

        std::string getFirstErrorMessage() const;
    };

    /// Get eviction candidates which failed to be evicted during `evict`.
    FailedCandidates getFailedCandidates() const { return failed_candidates; }

    /// Get the original queue type of a candidate saved during removeQueueEntries.
    /// Returns None if not found (e.g., if removeQueueEntries was not called).
    IFileCachePriority::QueueEntryType getOriginalQueueType(const FileSegmentMetadata * candidate) const
    {
        auto it = original_queue_types.find(candidate);
        return it != original_queue_types.end() ? it->second : IFileCachePriority::QueueEntryType::None;
    }

private:
    /// Restore the queue entries of one key. Never throws: each entry is restored independently,
    /// because abandoning the rest would leave their files on disk unaccounted in any queue.
    void restoreKeyCandidates(
        IFileCachePriority & priority,
        const CachePriorityGuard::WriteLock & lock,
        const CacheStateGuard::Lock & state_lock,
        const KeyMetadataPtr & key_metadata,
        const std::vector<FileSegmentMetadataPtr> & key_candidates);

    /// Eviction candidates grouped by key.
    absl::flat_hash_map<FileCacheKey, KeyCandidates, std::hash<FileCacheKey>> candidates;
    /// Candidate keys grouped by the guard protecting their queue entries;
    /// `removeQueueEntries` and `afterEvictWrite` take one write lock per group.
    std::unordered_map<CachePriorityGuard *, std::vector<FileCacheKey>> candidates_by_priority_guard;

    size_t candidates_size = 0; /// Total number of candidates
    size_t candidates_bytes = 0; /// Total eviction size of candidates

    std::unordered_map<CachePriorityGuard *, std::vector<AfterEvictWriteCallback>> after_evict_write_callbacks;
    std::vector<AfterEvictStateCallback> after_evict_state_callbacks;

    /// Queue entries of file segments which `evict` already removed from filesystem. They are
    /// invalidated (lock-free) in `afterEvictState` or the destructor; the background cleanup
    /// later removes the invalidated entries from the queue under the priority write lock.
    std::vector<IFileCachePriority::IteratorPtr> queue_entries_to_invalidate;

    const LoggerPtr log;

    /// Candidates which failed to be evicted in `evict`; used by dynamic resize
    /// to restore the state in case of logical error.
    FailedCandidates failed_candidates;
    /// Saved original queue type per candidate, populated in `removeQueueEntries`.
    std::unordered_map<const FileSegmentMetadata *, IFileCachePriority::QueueEntryType> original_queue_types;
    /// Set by `removeQueueEntries`: the candidates' queue entries are already removed. Only
    /// dynamic resize does this before evicting the files; ordinary eviction keeps the entries
    /// until the files are gone and invalidates them afterwards.
    bool removed_queue_entries = false;

    IFileCachePriority::OnEvictCallback on_evict_callback;
};

using EvictionCandidatesPtr = std::unique_ptr<EvictionCandidates>;

}
