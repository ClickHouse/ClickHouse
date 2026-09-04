#pragma once

#include <Core/Names.h>
#include <Core/Field.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentMetrics.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/MergeAlgorithm.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/BackgroundProcessList.h>
#include <Interpreters/StorageID.h>
#include <boost/noncopyable.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <atomic>
#include <vector>


namespace CurrentMetrics
{
    extern const Metric Merge;
}

class MemoryTracker;

namespace DB
{

struct MergeInfo
{
    std::string database;
    std::string table;
    std::string result_part_name;
    std::string result_part_path;
    Array source_part_names;
    Array source_part_paths;
    std::string partition_id;
    std::string partition;
    bool is_mutation{};
    Float64 elapsed{};
    Float64 progress{};
    UInt64 num_parts{};
    UInt64 total_size_bytes_compressed{};
    UInt64 total_size_bytes_uncompressed{};
    UInt64 total_size_marks{};
    UInt64 total_rows_count{};
    UInt64 bytes_read_uncompressed{};
    UInt64 bytes_written_uncompressed{};
    UInt64 rows_read{};
    UInt64 rows_written{};
    UInt64 columns_written{};
    UInt64 memory_usage{};
    UInt64 thread_id{};
    std::string merge_type;
    std::string merge_algorithm;

    std::string current_projection;
    Float64 current_projection_progress{0};
    UInt64 current_projection_parts_merging{0};
    UInt64 current_projection_parts_remaining{0};
    Array projections_completed;
    Array projections_remaining;
};

struct FutureMergedMutatedPart;
using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

struct MergeListElement;
using MergeListEntry = BackgroundProcessListEntry<MergeListElement, MergeInfo>;

class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

struct Settings;

/// State used to cancel a running merge/mutation pipeline from another thread (e.g. KILL MUTATION).
/// It is kept in a `shared_ptr` (see `MergeListElement::pipeline_cancel_state`) so the cancellation
/// hook that invokes it outlives the `MergeListElement` that owns the entry: the hook captures this
/// state by value, so it never dereferences the (possibly already destroyed) merge list entry and
/// never locks a mutex that belongs to a freed object.
struct PipelineCancelState
{
    mutable std::mutex mutex;
    std::function<void()> hook;
};
using PipelineCancelStatePtr = std::shared_ptr<PipelineCancelState>;


struct MergeListElement : boost::noncopyable
{
    static const MergeTreePartInfo FAKE_RESULT_PART_FOR_PROJECTION;

    const StorageID table_id;
    std::string partition_id;
    std::string partition;

    const std::string result_part_name;
    const std::string result_part_path;
    MergeTreePartInfo result_part_info;
    bool is_mutation{};

    UInt64 num_parts{};
    Names source_part_names;
    Names source_part_paths;
    Int64 source_data_version{};

    Stopwatch watch;
    std::atomic<Float64> progress{};
    std::atomic<bool> is_cancelled{};

    /// Optional hook to cancel the running merge/mutation pipeline when the entry
    /// is cancelled (e.g. KILL MUTATION). Set and cleared by the owning task. The
    /// `PipelineCancelState` keeps its mutex (and the hook) alive independently of this
    /// entry, so a concurrent cancellation cannot touch a freed object.
    PipelineCancelStatePtr pipeline_cancel_state = std::make_shared<PipelineCancelState>();

    UInt64 total_size_bytes_compressed{};
    UInt64 total_size_bytes_uncompressed{};
    UInt64 total_size_marks{};
    UInt64 total_rows_count{};
    std::atomic<UInt64> bytes_read_uncompressed{};
    std::atomic<UInt64> bytes_written_uncompressed{};

    /// In case of Vertical algorithm they are actual only for primary key columns
    std::atomic<UInt64> rows_read{};
    std::atomic<UInt64> rows_written{};

    /// Updated only for Vertical algorithm
    std::atomic<UInt64> columns_written{};

    UInt64 thread_id;
    MergeType merge_type;
    /// Detected after merge already started
    std::atomic<MergeAlgorithm> merge_algorithm;

    /// Projection merge introspection.
    /// Updated by MergeTask when merging/rebuilding projections.
    mutable std::mutex projection_introspection_mutex;
    String current_projection;
    Names projections_done;
    Names projections_pending;

    /// Atomic fields for projection sub-merge progress (lock-free reads from system.merges).
    /// current_projection_progress is written by child MergeListElement via parent_progress pointer.
    std::atomic<Float64> current_projection_progress{0};
    std::atomic<UInt64> current_projection_parts_merging{0};
    std::atomic<UInt64> current_projection_parts_remaining{0};

    /// When non-null, child MergeListElement writes its progress here.
    /// Points to parent's current_projection_progress. Safe because parent
    /// lifetime always exceeds child lifetime.
    std::atomic<Float64> * parent_progress{nullptr};

    ThreadGroupPtr thread_group;
    CurrentMetrics::Increment num_parts_metric_increment;

    MergeListElement(
        const StorageID & table_id_,
        FutureMergedMutatedPartPtr future_part,
        const ContextPtr & context);

    MergeInfo getInfo() const;

    const MemoryTracker & getMemoryTracker() const;

    MergeListElement * ptr() { return this; }

    MergeListElement & ref() { return *this; }

    ~MergeListElement();
};

/** Maintains a list of currently running merges.
  * For implementation of system.merges table.
  */
class MergeList final : public BackgroundProcessList<MergeListElement, MergeInfo>
{
private:
    using Parent = BackgroundProcessList<MergeListElement, MergeInfo>;
    std::atomic<size_t> merges_with_ttl_counter = 0;
    /// Set by cancelAll (on server shutdown): entries inserted after it are cancelled at birth.
    std::atomic<bool> all_cancelled = false;
public:
    MergeList()
        : Parent(CurrentMetrics::Merge)
    {}

    void onEntryDestroy(const Parent::Entry & entry) override
    {
        if (isTTLMergeType(entry->merge_type))
            --merges_with_ttl_counter;
    }

    void cancelPartMutations(const StorageID & table_id, const String & partition_id, Int64 mutation_version)
    {
        /// Mark entries as cancelled and collect their hooks under the merge list mutex, then invoke
        /// the hooks after it is released: a hook cancels a running pipeline, which must not touch
        /// the merge list under a lock. Marking is allocation-free and infallible and always precedes
        /// hook collection, so no matching entry escapes cancellation; the hook pass is best-effort
        /// and only affects how promptly the in-flight pipeline is interrupted. The whole collect must
        /// not throw out of this function (a `std::function` copy can allocate, e.g. OOM): a throw here
        /// would break the callers, e.g. `StorageMergeTree::killMutation` erases the entry before
        /// calling us and must still remove the mutation file afterwards, and the for-loop of the
        /// replicated `killMutation` must still cancel the remaining partitions. Marking has already
        /// happened before collection, so a failure of the best-effort pass is logged and skipped
        /// without undoing any cancellation.
        std::vector<std::function<void()>> hooks_to_invoke;
        try
        {
            std::lock_guard lock{mutex};
            auto matches = [&](const MergeListElement & merge_element)
            {
                return (partition_id.empty() || merge_element.partition_id == partition_id)
                    && merge_element.table_id == table_id
                    && merge_element.source_data_version < mutation_version
                    && merge_element.result_part_info.getDataVersion() >= mutation_version;
            };
            for (auto & merge_element : entries)
                if (matches(merge_element))
                    merge_element.is_cancelled = true;
            for (auto & merge_element : entries)
            {
                if (!matches(merge_element))
                    continue;
                std::lock_guard hook_lock{merge_element.pipeline_cancel_state->mutex};
                if (merge_element.pipeline_cancel_state->hook)
                    hooks_to_invoke.push_back(merge_element.pipeline_cancel_state->hook);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        for (const auto & hook : hooks_to_invoke)
        {
            try
            {
                hook();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    /// Cancel all current merges and mutations, and also all inserted later.
    /// Used on server shutdown, when their results would be discarded anyway.
    void cancelAll()
    {
        /// Cancel every running merge/mutation in the table, including a still in-flight pipeline.
        /// `all_cancelled` prevents new entries from being inserted after this point (checked in `insert`).
        /// As in `cancelPartMutations`, the allocation-free marking pass is infallible and precedes the
        /// best-effort hook collection, so even if hook collection throws (OOM during `OOMCanary`
        /// shutdown) no entry escapes cancellation; the throw never propagates, keeping the callers of
        /// `cancelAll` (e.g. `OOMCanary`) consistent.
        all_cancelled = true;
        std::vector<std::function<void()>> hooks_to_invoke;
        try
        {
            std::lock_guard lock{mutex};
            for (auto & merge_element : entries)
                merge_element.is_cancelled = true;
            for (auto & merge_element : entries)
            {
                std::lock_guard hook_lock{merge_element.pipeline_cancel_state->mutex};
                if (merge_element.pipeline_cancel_state->hook)
                    hooks_to_invoke.push_back(merge_element.pipeline_cancel_state->hook);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        for (const auto & hook : hooks_to_invoke)
        {
            try
            {
                hook();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    template <typename... Args>
    EntryPtr insert(Args &&... args)
    {
        auto entry = Parent::insert(std::forward<Args>(args)...);
        if (all_cancelled)
            (*entry)->is_cancelled = true;
        return entry;
    }

    void cancelInPartition(const StorageID & table_id, const String & partition_id, Int64 delimiting_block_number)
    {
        /// Cancel every running merge/mutation in the partition, including a still in-flight pipeline.
        /// Same split into an infallible marking pass and a best-effort hook pass as the other cancels:
        /// marking always precedes hook collection and a failure of the latter is logged and skipped,
        /// never thrown out of this function.
        std::vector<std::function<void()>> hooks_to_invoke;
        try
        {
            std::lock_guard lock{mutex};
            auto matches = [&](const MergeListElement & merge_element)
            {
                return merge_element.table_id == table_id
                    && merge_element.partition_id == partition_id
                    && merge_element.result_part_info.min_block < delimiting_block_number;
            };
            for (auto & merge_element : entries)
                if (matches(merge_element))
                    merge_element.is_cancelled = true;
            for (auto & merge_element : entries)
            {
                if (!matches(merge_element))
                    continue;
                std::lock_guard hook_lock{merge_element.pipeline_cancel_state->mutex};
                if (merge_element.pipeline_cancel_state->hook)
                    hooks_to_invoke.push_back(merge_element.pipeline_cancel_state->hook);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        for (const auto & hook : hooks_to_invoke)
        {
            try
            {
                hook();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    /// Merge consists of two parts: assignment and execution. We add merge to
    /// merge list on execution, but checking merge list during merge
    /// assignment. This lead to the logical race condition (we can assign more
    /// merges with TTL than allowed). So we "book" merge with ttl during
    /// assignment, and remove from list after merge execution.
    ///
    /// NOTE: Not important for replicated merge tree, we check count of merges twice:
    /// in assignment and in queue before execution.
    void bookMergeWithTTL()
    {
        ++merges_with_ttl_counter;
    }

    void cancelMergeWithTTL()
    {
        --merges_with_ttl_counter;
    }

    size_t getMergesWithTTLCount() const
    {
        return merges_with_ttl_counter;
    }
};

}
