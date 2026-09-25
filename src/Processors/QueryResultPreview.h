#pragma once

#include <Processors/Chunk.h>
#include <base/types.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{

struct Settings;

/** Marks a chunk as a preview of the query result (see the `query_result_previews` setting).
  *
  * While a long-running query accumulates data in an aggregation or sorting transform, the transform
  * may periodically emit a snapshot of its current state as a single self-contained chunk annotated
  * with this info. The chunk then flows through the rest of the pipeline (projection expressions,
  * HAVING, sorting, LIMIT) like ordinary data, but every stateful processor on the way must treat it
  * out-of-band: apply its operation to the single chunk without mixing it into the accumulated state
  * and without advancing limits, quotas, or result counters. Each preview fully replaces the
  * previous one.
  *
  * Previews are enabled fail-close: emitters stay dormant unless `QueryPipeline::complete` proves
  * that every processor between the emitter and the output format declares
  * `IProcessor::supportsQueryResultPreviews` and the format itself can deliver previews.
  */
class QueryResultPreviewInfo : public ChunkInfoCloneable<QueryResultPreviewInfo>
{
public:
    QueryResultPreviewInfo() = default;
    QueryResultPreviewInfo(const QueryResultPreviewInfo & other) = default;
};

bool isQueryResultPreview(const Chunk & chunk);
void markAsQueryResultPreview(Chunk & chunk);

/// Thresholds extracted from the query settings. All frequency limits must pass for a preview
/// to be emitted: at least `min_interval_ms`/`min_rows`/`min_bytes` processed since the previous
/// preview; and the accumulated state must stay small enough (`max_result_rows`/`max_result_bytes`),
/// otherwise previews stop for the rest of the query.
struct QueryResultPreviewsSettings
{
    bool enabled = false;
    UInt64 min_interval_ms = 0;
    UInt64 min_rows = 0;
    UInt64 min_bytes = 0;
    UInt64 max_result_rows = 0;
    UInt64 max_result_bytes = 0;

    static QueryResultPreviewsSettings fromSettings(const Settings & settings);
};

/// Shared bookkeeping of one preview-emitting stage (e.g. all parallel aggregating transforms of
/// one aggregation). Thread-safe.
class QueryResultPreviewsControl
{
public:
    QueryResultPreviewsControl(const QueryResultPreviewsSettings & settings_, size_t num_participants);

    const QueryResultPreviewsSettings settings;

    /// Set by `QueryPipeline::complete` when the downstream path supports previews (fail-close:
    /// emitters stay dormant otherwise).
    void activate() { activated.store(true, std::memory_order_release); }
    bool isActivated() const { return activated.load(std::memory_order_acquire); }

    /// Permanently stop emitting previews (the state grew too large or spilled to disk).
    void disable() { disabled.store(true, std::memory_order_relaxed); }
    bool isDisabled() const { return disabled.load(std::memory_order_relaxed); }

    /// Account a consumed block and check the frequency thresholds. Returns true when the caller
    /// should attempt to emit a preview (under `emit_mutex`).
    bool accountAndCheckThresholds(UInt64 rows, UInt64 bytes);

    /// Resets the frequency counters; called by the emitter of a round under `emit_mutex`.
    void startNextRound();

    /// Serializes emission attempts across the participants.
    std::mutex emit_mutex;

    /// Protects the accumulated state of one participant: its owner takes the lock around state
    /// updates, the emitter of a round takes the locks one by one while snapshotting the states.
    std::mutex & participantMutex(size_t participant) { return *participant_mutexes[participant]; }

private:
    std::atomic<bool> activated{false};
    std::atomic<bool> disabled{false};
    std::atomic<UInt64> rows_since_last_preview{0};
    std::atomic<UInt64> bytes_since_last_preview{0};
    std::atomic<UInt64> last_preview_ns{0};
    std::vector<std::unique_ptr<std::mutex>> participant_mutexes;

    static UInt64 clockNanoseconds();
};

/// Implemented by processors that can emit preview chunks. `QueryPipeline::complete` activates an
/// emitter after verifying that every processor downstream of it supports preview chunks.
class IQueryResultPreviewEmitter
{
public:
    virtual void activateQueryResultPreviews() = 0;
    virtual ~IQueryResultPreviewEmitter() = default;
};

}
