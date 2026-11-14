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
class ActionsDAG;
class Exception;
class QueryResultPreviewsControl;

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
  *
  * Previews of one stage may travel to the output through different branches of the pipeline (for
  * example, the parallel transforms of a sorting after an aggregation), so a newer preview can
  * overtake an older one. Every preview carries a sequence number assigned at its emission, which
  * is monotonic across all the stages, and the processors that merge several inputs, as well as the
  * output format, drop a preview older than the one they have already forwarded.
  *
  * A preview holds intermediate values that the result of the query never has (for example, a
  * partial `count()` that is zero), so a function downstream may throw on it while it would not
  * throw on the result. Such an error must not fail the query: the processor that evaluates the
  * preview drops it and disables the previews of its stage (see `onQueryResultPreviewEvaluationError`).
  */
class QueryResultPreviewInfo : public ChunkInfoCloneable<QueryResultPreviewInfo>
{
public:
    QueryResultPreviewInfo(UInt64 sequence_number_, std::shared_ptr<QueryResultPreviewsControl> control_)
        : sequence_number(sequence_number_), control(std::move(control_))
    {
    }
    QueryResultPreviewInfo(const QueryResultPreviewInfo & other) = default;

    const UInt64 sequence_number;

    /// The control of the stage that emitted the preview, so that a processor downstream can
    /// disable the previews of the stage.
    const std::shared_ptr<QueryResultPreviewsControl> control;
};

bool isQueryResultPreview(const Chunk & chunk);

/// Annotates a freshly emitted preview with the next sequence number and the control of the stage.
/// Emitters call it while holding the emission lock of their stage, so that the order of the
/// sequence numbers is the order of the snapshots.
void markAsQueryResultPreview(Chunk & chunk, std::shared_ptr<QueryResultPreviewsControl> control);

/// The sequence number of a preview chunk, or 0 for a chunk which is not a preview.
UInt64 getQueryResultPreviewSequenceNumber(const Chunk & chunk);

/// Whether an expression can be evaluated on preview chunks without changing the result of the
/// query:
/// - no stateful function (`isStateful`, including the body of a lambda): it carries its state from
///   a preview to the real data, e.g. `rowNumberInAllBlocks` would count the rows of the previews;
/// - no function with observable side effects;
/// - no `arrayJoin`.
/// A function that can throw on a preview is allowed (see `onQueryResultPreviewEvaluationError`).
/// The check is fail-close: an expression that it rejects keeps the preview emitters dormant.
bool canEvaluateOnQueryResultPreviews(const ActionsDAG & expression);

/// Called from the `catch` block of a processor whose evaluation of a preview chunk threw `error`.
/// Rethrows the error if it is not caused by the values of the preview: a cancellation of the
/// query (`is_cancelled`), or a logical error. Otherwise disables the previews of the stage that
/// emitted the preview - the next previews would likely fail in the same way - and returns; the
/// caller then drops the preview chunk.
void onQueryResultPreviewEvaluationError(const Exception & error, const QueryResultPreviewInfo & preview, bool is_cancelled);

/// The same for an error while building a preview of the stage controlled by `control` (for
/// example, the finalization of an aggregation snapshot, where an aggregate function may throw on
/// an intermediate state): rethrows a cancellation or a logical error, otherwise disables the
/// previews of the stage, and the caller drops the preview.
void onQueryResultPreviewEvaluationError(const Exception & error, QueryResultPreviewsControl & control, bool is_cancelled);

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

/// Implemented by processors that can emit preview chunks. `QueryPipeline::complete` activates the
/// control of a preview-emitting stage after verifying that every processor downstream of its
/// participants supports preview chunks and never mixes them with chunks of another source.
class IQueryResultPreviewEmitter
{
public:
    /// The control shared by all participants of the stage; nullptr when this processor cannot emit previews.
    virtual QueryResultPreviewsControl * getQueryResultPreviewsControl() = 0;
    virtual ~IQueryResultPreviewEmitter() = default;
};

}
