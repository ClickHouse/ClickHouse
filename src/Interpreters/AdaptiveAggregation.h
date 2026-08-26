#pragma once

#include <memory>

namespace DB
{

/// Shared state of the adaptive aggregation.
/// One instance per aggregation, created by `AggregatingStep` when the query qualifies,
/// owned by `ManyAggregatedData` and shared by all its transforms.
///
/// Production phase: every thread aggregates into its own local hash table as usual, until the
/// table holds `adaptive_aggregator_freeze_threshold` keys and freezes. From that point a row
/// whose key the table already holds (a frequent key, learned for free from the first rows)
/// keeps aggregating in place with zero coordination, while a miss (a rare key) is not inserted
/// anywhere: it becomes a delayed record in one of the 256 backlogs, chosen by the two-level
/// bucket of the key's hash. A record is the key value itself with a run-length count when the
/// only aggregate is count, and otherwise the key plus its row's aggregate-argument values,
/// gathered into dense per-block columns at publish so the source block is released; both
/// carry the precomputed routing hash. Nothing is drained while production runs unless memory
/// demands it: past the external-aggregation threshold a pressure sweep drains the backlogs
/// early into the shared routing table and, if that is not enough, spills the routing table
/// through the ordinary external-aggregation machinery, so the memory bound holds.
///
/// Two guards hand the work back to the baseline path, with its ordinary byte-triggered
/// two-level conversion, when freezing cannot pay. A table that consumes many times the
/// threshold in rows while staying below it in keys gives up on freezing, per thread: the
/// stream has few groups (typically with fat states, which want the conversion and its
/// bucket-parallel merge). And when the staged stream as a whole proves to repeat the same keys
/// over and over, every thread thaws its table. A key's first staged record is the price of
/// storing it once; every repeat is bytes the baseline would have absorbed as a cheap
/// in-place update. The thaw therefore fires once the wasted staged bytes per distinct key
/// exceed a bound, which stands repetitive streams down early in proportion to how heavy
/// their keys and arguments are. The thaw verdict is remembered in the hash-table
/// statistics, so later runs of the query skip the engagement altogether instead of
/// re-measuring the stream.
///
/// Merge phase: at the end of input every local table converts to two-level and the standard
/// bucket-parallel merge runs, except that the merge task owning bucket b first drains backlog b
/// into the destination's bucket b (it is the exclusive owner, so no locks are needed) and only
/// then folds the locals' bucket b in as usual.
///
/// The net effect: frequent keys stay in small cache-resident tables, and a rare key is stored
/// and emplaced exactly once, by one thread, instead of once per thread that saw it.
struct AdaptiveAggregationSession;
using AdaptiveAggregationSessionPtr = std::shared_ptr<AdaptiveAggregationSession>;

/// Per-transform context of the adaptive aggregation: the thread's lifecycle phase, per-block
/// staging for the missed rows, and the buffered chunks awaiting coalescing.
struct AdaptiveAggregationProducer;

/// All delayed records of one consumed block, grouped by bucket. A published chunk is
/// immutable; only the producer building a chunk holds it mutably.
struct StagedChunk;
using StagedChunkPtr = std::shared_ptr<const StagedChunk>;
using MutableStagedChunkPtr = std::shared_ptr<StagedChunk>;

/// A published chunk's shared aggregate-instruction preparation (see `prepareStagedChunk`).
struct StagedChunkPreparation;

/// Who owns a staged key once it is emplaced into a table: the merge-time drain borrows the
/// chunk's bytes (the chunks are retained until after the conversion), while a pressure-time
/// drain copies them into the bucket's arena, because freeing the chunks is its purpose.
enum class AdaptiveKeyStorage
{
    BorrowFromChunk,
    CopyToArena,
};

}
