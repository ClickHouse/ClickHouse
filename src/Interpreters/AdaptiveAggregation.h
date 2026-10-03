#pragma once

#include <memory>
#include <string_view>
#include <vector>

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
/// anywhere: it becomes a delayed record, appended to the thread's own buffer of the partition its
/// key's hash falls in. A partition is a slice of a two-level bucket. A record carries the key's hash and
/// bytes, plus a run-length count when the only aggregate is count, or the row's aggregate-argument values
/// otherwise; it never points into the source block, so the block is released.
///
/// A table also freezes where the baseline would convert it to two-level, which catches the few
/// groups whose states own heap memory (`uniqExact` per region): such a table never fills in keys
/// or in its own footprint, and the adaptive merge gives it the bucket-parallel merge the conversion
/// is for. A table that reaches no bound is small and keeps learning, like a small baseline table;
/// a frozen one whose states keep growing is written to disk over the external-aggregation
/// threshold and learns again from empty.
///
/// One guard hands the work back to the baseline path when freezing cannot pay: when a thread's
/// staged stream proves to repeat the same keys over and over, the thread thaws its table. A key's
/// first staged record is the price of storing it once; every repeat is bytes the baseline would
/// have absorbed as a cheap in-place update. The thaw therefore fires once the wasted staged bytes
/// per distinct key exceed a bound, which stands repetitive streams down early in proportion to how
/// heavy their keys and arguments are, but only while the staged records are a fair share of the
/// thread's rows: a table that absorbs nearly every row in place loses little to a sliver of
/// repeated misses.
///
/// Merge phase: at the end of input every thread hands its partitions to the session and its local table converts
/// to two-level. The merge task owning bucket b then merges the bucket's partitions a few at a time: it drains every
/// thread's records of those partitions and the locals' cells routed to them into one table sized for them, converts
/// that table into a chunk and frees the partitions' memory, so the table stays in the cache and the staged memory
/// shrinks as the merge proceeds. When the aggregation feeds `ORDER BY count() DESC LIMIT n`, the merge skips the
/// partitions, records and cells whose groups provably cannot reach the top (see `AdaptiveTopKPruning`).
///
/// The net effect: frequent keys stay in small cache-resident tables, and a rare key is stored
/// and emplaced exactly once, by one thread, instead of once per thread that saw it.
struct AdaptiveAggregationSession;
using AdaptiveAggregationSessionPtr = std::shared_ptr<AdaptiveAggregationSession>;

/// Per-transform context of the adaptive aggregation: the thread's lifecycle phase, its staged records, and the
/// per-block staging of the missed rows.
struct AdaptiveAggregationProducer;

/// The record layout of the aggregate arguments that general payloads stage.
struct AdaptiveArgumentLayout;

/// The working memory an adaptive merge task keeps across the buckets it merges.
struct AdaptiveMergeScratch;

/// The bin-bound pruning of an aggregation that feeds `ORDER BY count() DESC LIMIT n`.
struct AdaptiveTopKPruning;

/// The staged records of one partition as contiguous byte ranges of whole records: a producer's chunk, or a block
/// read back from a spill stream.
using AdaptiveRecordRanges = std::vector<std::string_view>;

}
