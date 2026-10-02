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
