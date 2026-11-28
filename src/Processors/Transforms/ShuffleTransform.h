#pragma once

#include <Columns/IColumn.h>
#include <Processors/Chunk.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/IProcessor.h>
#include <Processors/ISimpleTransform.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>
#include <pcg_random.hpp>


namespace DB
{

/** Processors of `SHUFFLE` (and of `ORDER BY rand()`), which return the rows in a uniformly random order in O(n) time.
  *
  * The random choices are made just in time; no random values are stored for the rows:
  *
  * - Full shuffle: the rows are scattered into parts at random (`ScatterByPartitionTransform::createRandom`), every part
  *   is shuffled by `ShuffleTransform`, and the parts are concatenated by `ShuffleConcatTransform`. If every row goes into
  *   a uniformly random part and every part is uniformly shuffled, the concatenation is a uniformly random permutation:
  *   this is the same as sorting by independent uniformly random keys, with the part number as the high part of the key.
  *
  * - With `LIMIT n`: every stream is sampled by `ReservoirSampleTransform` (reservoir sampling), and the samples are merged
  *   by `MergingShuffledTransform`, which draws the number of rows to take from every stream as sampling without
  *   replacement from the populations of the streams.
  *
  * Sort keys are generated only if the result has to be sorted by them: for `ORDER BY rand()` (the `rand()` column must
  * be sorted) and for the output of a shard of a distributed query, which is merged by the key on the initiator.
  * The keys are generated in sorted order and have the distribution of sorted independent uniformly random numbers,
  * so the result is exactly as if the rows were sorted by random keys.
  */

/// The number of rows of a sample of a stream, or of the stream itself: the population.
/// Attached to the chunks of the samples, to merge them.
struct ShuffledChunkInfo final : public ChunkInfoCloneable<ShuffledChunkInfo>
{
    explicit ShuffledChunkInfo(UInt64 population_) : population(population_) {}
    ShuffledChunkInfo(const ShuffledChunkInfo &) = default;

    UInt64 population = 0;
};

/// Where to put the generated sort keys.
struct ShuffleKeyDescription
{
    /// Position of the key column in the output header.
    size_t position = 0;
    /// The key column exists in the input (its values are replaced, e.g. the `rand()` column), otherwise it is appended.
    bool replace = false;
    /// 1 for ascending order of keys, -1 for descending.
    int direction = 1;
};

/// Generates sorted random keys for a sequence of rows: either for the `index`-th of `count` equal ranges of keys
/// (the rows of a part of a full shuffle), or for the smallest of `population` random keys (a sample with `LIMIT`).
class ShuffleKeyGenerator
{
public:
    ShuffleKeyGenerator(const DataTypePtr & type, int direction_);

    /// `num_rows` independent uniformly random keys in [index / count, (index + 1) / count) of the range of keys, sorted.
    ColumnPtr generateInRange(pcg64 & rng, size_t num_rows, size_t index, size_t count) const;

    /// The next `num_rows` of the sorted keys of `population` independent uniformly random keys.
    ColumnPtr generateNextOrderStatistics(pcg64 & rng, size_t num_rows, UInt64 population);

private:
    [[nodiscard]] ColumnPtr makeColumn(const PaddedPODArray<UInt64> & keys) const;

    bool is_uint32 = false;
    int direction = 1;

    /// For order statistics: log(1 - the last generated key), initially log(1) = 0, and the number of generated keys.
    double remaining_fraction = 0.0;
    UInt64 num_generated = 0;
};

/// Shuffles all rows of a stream: scatters them into buckets by a random bucket number, and shuffles every bucket
/// by the Fisher-Yates algorithm in the CPU cache. The memory usage is about the size of the data.
/// If `key` is set, fills the key column of the output with sorted keys of the range `range_index` of `num_ranges`.
class ShuffleTransform final : public IAccumulatingTransform
{
public:
    ShuffleTransform(
        SharedHeader input_header,
        SharedHeader output_header,
        std::optional<ShuffleKeyDescription> key_,
        size_t max_block_size_,
        size_t range_index_,
        size_t num_ranges_);

    String getName() const override { return "ShuffleTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    void scatterIntoBuckets();
    bool prepareNextBucket();

    const std::optional<ShuffleKeyDescription> key;
    const size_t max_block_size;
    const size_t range_index;
    const size_t num_ranges;
    std::optional<ShuffleKeyGenerator> key_generator;

    pcg64 rng;

    /// The template of the accumulated columns (by the input positions). Constant columns and the replaced key column
    /// are not accumulated (they are nullptr).
    Columns empty_columns;
    size_t accumulated_rows = 0;
    VectorWithMemoryTracking<Columns> chunks;

    VectorWithMemoryTracking<MutableColumns> buckets;
    VectorWithMemoryTracking<size_t> bucket_rows;
    size_t num_buckets = 0;
    size_t next_bucket = 0;

    bool buckets_prepared = false;
    Columns part_columns;
    ColumnPtr part_key;
    size_t part_rows = 0;
    size_t part_offset = 0;
};

/// Reservoir sampling (Algorithm L by Kim-Hung Li): keeps a uniformly random sample of `limit` rows of a stream,
/// generating O(limit * log(n / limit)) random numbers for a stream of n rows - the rows which cannot get into the sample
/// are skipped without looking at them. Returns the sample in a uniformly random order, with `ShuffledChunkInfo`.
class ReservoirSampleTransform final : public IAccumulatingTransform
{
public:
    ReservoirSampleTransform(SharedHeader header, UInt64 limit_, size_t max_block_size_);

    String getName() const override { return "ReservoirSampleTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { read_rows.swap(counter); }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    double randomUnit();
    void advance();
    void compact();

    const UInt64 limit;
    const size_t max_block_size;

    RowsBeforeStepCounterPtr read_rows;

    pcg64 rng;

    /// The template of the stored columns (nullptr for constant columns).
    Columns empty_columns;
    /// The rows are appended; `slots` are the positions of the rows of the sample. Replaced rows are removed by compaction.
    MutableColumns data;
    size_t data_rows = 0;
    PaddedPODArray<UInt64> slots;

    /// The number of rows of the stream seen so far, the index of the next row to take into the sample, and the parameter
    /// of Algorithm L.
    UInt64 num_seen = 0;
    UInt64 next_index = 0;
    double w = 0;

    bool generated_prepared = false;
    size_t output_offset = 0;
};

/// Merges uniformly random samples of streams (in uniformly random order, with `ShuffledChunkInfo`) into a uniformly random
/// sample of the union of the streams of at most `limit` rows (all rows if `limit` is 0), in a uniformly random order.
/// For every output block, the number of rows to take from every stream is drawn as sampling without replacement
/// from the populations of the streams, these rows are concatenated and shuffled. Used with `LIMIT`.
class MergingShuffledTransform final : public IProcessor
{
public:
    MergingShuffledTransform(SharedHeader header, size_t num_inputs, UInt64 limit_, size_t max_block_size_);

    String getName() const override { return "MergingShuffledTransform"; }

    Status prepare() override;
    void work() override;

private:
    struct Input
    {
        DequeWithMemoryTracking<Chunk> chunks;
        size_t offset = 0;
        size_t buffered_rows = 0;
        std::optional<UInt64> population;
        UInt64 remaining_population = 0;
        size_t to_take = 0;
        bool finished = false;
    };

    void drawCounts();
    Chunk buildBlock();

    const UInt64 limit;
    const size_t max_block_size;

    pcg64 rng;

    VectorWithMemoryTracking<Input> inputs_state;
    bool populations_known = false;
    UInt64 total_population = 0;
    UInt64 remaining_output = 0;
    UInt64 remaining_population = 0;

    bool counts_drawn = false;
    Chunk output_chunk;
};

/// Concatenates the streams in the order of the inputs. Unlike `ConcatProcessor`, reads all inputs at the same time,
/// so that the scatter in front of the shuffle of the parts is not stuck.
class ShuffleConcatTransform final : public IProcessor
{
public:
    ShuffleConcatTransform(SharedHeader header, size_t num_inputs);

    String getName() const override { return "ShuffleConcatTransform"; }

    Status prepare() override;

private:
    VectorWithMemoryTracking<DequeWithMemoryTracking<Chunk>> queues;
    size_t current_input = 0;
};

/// Fills the key column with sorted keys for a sample (with `ShuffledChunkInfo`): the smallest of `population` random keys.
class FillShuffleKeyTransform final : public ISimpleTransform
{
public:
    FillShuffleKeyTransform(SharedHeader input_header, SharedHeader output_header, ShuffleKeyDescription key_);

    String getName() const override { return "FillShuffleKeyTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    const ShuffleKeyDescription key;
    ShuffleKeyGenerator key_generator;
    pcg64 rng;
};

/// Appends a column with zeros: the key column for the totals and extremes, which are not shuffled.
class AppendShuffleKeyTransform final : public ISimpleTransform
{
public:
    AppendShuffleKeyTransform(SharedHeader input_header, SharedHeader output_header);

    String getName() const override { return "AppendShuffleKeyTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
