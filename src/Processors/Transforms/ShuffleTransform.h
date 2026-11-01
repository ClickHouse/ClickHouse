#pragma once

#include <Columns/IColumn.h>
#include <Processors/IAccumulatingTransform.h>
#include <Common/PODArray.h>

#include <pcg_random.hpp>


namespace DB
{

/** Shuffles the rows of a single stream into a uniformly random order in O(n) time
  * (unlike sorting by random numbers, which is O(n * log(n))).
  *
  * The output is sorted by a key column. The values of this column are independent uniformly distributed
  * random numbers assigned to the rows in sorted order, while the rows themselves are put in a uniformly random
  * order. This has exactly the same distribution as generating a random key for every row and sorting the rows by it.
  *
  * Because of that:
  * - streams shuffled this way (including the streams from remote servers) are merged by `MergingSortedTransform`
  *   on the key column with O(1) memory, and the result is a uniformly random permutation of all rows;
  * - `ORDER BY rand()` is executed by this transform, with the `rand()` column used as the key column
  *   (its values are replaced by the new keys, which have the same distribution).
  *
  * The key column must be of type `UInt64` or `UInt32` (as for `rand64` and `rand`), and its input values are ignored.
  *
  * Without a limit, the rows are scattered into buckets by a random bucket number (the high part of the key),
  * then every bucket is shuffled by the Fisher-Yates algorithm and gets random keys (the low part of the key) in sorted
  * order. The buckets are small enough to be shuffled in the CPU cache, and the memory usage is about the size of the data.
  *
  * To shuffle several streams in parallel without merging them, the rows are first scattered into `num_ranges` streams
  * at random, and the stream number `range_index` gets the keys from the `range_index`-th of `num_ranges` equal ranges.
  * Then the concatenation of the streams in the order of `range_index` is sorted by the key.
  *
  * If `limit` is not zero, only the rows with the `limit` smallest keys are kept, using O(limit) memory
  * (reservoir sampling). Merging such streams with the same limit returns a uniformly random sample of `limit` rows
  * in a uniformly random order.
  */
class PartialShuffleTransform final : public IAccumulatingTransform
{
public:
    /// `direction` is 1 if the output should be sorted by the key column ascending, -1 - descending.
    PartialShuffleTransform(
        SharedHeader header,
        size_t key_position_,
        int direction_,
        UInt64 limit_,
        size_t max_block_size_,
        size_t range_index_ = 0,
        size_t num_ranges_ = 1);

    String getName() const override { return "PartialShuffleTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    using Keys = PaddedPODArray<UInt64>;

    void consumeWithLimit(const Columns & columns, size_t num_rows);
    void pruneReservoir();
    UInt64 sampleRowsToSkip();

    void prepareResult();
    void prepareResultWithLimit();
    void scatterIntoBuckets();
    bool prepareNextBucket();

    ColumnPtr makeKeyColumn(size_t offset, size_t length) const;

    const size_t key_position;
    const int direction;
    const UInt64 limit;
    const size_t max_block_size;
    const size_t range_index;
    const size_t num_ranges;
    const bool is_key_uint32;

    pcg64 rng;

    /// The template of the accumulated columns. The key column and constant columns are not accumulated (they are nullptr).
    Columns empty_columns;
    size_t accumulated_rows = 0;

    /// Without limit: the input chunks, which are scattered into buckets at the end.
    std::vector<Columns> chunks;
    std::vector<MutableColumns> buckets;
    std::vector<size_t> bucket_rows;
    size_t next_bucket = 0;

    /// With limit: the accumulated rows, the internal random key of every row, and the threshold:
    /// a row with a key not less than the threshold cannot be among the rows with the `limit` smallest keys.
    MutableColumns accumulated_columns;
    Keys reservoir_keys;
    UInt64 threshold;
    /// The number of the next rows that are skipped because their keys are not less than the threshold.
    UInt64 rows_to_skip = 0;

    /// The rows which are being returned: shuffled columns and their sorted internal keys.
    bool result_prepared = false;
    Columns part_columns;
    Keys part_keys;
    size_t part_offset = 0;
};

}
