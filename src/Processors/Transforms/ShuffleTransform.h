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
  * order by the Fisher-Yates shuffle. This has exactly the same distribution as generating a random key for every
  * row and sorting the rows by it.
  *
  * Because of that:
  * - streams shuffled this way (including the streams from remote servers) are merged by `MergingSortedTransform`
  *   on the key column with O(1) memory, and the result is a uniformly random permutation of all rows;
  * - `ORDER BY rand()` is executed by this transform, with the `rand()` column used as the key column
  *   (its values are replaced by the new keys, which have the same distribution).
  *
  * The key column must be of type `UInt64` or `UInt32` (as for `rand64` and `rand`), and its input values are ignored.
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
        size_t max_block_size_);

    String getName() const override { return "PartialShuffleTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    using Keys = PaddedPODArray<UInt64>;

    void consumeWithLimit(const Columns & columns, size_t num_rows);
    void pruneReservoir();
    void prepareResult();
    ColumnPtr makeKeyColumn(size_t offset, size_t length) const;

    const size_t key_position;
    const int direction;
    const UInt64 limit;
    const size_t max_block_size;
    const bool is_key_uint32;

    pcg64 rng;

    /// The accumulated rows. The key column and constant columns are not accumulated (they are nullptr).
    MutableColumns accumulated_columns;
    size_t accumulated_rows = 0;

    /// Only with limit: the internal random key of every accumulated row, and the threshold:
    /// a row with a key not less than the threshold cannot be among the rows with the `limit` smallest keys.
    Keys reservoir_keys;
    UInt64 threshold;

    /// The prepared result: the accumulated columns, the order of rows to output, and the sorted internal keys.
    bool result_prepared = false;
    Columns result_columns;
    IColumn::Permutation result_permutation;
    Keys result_keys;
    size_t output_offset = 0;
};

}
