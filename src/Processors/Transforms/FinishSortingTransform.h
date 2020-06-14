#pragma once
#include <Processors/Transforms/SortingTransform.h>

namespace DB
{

/** Takes stream already sorted by `x` and finishes sorting it by (`x`, `y`).
 *  During sorting only chunks with rows that equal by `x` saved in RAM.
 * */
class FinishSortingTransform : public SortingTransform
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    FinishSortingTransform(const Block & header, const SortDescription & description_sorted_,
        const SortDescription & description_to_sort_,
        size_t max_merged_block_size_, UInt64 limit_);

    String getName() const override { return "FinishSortingTransform"; }

protected:
    void consume(Chunk chunk) override;
    void generate() override;

private:
    SortDescription description_sorted;

    Chunk tail_chunk;
};

}
