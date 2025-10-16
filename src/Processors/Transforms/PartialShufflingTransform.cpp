#include <Processors/Transforms/PartialShufflingTransform.h>
#include <Common/PODArray.h>
#include <Common/iota.h>
#include <random>
#include <algorithm>

#include <Common/logger_useful.h>

namespace DB
{

PartialShufflingTransform::PartialShufflingTransform(
    SharedHeader header_, UInt64 limit_)
    : ISimpleTransform(header_, header_, false)
    , limit(limit_)
{
    // LOG_TRACE(getLogger("PartialShufflingTransform"), "PartialShufflingTransform");
}

void PartialShufflingTransform::transform(Chunk & chunk)
{

    if (chunk.getNumRows())
    {
        // The following code works with Blocks and will lose the number of
        // rows when there are no columns. We shouldn't get such block, because
        // we have to shuffle by at least one column.
        assert(chunk.getNumColumns());
    }

    // LOG_TRACE(getLogger("PartialShufflingTransform"), "transform, rows: {}", chunk.getNumRows());


    // if (read_rows)
    //     read_rows->add(chunk.getNumRows());

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    shuffleBlock(block);

    // LOG_TRACE(getLogger("PartialShufflingTransform"), "transform, rows after shuffle: {}", block.rows());

    chunk.setColumns(block.getColumns(), block.rows());
}

IColumn::Permutation PartialShufflingTransform::getIdentityPermutation(size_t size) {
    IColumnPermutation identity_permutation(size);
    iota(identity_permutation.begin(), size, 0UL);
    return identity_permutation;
}

void PartialShufflingTransform::shufflePermutation(IColumn::Permutation & permutation)
{
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(permutation.begin(), permutation.end(), g);
}

void PartialShufflingTransform::shuffleBlock(Block & block)
{
    auto size = block.rows();
    // LOG_TRACE(getLogger("PartialShufflingTransform"), "shuffleBlock, rows: {}", size);


    IColumn::Permutation permutation = getIdentityPermutation(size);

    shufflePermutation(permutation);

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & column_to_shuffle = block.getByPosition(i).column;
        column_to_shuffle = column_to_shuffle->permute(permutation, limit);
    }
}
}
