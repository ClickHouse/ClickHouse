#include <DataStreams/ConcatBlockInputStream.h>


namespace DB
{

BlockInputStreams narrowBlockInputStreams(BlockInputStreams & inputs, size_t width)
{
    size_t size = inputs.size();
    if (size <= width)
        return inputs;

    std::vector<BlockInputStreams> partitions(width);

    using Distribution = std::vector<size_t>;
    Distribution distribution(size);

    for (size_t i = 0; i < size; ++i)
        distribution[i] = i % width;

    std::random_shuffle(distribution.begin(), distribution.end());

    for (size_t i = 0; i < size; ++i)
        partitions[distribution[i]].push_back(inputs[i]);

    BlockInputStreams res(width);
    for (size_t i = 0; i < width; ++i)
        res[i] = std::make_shared<ConcatBlockInputStream>(partitions[i]);

    return res;
}

}
