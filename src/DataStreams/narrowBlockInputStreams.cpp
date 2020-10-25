#include <random>
#include <Common/thread_local_rng.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/Pipe.h>
#include "narrowBlockInputStreams.h"


namespace DB
{

namespace
{
    using Distribution = std::vector<size_t>;
    Distribution getDistribution(size_t from, size_t to)
    {
        Distribution distribution(from);

        for (size_t i = 0; i < from; ++i)
            distribution[i] = i % to;

        std::shuffle(distribution.begin(), distribution.end(), thread_local_rng);
        return distribution;
    }
}

Pipes narrowPipes(Pipes pipes, size_t width)
{
    size_t size = pipes.size();
    if (size <= width)
        return pipes;

    std::vector<Pipes> partitions(width);

    auto distribution = getDistribution(size, width);

    for (size_t i = 0; i < size; ++i)
        partitions[distribution[i]].emplace_back(std::move(pipes[i]));

    Pipes res;
    res.reserve(width);

    for (size_t i = 0; i < width; ++i)
    {
        auto processor = std::make_shared<ConcatProcessor>(partitions[i].at(0).getHeader(), partitions[i].size());
        res.emplace_back(std::move(partitions[i]), std::move(processor));
    }

    return res;
}

}
