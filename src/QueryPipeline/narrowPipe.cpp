#include <QueryPipeline/narrowPipe.h>

#include <random>
#include <Common/thread_local_rng.h>
#include <Processors/ConcatProcessor.h>
#include <QueryPipeline/Pipe.h>


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

void narrowPipe(Pipe & pipe, size_t width)
{
    size_t size = pipe.numOutputPorts();
    if (size <= width)
        return;

    std::vector<std::vector<OutputPort *>> partitions(width);

    auto distribution = getDistribution(size, width);

    pipe.transform([&](OutputPortRawPtrs ports)
    {
        for (size_t i = 0; i < size; ++i)
            partitions[distribution[i]].emplace_back(ports[i]);

        Processors concats;
        concats.reserve(width);

        for (size_t i = 0; i < width; ++i)
        {
           auto concat = std::make_shared<ConcatProcessor>(partitions[i].at(0)->getHeader(),
                                                           partitions[i].size());
           size_t next_port = 0;
           for (auto & port : concat->getInputs())
           {
               connect(*partitions[i][next_port], port);
               ++next_port;
           }

           concats.emplace_back(std::move(concat));
        }

        return concats;
    });
}

}
