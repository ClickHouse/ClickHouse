#pragma once

#include <Processors/IProcessor_fwd.h>

#include <memory>
#include <mutex>

namespace DB
{

class ExecutionGraph
{
public:
    explicit ExecutionGraph(std::shared_ptr<Processors> processors_);

    void addProcessor(ProcessorPtr processor);
    void removeProcessor(const ProcessorPtr & processor);

    template <class F>
    void forEachProcessor(F && f)
    {
        std::lock_guard lock(mutex);
        for (const auto & processor : *processors)
            f(*processor);
    }

    const Processors & getProcessors() const;

private:
    std::shared_ptr<Processors> processors;
    std::mutex mutex;
};

}
