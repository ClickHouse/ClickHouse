#pragma once

#include <list>
#include <set>
#include <mutex>
#include <Processors/IProcessor.h>


class ThreadPool;

namespace DB
{

/** Wraps pipeline in a single processor.
  * This processor has no inputs and outputs and just executes the pipeline,
  *  performing all synchronous work within a threadpool.
  */
class ParallelPipelineExecutor : public IProcessor
{
private:
    std::list<ProcessorPtr> processors;
    ThreadPool & pool;

    std::set<IProcessor *> active_processors;
    std::mutex mutex;

    IProcessor * current_processor = nullptr;
    Status current_status;

public:
    ParallelPipelineExecutor(const std::list<ProcessorPtr> & processors, ThreadPool & pool);

    String getName() const override { return "ParallelPipelineExecutor"; }

    Status prepare() override;
    void schedule(EventCounter & watch) override;
};

}
