#pragma once

#include <vector>
#include <set>
#include <mutex>
#include <Processors/IProcessor.h>

template <typename>
class ThreadPoolImpl;
class ThreadFromGlobalPool;
using ThreadPool = ThreadPoolImpl<ThreadFromGlobalPool>;

namespace DB
{

/** Wraps pipeline in a single processor.
  * This processor has no inputs and outputs and just executes the pipeline,
  *  performing all synchronous work within a threadpool.
  */
//class ParallelPipelineExecutor : public IProcessor
//{
//private:
//    Processors processors;
//    ThreadPool & pool;
//
//    std::set<IProcessor *> active_processors;
//    std::mutex mutex;
//
//    IProcessor * current_processor = nullptr;
//    Status current_status;
//
//public:
//    ParallelPipelineExecutor(const Processors & processors, ThreadPool & pool);
//
//    String getName() const override { return "ParallelPipelineExecutor"; }
//
//    Status prepare() override;
//    void schedule(EventCounter & watch) override;
//};

}
