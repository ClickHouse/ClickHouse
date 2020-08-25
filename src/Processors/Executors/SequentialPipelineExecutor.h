#pragma once

#include <vector>

#include <Processors/IProcessor.h>


namespace DB
{

/** Wraps pipeline in a single processor.
  * This processor has no inputs and outputs and just executes the pipeline,
  *  performing all synchronous work from the current thread.
  */
//class SequentialPipelineExecutor : public IProcessor
//{
//private:
//    Processors processors;
//    IProcessor * current_processor = nullptr;
//
//public:
//    SequentialPipelineExecutor(const Processors & processors);
//
//    String getName() const override { return "SequentialPipelineExecutor"; }
//
//    Status prepare() override;
//    void work() override;
//    void schedule(EventCounter & watch) override;
//};

}
