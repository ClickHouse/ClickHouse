#include <Common/EventCounter.h>
#include <Common/ThreadPool.h>
#include <Processors/Executors/ParallelPipelineExecutor.h>
#include <Processors/Executors/traverse.h>


namespace DB
{
//
//ParallelPipelineExecutor::ParallelPipelineExecutor(const std::vector<ProcessorPtr> & processors, ThreadPool & pool)
//    : processors(processors), pool(pool)
//{
//}
//
//
//ParallelPipelineExecutor::Status ParallelPipelineExecutor::prepare()
//{
//    current_processor = nullptr;
//
//    bool has_someone_to_wait = false;
//
//    for (auto & element : processors)
//    {
//        traverse(*element,
//            [&] (IProcessor & processor)
//            {
//                {
//                    std::lock_guard lock(mutex);
//                    if (active_processors.count(&processor))
//                    {
//                        has_someone_to_wait = true;
//                        return Status::Wait;
//                    }
//                }
//
//                Status status = processor.prepare();
//
//                if (status == Status::Wait)
//                    has_someone_to_wait = true;
//
//                if (status == Status::Ready || status == Status::Async)
//                {
//                    current_processor = &processor;
//                    current_status = status;
//                }
//
//                return status;
//            });
//
//        if (current_processor)
//            break;
//    }
//
//    if (current_processor)
//        return Status::Async;
//
//    if (has_someone_to_wait)
//        return Status::Wait;
//
//    for (auto & element : processors)
//    {
//        if (element->prepare() == Status::NeedData)
//            throw Exception("Pipeline stuck: " + element->getName() + " processor needs input data but no one is going to generate it", ErrorCodes::LOGICAL_ERROR);
//        if (element->prepare() == Status::PortFull)
//            throw Exception("Pipeline stuck: " + element->getName() + " processor has data in output port but no one is going to consume it", ErrorCodes::LOGICAL_ERROR);
//    }
//
//    return Status::Finished;
//}
//
//
//void ParallelPipelineExecutor::schedule(EventCounter & watch)
//{
//    if (!current_processor)
//        throw Exception("Bad pipeline", ErrorCodes::LOGICAL_ERROR);
//
//    if (current_status == Status::Async)
//    {
//        current_processor->schedule(watch);
//    }
//    else
//    {
//        {
//            std::lock_guard lock(mutex);
//            active_processors.insert(current_processor);
//        }
//
//        pool.schedule([processor = current_processor, &watch, this]
//        {
//            processor->work();
//            {
//                std::lock_guard lock(mutex);
//                active_processors.erase(processor);
//            }
//            watch.notify();
//        });
//    }
//}

}

