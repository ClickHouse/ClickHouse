#include <Processors/Executors/SequentialPipelineExecutor.h>
#include <Processors/Executors/traverse.h>


namespace DB
{

//SequentialPipelineExecutor::SequentialPipelineExecutor(const Processors & processors)
//    : processors(processors)
//{
//}
//
//
//SequentialPipelineExecutor::Status SequentialPipelineExecutor::prepare()
//{
//    current_processor = nullptr;
//
//    bool has_someone_to_wait = false;
//    Status found_status = Status::Finished;
//
//    for (auto & element : processors)
//    {
//        traverse(*element,
//            [&] (IProcessor & processor)
//            {
//                Status status = processor.prepare();
//
//                if (status == Status::Wait)
//                    has_someone_to_wait = true;
//
//                if (status == Status::Ready || status == Status::Async)
//                {
//                    current_processor = &processor;
//                    found_status = status;
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
//        return found_status;
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
//void SequentialPipelineExecutor::work()
//{
//    if (!current_processor)
//        throw Exception("Bad pipeline", ErrorCodes::LOGICAL_ERROR);
//
//    current_processor->work();
//}
//
//
//void SequentialPipelineExecutor::schedule(EventCounter & watch)
//{
//    if (!current_processor)
//        throw Exception("Bad pipeline", ErrorCodes::LOGICAL_ERROR);
//
//    current_processor->schedule(watch);
//}

}

