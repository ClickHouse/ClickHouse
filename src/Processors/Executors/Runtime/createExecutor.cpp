#include <Processors/Executors/Runtime/createExecutor.h>
#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>

namespace DB
{

ExecutorPtr createExecutor(std::shared_ptr<Processors> processors, QueryStatusPtr process_list_element)
{
    return std::make_shared<Runtime::V1::PipelineExecutor>(std::move(processors), std::move(process_list_element));
}

}
