#include <Processors/Executors/Runtime/createExecutor.h>
#include <Processors/Executors/Runtime/v1/PipelineExecutor.h>

namespace DB
{

ExecutorPtr createExecutor(std::shared_ptr<Processors> processors, QueryStatusPtr process_list_element)
{
    return std::make_shared<PipelineExecutor>(std::move(processors), std::move(process_list_element));
}

}
