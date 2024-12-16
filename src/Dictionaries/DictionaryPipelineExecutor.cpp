#include <Dictionaries/DictionaryPipelineExecutor.h>

#include <Core/Block.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

DictionaryPipelineExecutor::DictionaryPipelineExecutor(QueryPipeline & pipeline_, bool async)
    : async_executor(async ? std::make_unique<PullingAsyncPipelineExecutor>(pipeline_) : nullptr)
    , executor(async ? nullptr : std::make_unique<PullingPipelineExecutor>(pipeline_))
{
}

bool DictionaryPipelineExecutor::pull(Block & block)
{
    if (async_executor)
    {
        while (true)
        {
            bool has_data = async_executor->pull(block);
            if (has_data && !block)
                continue;
            return has_data;
        }
    }
    else if (executor)
        return executor->pull(block);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DictionaryPipelineExecutor is not initialized");
}

DictionaryPipelineExecutor::~DictionaryPipelineExecutor() = default;

}
