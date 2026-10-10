#include <Processors/Executors/Runtime/createExecutor.h>
#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>
#include <Processors/Executors/Runtime/V2/PipelineExecutor.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

namespace DB
{

namespace Setting
{
    extern const SettingsPipelineExecutorKind pipeline_executor;
}

namespace
{

PipelineExecutorKind executorKind(const QueryStatusPtr & process_list_element)
{
    if (!process_list_element)
        return PipelineExecutorKind::V1;

    return process_list_element->getContext()->getSettingsRef()[Setting::pipeline_executor].value;
}

}

ExecutorPtr createExecutor(std::shared_ptr<Processors> processors, QueryStatusPtr process_list_element)
{
    switch (executorKind(process_list_element))
    {
        case PipelineExecutorKind::V1:
            return std::make_shared<Runtime::V1::PipelineExecutor>(std::move(processors), std::move(process_list_element));
        case PipelineExecutorKind::V2:
            return std::make_shared<Runtime::V2::PipelineExecutor>(std::move(processors), std::move(process_list_element));
    }
}

}
