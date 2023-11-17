#include <string>

#include <Interpreters/PipelineTrace.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

namespace DB 
{

using PipelineLogStageType = PipelineLogElement::PipelineLogStageType;

const PipelineLogStageType::Values PipelineLogElement::pipeline_stage_values = 
{
    {"Prepare", static_cast<UInt8>(PipelineStageType::Prepare)},
    {"Execute", static_cast<UInt8>(PipelineStageType::Execute)},
};

NamesAndTypesList PipelineLogElement::getNamesAndTypes()
{
    return
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},

        {"query_id", std::make_shared<DataTypeString>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"processor_name", std::make_shared<DataTypeString>()},
        {"processor_id", std::make_shared<DataTypeUInt64>()},
        {"stage_type", std::make_shared<PipelineLogStageType>(pipeline_stage_values)},
        {"start_ns", std::make_shared<DataTypeUInt64>()},
        {"end_ns", std::make_shared<DataTypeUInt64>()},
    };
}

void PipelineLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(thread_id);
    columns[i++]->insertData(processor_name.data(), processor_name.size());
    columns[i++]->insert(processor_id);
    columns[i++]->insert(static_cast<UInt8>(stage_type));
    columns[i++]->insert(start_ns);
    columns[i++]->insert(end_ns);
}

static const unsigned int UUID_LEN = 36;

void PipelineLog::record(UInt64 start_ns, UInt64 end_ns, PipelineStageType stage, ExecutingGraph::Node* node)
{
    if (!CurrentThread::isInitialized()) 
    {
        return;
    }

    ContextPtr query_context = CurrentThread::get().getQueryContext() ;
    
    if (!query_context) 
    {
        return;
    }

    const Settings & settings = query_context->getSettingsRef();
    auto trace_pipeline = settings.trace_pipeline;
    if (!trace_pipeline) 
    {
        return;
    }

    auto log = query_context->getPipelineTraceLog();
    if (!log) 
    {
        return;
    }
    
    UInt64 time = now_ns();
    UInt64 time_in_microseconds = time / 1000;

    std::string_view query_id = CurrentThread::getQueryId();
    query_id.remove_suffix(query_id.size() - UUID_LEN);

    UInt64 thread_id = CurrentThread::get().thread_id;

    PipelineLogElement element {
        time_t(time / 1000000000),
        time_in_microseconds, 
        std::string(query_id),
        thread_id,
        node->processor->getName(),
        node->processors_id,
        stage,
        start_ns,
        end_ns
    };


    log->add(std::move(element));
}

UInt64 PipelineLog::now_ns()
{    
    if (!CurrentThread::isInitialized()) 
    {
        return 0;
    }

    ContextPtr query_context = CurrentThread::get().getQueryContext() ;
    
    if (!query_context) 
    {
        return 0;
    }

    const Settings & settings = query_context->getSettingsRef();
    auto trace_pipeline = settings.trace_pipeline;
    if (!trace_pipeline) 
    {
        return 0;
    }

    std::chrono::time_point<std::chrono::system_clock> point = std::chrono::system_clock::now();

    return timeInNanoseconds(point);
} 

}
