#include <Processors/Executors/Runtime/V2/ExecutionOptions.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool log_processors_profiles;
    extern const SettingsBool opentelemetry_trace_processors;
}

namespace Runtime::V2
{

ExecutionOptions makeExecutionOptions(QueryStatusPtr process_list_element)
{
    ExecutionOptions options;
    if (process_list_element)
    {
        const auto & context = process_list_element->getContext();
        const auto & settings = context->getSettingsRef();
        options.profile_processors = settings[Setting::log_processors_profiles] && context->getProcessorsProfileLog();
        options.trace_processors = settings[Setting::opentelemetry_trace_processors];
        options.reservation = process_list_element->getMemoryReservation();
        options.tracker = process_list_element->getMemoryTracker();
    }
    options.process_list_element = std::move(process_list_element);
    return options;
}

}

}
