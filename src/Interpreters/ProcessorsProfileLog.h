#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Processors/IProcessor.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct ProcessorProfileLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};

    UInt64 id{};
    std::vector<UInt64> parent_ids;

    UInt64 plan_step{};
    UInt64 plan_group{};
    String plan_step_name;
    String plan_step_description;

    String initial_query_id;
    String query_id;
    String processor_name;

    /// Milliseconds spend in IProcessor::work()
    UInt64 elapsed_us{};
    /// IProcessor::NeedData
    UInt64 input_wait_elapsed_us{};
    /// IProcessor::PortFull
    UInt64 output_wait_elapsed_us{};

    size_t input_rows{};
    size_t input_bytes{};
    size_t output_rows{};
    size_t output_bytes{};

    static std::string name() { return "ProcessorsProfileLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class ProcessorsProfileLog : public SystemLog<ProcessorProfileLogElement>
{
public:
    using SystemLog<ProcessorProfileLogElement>::SystemLog;
};

}
