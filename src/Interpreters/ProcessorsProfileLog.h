#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Processors/IProcessor.h>

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

    String query_id;
    String processor_name;

    /// Milliseconds spend in IProcessor::work()
    UInt32 elapsed_us{};
    /// IProcessor::NeedData
    UInt32 input_wait_elapsed_us{};
    /// IProcessor::PortFull
    UInt32 output_wait_elapsed_us{};

    size_t input_rows{};
    size_t input_bytes{};
    size_t output_rows{};
    size_t output_bytes{};

    static std::string name() { return "ProcessorsProfileLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class ProcessorsProfileLog : public SystemLog<ProcessorProfileLogElement>
{
public:
    ProcessorsProfileLog(
        ContextPtr context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);
};

}
