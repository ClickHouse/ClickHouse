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

    String initial_query_id;
    String query_id;
    String processor_name;

    /// Milliseconds spend in IProcessor::work()
    UInt32 elapsed_us{};
    /// IProcessor::NeedData
    UInt32 input_wait_elapsed_us{};
    /// IProcessor::PortFull
    UInt32 output_wait_elapsed_us{};

    size_t input_rows{};
    size_t input_blocks{};
    size_t input_bytes{};
    size_t input_allocated_bytes{};
    size_t output_rows{};
    size_t output_blocks{};
    size_t output_bytes{};
    size_t output_allocated_bytes{};

    size_t alloc_bytes{};
    size_t alloc_calls{};
    size_t free_bytes{};
    size_t free_calls{};
    Int64 peak_memory_usage{};

    static std::string name() { return "ProcessorsProfileLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class ProcessorsProfileLog : public SystemLog<ProcessorProfileLogElement>
{
public:
    using SystemLog<ProcessorProfileLogElement>::SystemLog;
};

}
