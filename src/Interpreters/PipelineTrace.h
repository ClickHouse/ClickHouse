#pragma once

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Processors/Executors/ExecutingGraph.h>


namespace DB
{

enum class PipelineStageType : uint8_t
{
    Prepare,
    Execute,
};

/**
 * Pipeline log element definition
*/
struct PipelineLogElement
{
    using PipelineLogStageType = DataTypeEnum8;
    static const PipelineLogStageType::Values pipeline_stage_values;

    time_t event_time{};
    Decimal64 event_time_microseconds{};

    String query_id{};
    UInt64 thread_id{};
    String processor_name{};
    UInt64 processor_id{};
    PipelineStageType stage_type{};
    UInt64 start_ns{};
    UInt64 end_ns{};


    static std::string name() { return "PipelineLog"; }
    static NamesAndTypesList getNamesAndTypes() ;
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};


class PipelineLog : public SystemLog<PipelineLogElement>
{
    using SystemLog<PipelineLogElement>::SystemLog;

public:
    static void record(UInt64 start_ns, UInt64 end_ns, PipelineStageType stage, ExecutingGraph::Node* node);
    static UInt64 now_ns();
};

}
