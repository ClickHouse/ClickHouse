#include <Interpreters/PipelineTrace.h>
#include <DataTypes/DataTypeString.h>

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
    
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(thread_id);
    columns[i++]->insertData(processor_name.data(), processor_name.size());
    columns[i++]->insert(processor_id);
    columns[i++]->insert(static_cast<UInt8>(stage_type));
    columns[i++]->insert(start_ns);
    columns[i++]->insert(end_ns);
}

}
