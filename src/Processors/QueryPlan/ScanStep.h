#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ScanStep final : public ISourceStep
{

public:
    ScanStep(const DataStream & data_stream, StoragePtr main_table);

    String getName() const override { return "Scan"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override {}

    StoragePtr getTable() const { return main_table; }

private:
    StoragePtr main_table;

};

}
