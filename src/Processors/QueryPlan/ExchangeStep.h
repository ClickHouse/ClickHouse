#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

class ExchangeStep final : public ISourceStep
{

public:
    ExchangeStep(const DataStream & data_stream);

    String getName() const override { return "Exchange"; }

    void initializePipeline(QueryPipelineBuilder & /*pipeline*/, const BuildQueryPipelineSettings & /*settings*/) override {}
};

}
