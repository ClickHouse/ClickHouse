#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>


namespace DB
{

/// Send part of GatherExchangeStep
class GatherSendStep final : public IQueryPlanStep
{
public:
    GatherSendStep(Header input_header_, const String & exchange_id_)
        : exchange_id(exchange_id_)
    {
        updateInputHeaders({std::move(input_header_)});
    }

    String getName() const override { return "GatherSend"; }

    bool hasOutputStream() const { return false; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override {}

    const String exchange_id;
};

}
