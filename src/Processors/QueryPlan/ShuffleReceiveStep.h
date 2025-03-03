#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

/// Reads data corresponding to one shuffle bucket.
/// The data itself might have multiple shards (files) and we read them all.
class ShuffleReceiveStep : public ISourceStep
{
public:
    ShuffleReceiveStep(Header header_, const String & exchange_id_, const Strings & source_shards_)
        : ISourceStep(std::move(header_))
        , exchange_id(exchange_id_)
        , source_shards(source_shards_)
    {
    }

    String getName() const override { return "ShuffleReceive"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    const String exchange_id;
    const Strings source_shards;
};

}
