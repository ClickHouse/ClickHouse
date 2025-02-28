#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>


namespace DB
{

class ShuffleSendStep final : public IQueryPlanStep
{
public:
    ShuffleSendStep(Header input_header_, const String & exchange_id_, Names key_names_, size_t num_buckets_)
        : exchange_id(exchange_id_)
        , key_names(std::move(key_names_))
        , num_buckets(num_buckets_)
    {
        chassert(num_buckets > 0);
        updateInputHeaders({std::move(input_header_)});
    }


    String getName() const override { return "ShuffleSend"; }

    bool hasOutputStream() const { return false; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

private:
    void updateOutputHeader() override {}

    const String exchange_id;
    const Names key_names;
    const size_t num_buckets;
};

}
