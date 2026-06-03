#pragma once

#include <DataTypes/IDataType.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>


namespace DB
{

/// Send part of ShuffleExchangeStep
class ShuffleSendStep final : public IQueryPlanStep
{
public:
    /// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to
    /// before hashing, used to align buckets across both sides of a shuffle join.
    ShuffleSendStep(SharedHeader input_header_, const String & exchange_id_, Names key_names_, size_t num_buckets_, DataTypes hash_cast_types_ = {})
        : exchange_id(exchange_id_)
        , key_names(std::move(key_names_))
        , hash_cast_types(std::move(hash_cast_types_))
        , num_buckets(num_buckets_)
    {
        chassert(num_buckets > 0);
        chassert(hash_cast_types.empty() || hash_cast_types.size() == key_names.size());
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
    const DataTypes hash_cast_types;
    const size_t num_buckets;
};

}
