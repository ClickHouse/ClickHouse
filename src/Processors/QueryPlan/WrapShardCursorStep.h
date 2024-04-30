#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

#include <Storages/buildQueryTreeForShard.h>

namespace DB
{

class WrapShardCursorStep : public ITransformingStep
{
public:
    WrapShardCursorStep(const DataStream & input_stream_, size_t shard_num_, ShardCursorChanges changes_);

    String getName() const override { return "WrapShardCursorStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

protected:
    void updateOutputStream() override;

private:
    size_t shard_num;
    ShardCursorChanges changes;
};

}
