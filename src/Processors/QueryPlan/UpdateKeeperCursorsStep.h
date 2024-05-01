#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class UpdateKeeperCursorsStep : public ITransformingStep
{
public:
    UpdateKeeperCursorsStep(const DataStream & input_stream_, zkutil::ZooKeeperPtr zk_);

    String getName() const override { return "UpdateKeeperCursorsStep"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

protected:
    void updateOutputStream() override;

private:
    zkutil::ZooKeeperPtr zk;
};

}
