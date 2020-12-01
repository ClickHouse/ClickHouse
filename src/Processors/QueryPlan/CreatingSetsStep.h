#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>
#include <Interpreters/SubqueryForSet.h>

namespace DB
{

/// Creates sets for subqueries and JOIN. See CreatingSetsTransform.
class CreatingSetsStep : public ITransformingStep
{
public:
    CreatingSetsStep(
            const DataStream & input_stream_,
            SubqueriesForSets subqueries_for_sets_,
            SizeLimits network_transfer_limits_,
            const Context & context_);

    String getName() const override { return "CreatingSets"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    SubqueriesForSets subqueries_for_sets;
    SizeLimits network_transfer_limits;
    const Context & context;
};

}
