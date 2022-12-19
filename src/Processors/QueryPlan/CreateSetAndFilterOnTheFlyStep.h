#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/CreateSetAndFilterOnTheFlyTransform.h>
#include <Processors/DelayedPortsProcessor.h>


namespace DB
{

/*
 * Used to optimize JOIN when joining a small table over a large table.
 * Currently applied only for the full sorting join.
 * It tries to build a set for each stream.
 * Once one stream is finished, it starts to filter another stream with this set.
 */
class CreateSetAndFilterOnTheFlyStep : public ITransformingStep
{
public:
    /// Two instances of step need some shared state to connect processors crosswise
    class CrosswiseConnection;
    using CrosswiseConnectionPtr = std::shared_ptr<CrosswiseConnection>;
    static CrosswiseConnectionPtr createCrossConnection();

    CreateSetAndFilterOnTheFlyStep(
        const DataStream & input_stream_,
        const Names & column_names_,
        size_t max_rows_in_set_,
        CrosswiseConnectionPtr crosswise_connection_,
        JoinTableSide position_);

    String getName() const override { return "CreateSetAndFilterOnTheFlyStep"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    SetWithStatePtr getSet() const { return own_set; }

    /// Set for another stream.
    void setFiltering(SetWithStatePtr filtering_set_) { filtering_set = filtering_set_; }

private:
    void updateOutputStream() override;

    Names column_names;

    size_t max_rows_in_set;

    SetWithStatePtr own_set;
    SetWithStatePtr filtering_set;

    CrosswiseConnectionPtr crosswise_connection;

    JoinTableSide position;

    Poco::Logger * log = &Poco::Logger::get("CreateSetAndFilterOnTheFlyStep");
};

}
