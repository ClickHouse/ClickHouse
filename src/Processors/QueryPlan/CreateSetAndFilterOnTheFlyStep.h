#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/CreatingSetsOnTheFlyTransform.h>
#include <Processors/DelayedPortsProcessor.h>


namespace DB
{

class CreateSetAndFilterOnTheFlyStep : public ITransformingStep
{
public:
    /// Two instances of step need some shared state to connect processors crosswise
    class CrosswiseConnection;
    using CrosswiseConnectionPtr = std::shared_ptr<CrosswiseConnection>;
    static CrosswiseConnectionPtr createCrossConnection();

    CreateSetAndFilterOnTheFlyStep(
        const DataStream & input_stream_,
        const DataStream & rhs_input_stream_,
        const Names & column_names_,
        size_t max_rows_,
        CrosswiseConnectionPtr crosswise_connection_,
        JoinTableSide position_);

    String getName() const override { return "CreatingSetsOnTheFly"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    SetWithStatePtr getSet() const { return own_set; }
    void setFiltering(SetWithStatePtr filtering_set_) { filtering_set = filtering_set_; }

private:
    void updateOutputStream() override;

    Names column_names;

    size_t max_rows;

    Block rhs_input_stream_header;

    SetWithStatePtr own_set;
    SetWithStatePtr filtering_set;

    CrosswiseConnectionPtr crosswise_connection;

    JoinTableSide position;

    Poco::Logger * log = &Poco::Logger::get("CreateSetAndFilterOnTheFlyStep");
};

}
