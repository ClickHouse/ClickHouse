#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Unite several logical streams of data into single logical stream with specified structure.
class UnionStep : public IQueryPlanStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    explicit UnionStep(DataStreams input_streams_, size_t max_threads_ = 0);

    String getName() const override { return "Union"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    size_t getMaxThreads() const { return max_threads; }

    bool canUpdateInputStream() const override { return true; }

    // void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(WriteBuffer & out) const override;
    static std::unique_ptr<IQueryPlanStep> deserialize(ReadBuffer & in, const DataStreams & input_streams_, QueryPlanSerializationSettings &);

private:
    void updateOutputStream() override;

    Block header;
    size_t max_threads;
};

}
