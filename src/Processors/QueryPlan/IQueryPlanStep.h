#pragma once
#include <Core/Block.h>

namespace DB
{

class QueryPipeline;
using QueryPipelinePtr = std::unique_ptr<QueryPipeline>;
using QueryPipelines = std::vector<QueryPipelinePtr>;

/// Description of data stream.
class DataStream
{
public:
    Block header;

    /// Only header for now.
    /// Things which may be added:
    /// * sort description
    /// * distinct columns
    /// * limit
    /// * estimated rows number
    /// * memory allocation context
};

using DataStreams = std::vector<DataStream>;

/// Single step of query plan.
class IQueryPlanStep
{
public:
    virtual ~IQueryPlanStep() = default;

    virtual String getName() const = 0;

    /// Add processors from current step to QueryPipeline.
    /// Calling this method, we assume and don't check that:
    ///   * pipelines.size() == getInputStreams.size()
    ///   * header from each pipeline is the same as header from corresponding input_streams
    /// Result pipeline must contain any number of streams with compatible output header is hasOutputStream(),
    ///   or pipeline should be completed otherwise.
    virtual QueryPipelinePtr updatePipeline(QueryPipelines pipelines) = 0;

    const DataStreams & getInputStreams() const { return input_streams; }

    bool hasOutputStream() const { return output_stream.has_value(); }
    const DataStream & getOutputStream() const;

protected:
    DataStreams input_streams;
    std::optional<DataStream> output_stream;
};

}
