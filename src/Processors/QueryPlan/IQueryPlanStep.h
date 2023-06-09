#pragma once
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>

namespace DB
{

class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;
using QueryPipelineBuilders = std::vector<QueryPipelineBuilderPtr>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

namespace JSONBuilder { class JSONMap; }

/// Description of data stream.
/// Single logical data stream may relate to many ports of pipeline.
class DataStream
{
public:
    Block header;

    /// Tuples with those columns are distinct.
    /// It doesn't mean that columns are distinct separately.
    /// Removing any column from this list breaks this invariant.
    NameSet distinct_columns = {};

    /// QueryPipeline has single port. Totals or extremes ports are not counted.
    bool has_single_port = false;

    /// How data is sorted.
    enum class SortMode
    {
        None,
        Chunk, /// Separate chunks are sorted
        Port, /// Data from each port is sorted
        Stream, /// Data is globally sorted
    };

    /// It is not guaranteed that header has columns from sort_description.
    SortDescription sort_description = {};
    SortMode sort_mode = SortMode::None;

    /// Things which may be added:
    /// * limit
    /// * estimated rows number
    /// * memory allocation context

    bool hasEqualPropertiesWith(const DataStream & other) const
    {
        return distinct_columns == other.distinct_columns
            && has_single_port == other.has_single_port
            && sort_description == other.sort_description
            && (sort_description.empty() || sort_mode == other.sort_mode);
    }

    bool hasEqualHeaderWith(const DataStream & other) const
    {
        return blocksHaveEqualStructure(header, other.header);
    }
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
    virtual QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) = 0;

    const DataStreams & getInputStreams() const { return input_streams; }

    bool hasOutputStream() const { return output_stream.has_value(); }
    const DataStream & getOutputStream() const;

    /// Methods to describe what this step is needed for.
    const std::string & getStepDescription() const { return step_description; }
    void setStepDescription(std::string description) { step_description = std::move(description); }

    struct FormatSettings
    {
        WriteBuffer & out;
        size_t offset = 0;
        const size_t indent = 2;
        const char indent_char = ' ';
        const bool write_header = false;
    };

    /// Get detailed description of step actions. This is shown in EXPLAIN query with options `actions = 1`.
    virtual void describeActions(JSONBuilder::JSONMap & /*map*/) const {}
    virtual void describeActions(FormatSettings & /*settings*/) const {}

    /// Get detailed description of read-from-storage step indexes (if any). Shown in with options `indexes = 1`.
    virtual void describeIndexes(JSONBuilder::JSONMap & /*map*/) const {}
    virtual void describeIndexes(FormatSettings & /*settings*/) const {}

    /// Get description of processors added in current step. Should be called after updatePipeline().
    virtual void describePipeline(FormatSettings & /*settings*/) const {}

protected:
    DataStreams input_streams;
    std::optional<DataStream> output_stream;

    /// Text description about what current step does.
    std::string step_description;

    static void describePipeline(const Processors & processors, FormatSettings & settings);
};

using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;
}
