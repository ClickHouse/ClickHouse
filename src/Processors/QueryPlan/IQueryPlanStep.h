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

    /// QueryPipeline has single port. Totals or extremes ports are not counted.
    bool has_single_port = false;

    /// Sorting scope. Please keep the mutual order (more strong mode should have greater value).
    enum class SortScope
    {
        None   = 0,
        Chunk  = 1, /// Separate chunks are sorted
        Stream = 2, /// Each data steam is sorted
        Global = 3, /// Data is globally sorted
    };

    /// It is not guaranteed that header has columns from sort_description.
    SortDescription sort_description = {};
    SortScope sort_scope = SortScope::None;

    /// Things which may be added:
    /// * limit
    /// * estimated rows number
    /// * memory allocation context

    bool hasEqualPropertiesWith(const DataStream & other) const
    {
        return has_single_port == other.has_single_port
            && sort_description == other.sort_description
            && (sort_description.empty() || sort_scope == other.sort_scope);
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

    /// Append extra processors for this step.
    void appendExtraProcessors(const Processors & extra_processors);

protected:
    DataStreams input_streams;
    std::optional<DataStream> output_stream;

    /// Text description about what current step does.
    std::string step_description;

    /// This field is used to store added processors from this step.
    /// It is used only for introspection (EXPLAIN PIPELINE).
    Processors processors;

    static void describePipeline(const Processors & processors, FormatSettings & settings);
};

using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;
}
