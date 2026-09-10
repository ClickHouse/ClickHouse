#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/StepWireCodecs.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/Aggregator.h>

namespace DB
{

struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

struct CubeWire;

/// WITH CUBE. See CubeTransform.
class CubeStep : public ITransformingStep
{
public:
    CubeStep(const SharedHeader & input_header_, Aggregator::Params params_, bool final_, bool use_nulls_);

    String getName() const override { return "Cube"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    const Aggregator::Params & getParams() const;

    QueryPlanStepPtr clone() const override;

    void serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const override;
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `CubeStep.cpp` declares.
    CubeWire toWire() const;
    static QueryPlanStepPtr fromWire(CubeWire wire, Deserialization & ctx);
    bool isSerializable() const override { return true; }
private:
    /// Streams below the framed format.
    void serializeSettingsLegacy(QueryPlanSerializationSettings & settings) const;
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override;

    size_t keys_size;
    Aggregator::Params params;
    bool final;
    bool use_nulls;
};

/// What `CubeStep` puts on the wire in the framed format. The members from `max_block_size` on travel
/// through the settings channel.
struct CubeWire
{
    Names keys;
    AggregateDescriptionsWithoutArguments aggregates;
    bool final = false;
    bool overflow_row = false;
    bool use_nulls = false;

    UInt64 max_block_size = DEFAULT_BLOCK_SIZE;
    Float32 min_hit_rate_to_use_consecutive_keys_optimization = 0.5;
    bool serialize_string_in_memory_with_zero_byte = true;
    bool enable_packed_string_keys_in_aggregation = true;
};

}
