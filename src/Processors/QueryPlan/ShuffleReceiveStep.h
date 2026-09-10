#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

struct ShuffleReceiveWire;

/// Reads data corresponding to one shuffle bucket.
/// The data itself might have multiple shards (files) and we read them all.
class ShuffleReceiveStep : public ISourceStep
{
public:
    ShuffleReceiveStep(SharedHeader header_, const String & exchange_id_, const Strings & source_shards_)
        : ISourceStep(std::move(header_))
        , exchange_id(exchange_id_)
        , source_shards(source_shards_)
    {
    }

    String getName() const override { return "ShuffleReceive"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `ShuffleReceiveStep.cpp` declares.
    ShuffleReceiveWire toWire() const;
    static QueryPlanStepPtr fromWire(ShuffleReceiveWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    const String exchange_id;
    const Strings source_shards;
};

/// What `ShuffleReceiveStep` puts on the wire in the framed format.
struct ShuffleReceiveWire
{
    String exchange_id;
    Strings source_shards;

    bool operator==(const ShuffleReceiveWire &) const = default;
};

}
