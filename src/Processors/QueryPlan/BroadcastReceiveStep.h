#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

struct BroadcastReceiveWire;

/// Receive part of BroadcastExchangeStep
class BroadcastReceiveStep : public ISourceStep
{
public:
    BroadcastReceiveStep(SharedHeader header_, const String & exchange_id_, const Strings & source_shards_)
        : ISourceStep(std::move(header_))
        , exchange_id(exchange_id_)
        , source_shards(source_shards_)
    {
        /// TODO: is there a scenario where we broadcast partitioned source and thus have multiple source shards?
        chassert(source_shards.size() == 1);
    }

    String getName() const override { return "BroadcastReceive"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `BroadcastReceiveStep.cpp` declares.
    BroadcastReceiveWire toWire() const;
    static QueryPlanStepPtr fromWire(BroadcastReceiveWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    const String exchange_id;
    const Strings source_shards;
};

/// What `BroadcastReceiveStep` puts on the wire in the framed format.
struct BroadcastReceiveWire
{
    String exchange_id;
    Strings source_shards;

    bool operator==(const BroadcastReceiveWire &) const = default;
};

}
