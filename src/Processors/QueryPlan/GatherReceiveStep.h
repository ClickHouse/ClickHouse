#pragma once

#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

struct GatherReceiveWire;

/// Receive part of GatherExchangeStep
class GatherReceiveStep : public ISourceStep
{
public:
    GatherReceiveStep(SharedHeader header_, const String & exchange_id_, size_t num_buckets_,
                      std::optional<SortDescription> maintain_sort_description_ = std::nullopt)
        : ISourceStep(std::move(header_))
        , exchange_id(exchange_id_)
        , num_buckets(num_buckets_)
        , maintain_sort_description(std::move(maintain_sort_description_))
    {
    }

    String getName() const override { return "GatherReceive"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `GatherReceiveStep.cpp` declares.
    GatherReceiveWire toWire() const;
    static QueryPlanStepPtr fromWire(GatherReceiveWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    const String exchange_id;
    const size_t num_buckets;
    const std::optional<SortDescription> maintain_sort_description;
};


/// What `GatherReceiveStep` puts on the wire in the framed format.
struct GatherReceiveWire
{
    String exchange_id;
    UInt64 num_buckets = 0;
    /// Present when the gather keeps the input order.
    std::optional<SortDescription> maintain_sort_description;

    bool operator==(const GatherReceiveWire &) const = default;
};

}
