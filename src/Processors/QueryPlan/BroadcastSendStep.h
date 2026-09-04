#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>


namespace DB
{

struct BroadcastSendWire;

/// Send part of BroadcastExchangeStep
/// Copies all data to each of the destination buckets
class BroadcastSendStep final : public IQueryPlanStep
{
public:
    BroadcastSendStep(SharedHeader input_header_, const String & exchange_id_, size_t num_buckets_)
        : exchange_id(exchange_id_)
        , num_buckets(num_buckets_)
    {
        chassert(num_buckets > 0);
        updateInputHeaders({std::move(input_header_)});
    }

    String getName() const override { return "BroadcastSend"; }

    bool hasOutputStream() const { return false; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `BroadcastSendStep.cpp` declares.
    BroadcastSendWire toWire() const;
    static QueryPlanStepPtr fromWire(BroadcastSendWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override {}

    const String exchange_id;
    const size_t num_buckets;
};

/// What `BroadcastSendStep` puts on the wire in the framed format.
struct BroadcastSendWire
{
    String exchange_id;
    UInt64 num_buckets = 0;

    bool operator==(const BroadcastSendWire &) const = default;
};

}
