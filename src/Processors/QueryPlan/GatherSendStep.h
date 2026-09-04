#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Core/SortDescription.h>

#include <optional>


namespace DB
{

struct GatherSendWire;

/// Send part of GatherExchangeStep
class GatherSendStep final : public IQueryPlanStep
{
public:
    /// `maintain_sort_description_`, when set, must match `GatherReceiveStep`'s - see `updatePipeline`.
    GatherSendStep(SharedHeader input_header_, const String & exchange_id_,
                   std::optional<SortDescription> maintain_sort_description_ = std::nullopt)
        : exchange_id(exchange_id_)
        , maintain_sort_description(std::move(maintain_sort_description_))
    {
        updateInputHeaders({std::move(input_header_)});
    }

    String getName() const override { return "GatherSend"; }

    bool hasOutputStream() const { return false; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);

    /// The framed format: the wire struct is what the manifest in `GatherSendStep.cpp` declares.
    GatherSendWire toWire() const;
    static QueryPlanStepPtr fromWire(GatherSendWire wire, Deserialization & ctx);

private:
    /// Streams below the framed format.
    void serializeLegacy(Serialization & ctx) const;
    static QueryPlanStepPtr deserializeLegacy(Deserialization & ctx);
    void updateOutputHeader() override {}

    const String exchange_id;
    const std::optional<SortDescription> maintain_sort_description;
};

/// What `GatherSendStep` puts on the wire in the framed format.
struct GatherSendWire
{
    String exchange_id;
    /// Present when the gather keeps the input order.
    std::optional<SortDescription> maintain_sort_description;

    bool operator==(const GatherSendWire &) const = default;
};

}
