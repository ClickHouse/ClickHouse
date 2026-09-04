#pragma once

#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <unordered_map>

namespace DB
{

/// N-in / N-out processor between the in-order `MergeTree` streams and the final
/// `MergingSortedTransform` of a read-in-order query. Lane i connects input i to output i.
///
/// Skeleton: every lane passes its chunks through untouched, so the merge alone decides when a
/// stream is read. The read-ahead policy is to be built into the hooks below.
class VirtualRowReadAheadTransform final : public IProcessor
{
public:
    VirtualRowReadAheadTransform(
        SharedHeader header_,
        size_t num_lanes,
        SortDescription description_,
        bool apply_virtual_row_conversions_,
        UInt64 limit_,
        size_t max_rows_to_buffer_,
        size_t max_bytes_to_buffer_,
        size_t read_ahead_window_);

    String getName() const override { return "VirtualRowReadAhead"; }

    Status prepare() override;
    Status prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs) override;

private:
    struct Lane
    {
        InputPort * input = nullptr;
        OutputPort * output = nullptr;
        bool finished = false;
    };

    Status prepareImpl(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs);

    /// Moves one lane as far as it can go right now.
    void serve(size_t lane_num);

    /// Policy hooks, empty for now.
    bool mayRead(size_t lane_num) const;
    void consume(size_t lane_num, Chunk chunk);
    void finishLane(size_t lane_num);

    /// Inputs of the policy, unused by the skeleton.
    [[maybe_unused]] SharedHeader header;
    [[maybe_unused]] SortDescription description;
    [[maybe_unused]] const bool apply_virtual_row_conversions;
    [[maybe_unused]] const UInt64 limit;
    [[maybe_unused]] const size_t max_rows_to_buffer;
    [[maybe_unused]] const size_t max_bytes_to_buffer;
    [[maybe_unused]] const size_t read_ahead_window;

    std::vector<Lane> lanes;
    std::unordered_map<const Port *, size_t> lane_by_port;
    size_t finished_lanes = 0;
    bool initialized = false;
};

}
