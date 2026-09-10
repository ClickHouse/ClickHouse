#pragma once

#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <deque>

namespace DB
{

/// N-in / N-out processor between the in-order `MergeTree` streams and the final
/// `MergingSortedTransform` of a read-in-order query. Lane i connects input i to output i.
///
/// Starts on demand, then reads ahead when a portion produces too few surviving rows.
/// Active lanes buffer independently of speculation; consecutive announcements coalesce.
/// The merge still owns ordering and decides when to stop for `LIMIT`.
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
    void work() override;

private:
    struct Lane
    {
        InputPort * input = nullptr;
        OutputPort * output = nullptr;
        Chunk incoming;
        std::deque<Chunk> chunks;
        Columns boundary;
        size_t rows_since_boundary = 0;
        size_t buffered_rows = 0;
        size_t buffered_bytes = 0;
        UInt64 rows_read = 0;
        bool output_started = false;
        bool demanded = false;
        bool read_requested = false;
        bool limit_reached = false;
        bool input_finished = false;
        bool finished = false;
    };

    void finishLane(Lane & lane);
    Columns getBoundary(const Chunk & chunk, size_t row) const;
    int compareBoundaries(const Columns & lhs, const Columns & rhs) const;
    bool earlier(size_t lhs, size_t rhs) const;
    bool needsMoreSources(size_t lane_num, const Chunk & chunk) const;
    bool canBuffer(const Lane & lane) const;

    const SharedHeader header;
    const SortDescription description;
    const bool apply_virtual_row_conversions;
    const UInt64 limit;
    const size_t max_rows_to_buffer;
    const size_t max_bytes_to_buffer;
    const size_t read_ahead_window;
    const size_t useful_rows_target;
    std::vector<size_t> sort_positions;

    std::vector<Lane> lanes;
    std::vector<size_t> candidates;
    std::vector<size_t> ready_lanes;
    size_t finished_lanes = 0;
    bool read_ahead_started = false;
};

}
