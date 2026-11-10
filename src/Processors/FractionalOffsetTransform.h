#pragma once

#include <deque>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <base/types.h>

namespace DB
{

/// Implementation for OFFSET N (without limit)
///  where N is a fraction in (0, 1) range (non-inclusive) representing a percentage.
///
/// This processor supports multiple inputs and outputs (the same number).
/// The output ports are interchangeable, chunks can be pushed to any available output.
///
/// Processor workflow:
/// while input.read():
///     1. read and cache input chunk
///     2. increase total input rows counter
///     3. drop from cache chunks that
///        we became 100% sure they will be offsetted.
/// 3. calculate remaining integral offset = (fractional_offset * rows_cnt) - evicted_rows_cnt
/// 4. apply normal offset logic on remaining cached chunks.
class FractionalOffsetTransform final : public IProcessor
{
private:
    Float64 fractional_offset;
    /// Remaining integral offset to apply to the cached chunks once all input is read.
    UInt64 offset = 0;

    /// Guard for finalizeOffset(): offset depends on total rows_cnt and is later consumed
    /// (offset is set to 0 after splitting the first chunk), so it must be computed only once.
    bool offset_is_final = false;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// Per-port state.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_input_port_finished = false;
    };

    std::vector<PortsData> ports_data;
    size_t num_finished_input_ports = 0;

    UInt64 rows_cnt = 0;
    UInt64 evicted_rows_cnt = 0;

    size_t next_output_port = 0;

    std::deque<Chunk> chunks_cache;

    /// Compute remaining integral offset once total rows_cnt is known.
    void finalizeOffset();

    OutputPort * getAvailableOutputPort();
    bool allOutputsFinished() const;

public:
    FractionalOffsetTransform(const Block & header_, Float64 fractional_offset_, size_t num_streams = 1);

    String getName() const override { return "FractionalOffset"; }

    Status prepare() override;
    Status pullData(PortsData & data);
    Status pushData();
    void splitChunk(Chunk & current_chunk) const;

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
};

}
