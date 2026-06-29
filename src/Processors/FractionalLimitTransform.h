#pragma once

#include <deque>
#include <vector>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <base/types.h>

namespace DB
{

/// Implementation for LIMIT N OFFSET M
/// where N and M are fractions in (0, 1) range (non-inclusive) representing percentages.
///
/// This processor supports multiple inputs and outputs (the same number).
/// The output ports are interchangeable, chunks can be pushed to any available output.
/// The reason to have multiple ports is to be able to stop all sources when limit is reached, in a query like:
///     SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT 0.1
///
/// with_ties - implementation of LIMIT WITH TIES. It works only for single port.
///
/// Processor workflow:
/// while input.read():
///     1. read and cache input chunk
///     2. increase total input rows counter
///     3. if offset or fractional_offset, drop from cache chunks that we are 100% sure will be skipped entirely
///     4. remove from cache and push to output chunks that we are 100% sure will be returned
///     5. calculate integral limit/offset = (total_input_rows * fraction)
///     6. apply normal limit/offset logic on remaining cached chunks.
class FractionalLimitTransform final : public IProcessor
{
private:
    Float64 limit_fraction;
    Float64 offset_fraction;

    /// Variables to hold remaining integral limit/offset values to use.
    UInt64 offset_rows = 0; // additionally holds UInt64 offset_ from constructor
    UInt64 limit_rows = 0;

    bool with_ties;
    const SortDescription limit_with_ties_sort_description;

    Chunk ties_last_row; /// for WITH TIES, contains only sort columns
    std::vector<size_t> sort_key_positions;

    /// Total number of rows already accounted for from the cache
    /// (dropped due to offset / evicted / pushed to output).
    UInt64 rows_processed = 0;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// Per-port state.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_input_finished = false;
    };

    std::vector<PortsData> ports_data;
    UInt64 num_finished_input_ports = 0;

    /// Total number of input rows.
    UInt64 total_input_rows = 0;
    /// Number of rows pushed early (before all input is read).
    UInt64 early_pushed_rows = 0;

    /// Guard for finalizeLimits(): the integral values depend on total_input_rows and
    /// offset_rows is updated additively (offset_rows += ceil(total_input_rows * offset_fraction)),
    /// so we must not run it more than once.
    bool limits_are_final = false;

    /// Round-robin cursor for selecting an available output port.
    size_t next_output_port = 0;

    std::deque<Chunk> cached_chunks;

    /// Convert fractional limit/offset to integral values once total_input_rows is known.
    void finalizeLimits();

    /// Find any output port that can accept data (outputs are interchangeable).
    OutputPort * getAvailableOutputPort();

    /// Return true if all output ports are finished (nobody needs data).
    bool allOutputsFinished() const;

    Chunk makeChunkWithPreviousRow(const Chunk & current_chunk, UInt64 row_num) const;
    ColumnRawPtrs extractSortColumns(const Columns & columns) const;
    bool sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, UInt64 current_chunk_row_num) const;

public:
    FractionalLimitTransform(
        SharedHeader header_,
        Float64 limit_fraction_,
        Float64 offset_fraction_,
        UInt64 offset_ = 0,
        size_t num_streams = 1,
        bool with_ties_ = false,
        SortDescription description_ = {});

    String getName() const override { return "FractionalLimit"; }

    Status prepare() override;
    Status pullData(PortsData & data);
    Status pushData();
    void splitChunk(Chunk & current_chunk);

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
};

}
