#pragma once

#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Implementation for LIMIT N OFFSET M
/// This processor support multiple inputs and outputs (the same number).
/// Each pair of input and output port works independently.
/// The reason to have multiple ports is to be able to stop all sources when limit is reached, in a query like:
///     SELECT * FROM system.numbers_mt WHERE number = 1000000 LIMIT 1
///
/// always_read_till_end - read all data from input ports even if limit was reached.
/// with_ties, description - implementation of LIMIT WITH TIES. It works only for single port.
class LimitTransform final : public IProcessor
{
private:
    const UInt64 limit;
    const UInt64 offset;

    bool always_read_till_end;

    bool with_ties;
    const SortDescription description;

    Chunk previous_row_chunk;  /// for WITH TIES, contains only sort columns
    std::vector<size_t> sort_column_positions;

    UInt64 rows_read = 0; /// including the last read block
    RowsBeforeLimitCounterPtr rows_before_limit_at_least;

    /// State of port's pair.
    /// Chunks from different port pairs are not mixed for better cache locality.
    struct PortsData
    {
        Chunk current_chunk;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;
        bool is_finished = false;

        /// This flag is used to avoid counting rows multiple times before applying a limit
        /// condition, which can happen through certain input ports like PartialSortingTransform and
        /// RemoteSource.
        bool input_port_has_counter = false;

        /// When limit and offset are negative,
        /// a input port is finished does not mean its output port need to be finished.
        bool is_input_port_finished = false;
        bool is_output_port_finished = false;
    };

    std::vector<PortsData> ports_data;
    size_t num_finished_port_pairs = 0;
    size_t num_finished_input_port = 0; /// used when limit and offset are negative
    size_t num_finished_output_port = 0; /// used when limit and offset are negative

    const bool limit_is_unreachable;
    const bool is_negative;

    struct QueueElement
    {
        Chunk chunk;
        size_t port;
    };

    /// Used when limit and offset are negative, storing at least rows_to_keep rows
    std::list<QueueElement> queue;
    UInt64 rows_in_queue = 0;

    /// Used when limit and offset are negative,
    /// equals to limit + offset or offset when limit is unreachable
    UInt64 rows_to_keep;

    Chunk makeChunkWithPreviousRow(const Chunk & current_chunk, UInt64 row_num) const;
    ColumnRawPtrs extractSortColumns(const Columns & columns) const;
    bool sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, UInt64 current_chunk_row_num) const;

    void queuePushBack(QueueElement & element);
    QueueElement queuePopFront();
    QueueElement popAndCutIfNeeded();
    void skipChunksForFinishedOutputPorts();
    Status loopPop();

    Status prepareNonNegative(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/);
    Status prepareNegative(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/);

public:
    LimitTransform(
        const Block & header_, UInt64 limit_, UInt64 offset_, size_t num_streams = 1,
        bool always_read_till_end_ = false, bool with_ties_ = false, bool is_negative_ = false,
        SortDescription description_ = {});

    String getName() const override { return "Limit"; }

    Status prepare(const PortNumbers & /*updated_input_ports*/, const PortNumbers & /*updated_output_ports*/) override;
    Status prepare() override; /// Compatibility for TreeExecutor.
    Status preparePairNonNegative(PortsData & data);
    void preparePairNegative(size_t pos);
    void splitChunk(PortsData & data);

    InputPort & getInputPort() { return inputs.front(); }
    OutputPort & getOutputPort() { return outputs.front(); }

    void setRowsBeforeLimitCounter(RowsBeforeLimitCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }
    void setInputPortHasCounter(size_t pos) { ports_data[pos].input_port_has_counter = true; }
};

}
