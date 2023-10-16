#pragma once

#include <Processors/IProcessor.h>

#include <queue>

namespace Poco
{
class Logger;
}

namespace DB
{

namespace Streaming
{
/** Has arbitrary non zero number of inputs and arbitrary non zero number of outputs.
  * All of them have the same structure.
  *
  * Pulls data from arbitrary input (whenever it is ready) and pushes it to arbitrary output (whenever is is not full).
  * Doesn't do any heavy calculations.
  * Doesn't preserve an order of data.
  *
  * Examples:
  * - union data from multiple inputs to single output - to serialize data that was processed in parallel.
  * - split data from single input to multiple outputs - to allow further parallel processing.
  *
  * Resize processor needs rewritten for streaming processing since
  * 1. We may need align watermarks
  * 2. We may need propagate checkpoint barrier
  * Since resize processor can be arbitrary M inputs -> N outputs dimension mapping, this introduces some challenges when
  * dealing with watermarks and checkpoint barrier propagations
  *     Input -> Output
  * Case 1: M -> M: [USE: StrictResizeProcessor]
  *   a. For watermarks, don't need alignment, (actually defer the watermark alignment to down stream pipe)
  *   b. For checkpoint barriers, don't need alignment
  * Case 2: M -> 1 (M > 1) [USE: ShrinkResizeProcessor]
  *   a. For watermarks, need wait for all watermarks from all M inputs and pick the smallest watermark as the watermark
  *   b. For checkpoint barriers, need wait for all checkpoint barriers from all M inputs and then propagate the checkpoint
  *      barrier further to the down stream
  * Case 3: 1 -> N (N > 1) [USE: ExpandResizeProcessor]
  *   a. For watermarks, replicate watermark to all N outputs
  *   b. For checkpoint barriers, replicate checkpoint barriers to all N outputs
  * Case 4: M -> N (M != N, M > 1, N > 1) [USE: NO SUPPORT]
  *   a. For watermarks, don't support this combination
  *   b. For checkpoint barriers, replicate checkpoint barriers to all N outputs
  */

/// @brief Resize M -> 1
class ShrinkResizeProcessor final : public IProcessor
{
public:
    ShrinkResizeProcessor(const Block & header, size_t num_inputs);

    String getName() const override { return "StreamingShrinkResize"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    size_t num_finished_inputs = 0;
    std::queue<UInt64> inputs_with_data;
    bool initialized = false;

    enum class InputStatus
    {
        NeedData,
        NotNeedData,
        HasData,
        Finished,
    };

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
        Int64 watermark = 0;
        bool requested_checkpoint = false;
    };

    std::vector<InputPortWithStatus> input_ports;

    /// @returns true if has watermark handling.
    bool updateAndAlignWatermark(InputPortWithStatus & input_with_data, Chunk & chunk);

    /// Used in `updateAndAlignWatermark`
    Int64 aligned_watermark = INVALID_WATERMARK;

    Poco::Logger * log;
};

/// @brief Resize 1 -> N
class ExpandResizeProcessor final : public IProcessor
{
public:
    ExpandResizeProcessor(const Block & header, size_t num_outputs);

    String getName() const override { return "StreamingExpandResize"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    size_t num_finished_outputs = 0;

    bool initialized = false;

    enum class OutputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;

        static constexpr UInt8 NO_PROPAGATE = 0x0;
        static constexpr UInt8 PROPAGATE_HEARTBEAT = 0x1;
        static constexpr UInt8 PROPAGATE_WATERMARK = 0x2;
        static constexpr UInt8 PROPAGATE_CHECKPOINT_REQUEST = 0x8;
        UInt8 propagate_flag = NO_PROPAGATE;
    };

    std::vector<OutputPortWithStatus> output_ports;
    std::list<OutputPortWithStatus *> waiting_outputs;

    /// To propagate
    Chunk header_chunk;
    Int64 watermark = INVALID_WATERMARK;
};

class StrictResizeProcessor final : public IProcessor
{
public:
    StrictResizeProcessor(const Block & header, size_t num_inputs_and_outputs);

    String getName() const override { return "StreamingStrictResize"; }

    Status prepare(const PortNumbers &, const PortNumbers &) override;

private:
    size_t num_finished_inputs = 0;
    size_t num_finished_outputs = 0;
    std::queue<UInt64> disabled_input_ports;
    std::queue<UInt64> waiting_outputs;
    bool initialized = false;

    enum class OutputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    enum class InputStatus
    {
        NotActive,
        NeedData,
        Finished,
    };

    struct InputPortWithStatus
    {
        InputPort * port;
        InputStatus status;
        ssize_t waiting_output;
    };

    struct OutputPortWithStatus
    {
        OutputPort * port;
        OutputStatus status;
    };

    std::vector<InputPortWithStatus> input_ports;
    std::vector<OutputPortWithStatus> output_ports;
    /// This field contained chunks which were read for output which had became finished while reading was happening.
    /// They will be pushed to any next waiting output.
    std::vector<Port::Data> abandoned_chunks;
};

}
}
