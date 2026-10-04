#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <deque>
#include <vector>


namespace DB
{

/** Like ConcatProcessor: has N inputs and one output, all with the same structure.
  * Outputs data from inputs in order (first all data from input 0, then input 1, etc.).
  *
  * Unlike ConcatProcessor, marks a sliding window of upcoming inputs as needed
  * (up to max_prefetch_inputs ahead of the current input), which allows the pipeline
  * executor to schedule upstream sources in parallel with bounded lookahead.
  * Data from non-current inputs is buffered to keep their upstream sources producing
  * data while we consume the current input.
  *
  * Each input in the window buffers up to a row/byte budget rather than a fixed number of
  * chunks. That budget is the one a single merge input gets from `BufferChunksTransform`,
  * which is what this processor replaces for the streams of one part - so the read-ahead a
  * stream inside the window has is the read-ahead it would have had as a merge input, and
  * the total is bounded by the window rather than by the number of streams. Depth, not the
  * number of needed inputs, is what keeps reads in flight: a stream reads its ranges
  * sequentially, so it only has a request outstanding while it still has room to put the
  * result. As in `BufferChunksTransform`, the two limits are combined with OR.
  *
  * This is useful for read-in-order optimization where a single part is split
  * into non-overlapping range groups read by separate sources: we want parallel
  * IO/filtering across groups while preserving the sorted order via concatenation.
  */
class PrefetchingConcatProcessor final : public IProcessor
{
public:
    PrefetchingConcatProcessor(
        SharedHeader header,
        size_t num_inputs,
        size_t max_rows_to_buffer_,
        size_t max_bytes_to_buffer_,
        size_t max_prefetch_inputs_ = 2);

    String getName() const override { return "PrefetchingConcat"; }

    Status prepare() override;

    OutputPort & getOutputPort() { return outputs.front(); }

private:
    struct Buffer
    {
        std::deque<Chunk> chunks;
        size_t num_rows = 0;
        size_t num_bytes = 0;

        bool hasRoom(size_t max_rows, size_t max_bytes) const { return num_rows < max_rows || num_bytes < max_bytes; }

        void push(Chunk && chunk)
        {
            num_rows += chunk.getNumRows();
            num_bytes += chunk.bytes();
            chunks.push_back(std::move(chunk));
        }

        Chunk pop()
        {
            auto chunk = std::move(chunks.front());
            chunks.pop_front();
            num_rows -= chunk.getNumRows();
            num_bytes -= chunk.bytes();
            return chunk;
        }
    };

    size_t current_input_idx = 0;
    size_t max_rows_to_buffer;
    size_t max_bytes_to_buffer;
    size_t max_prefetch_inputs;
    std::vector<Buffer> buffers;
};

}
