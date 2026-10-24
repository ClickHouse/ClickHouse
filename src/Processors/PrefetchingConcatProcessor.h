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
  * Unlike ConcatProcessor, marks ALL inputs as needed simultaneously,
  * which allows the pipeline executor to schedule upstream sources in parallel.
  * Data from non-current inputs is buffered to keep their upstream sources producing data
  * while we consume the current input.
  *
  * Since the output consumes the inputs one after another, sustained parallelism requires
  * the later inputs to run far ahead of the current one, so a small per-input cap
  * (a couple of chunks) degenerates into reading one input at a time. Instead, the buffers
  * share a budget of `max_buffered_bytes`: an input keeps reading ahead while all buffers
  * together hold less than the budget. The inputs are visited starting from the current one,
  * so the nearest inputs, which are needed first, get the freed budget first. The budget bounds
  * the total buffered bytes (exceeded by at most one chunk, plus the head start of the current
  * input). Only the current input may buffer up to `min_buffered_chunks` chunks regardless of
  * the budget, so that it always makes progress. If `max_buffered_bytes` is 0, every input
  * buffers up to `min_buffered_chunks` chunks.
  *
  * This is useful for read-in-order optimization where a single part is split
  * into non-overlapping range groups read by separate sources: we want parallel
  * IO/filtering across groups while preserving the sorted order via concatenation.
  */
class PrefetchingConcatProcessor final : public IProcessor
{
public:
    PrefetchingConcatProcessor(SharedHeader header, size_t num_inputs, size_t max_buffered_bytes_, size_t min_buffered_chunks_ = 2);

    String getName() const override { return "PrefetchingConcat"; }

    Status prepare() override;

    OutputPort & getOutputPort() { return outputs.front(); }

private:
    struct Buffer
    {
        std::deque<Chunk> chunks;
        size_t bytes = 0;
    };

    size_t current_input_idx = 0;
    size_t max_buffered_bytes;
    size_t min_buffered_chunks;
    std::vector<Buffer> buffers;
};

}
