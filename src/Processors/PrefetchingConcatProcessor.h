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
  * Data from non-current inputs is buffered (up to max_buffered_chunks per input)
  * to keep their upstream sources producing data while we consume the current input.
  *
  * This is useful for read-in-order optimization where a single part is split
  * into non-overlapping range groups read by separate sources: we want parallel
  * IO/filtering across groups while preserving the sorted order via concatenation.
  */
class PrefetchingConcatProcessor final : public IProcessor
{
public:
    PrefetchingConcatProcessor(SharedHeader header, size_t num_inputs, size_t max_buffered_chunks_ = 2, size_t max_prefetch_inputs_ = 2);

    String getName() const override { return "PrefetchingConcat"; }

    Status prepare() override;

    OutputPort & getOutputPort() { return outputs.front(); }

private:
    size_t current_input_idx = 0;
    size_t max_buffered_chunks;
    size_t max_prefetch_inputs;
    std::vector<std::deque<Chunk>> buffers;
};

}
