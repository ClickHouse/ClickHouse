#pragma once

#include <Core/QueryCoordination.h>
#include <Core/SortDescription.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

#include <deque>

namespace DB
{

/** One gate serves one logical shard: it submits candidates once and consumes one terminal
  * selection or fallback response. Cancellation arrives as an exception. The callback owns
  * the request/response transport; the gate does not own its connection.
  */
class DistributedTopKCandidateGateTransform final : public IProcessor
{
public:
    DistributedTopKCandidateGateTransform(
        SharedHeader header_,
        UInt64 limit_,
        SortDescription sort_description_,
        QueryCoordinationCallback coordination_callback_);

    String getName() const override { return "DistributedTopKCandidateGateTransform"; }

    Status prepare() override;
    void work() override;

private:
    void retainChunk(Chunk chunk);
    void coordinate();
    void selectRows(const std::vector<UInt64> & selected_ordinals);
    void validateRowCount(UInt64 rows) const;

    SharedHeader header;
    UInt64 limit;
    SortDescription sort_description;
    QueryCoordinationCallback coordination_callback;
    std::vector<size_t> sort_key_positions;
    MutableColumns candidate_columns;

    UInt64 retained_rows = 0;
    Chunk current_chunk;
    std::deque<Chunk> retained_chunks;
    std::deque<Chunk> output_chunks;
    bool coordination_complete = false;
};

}
