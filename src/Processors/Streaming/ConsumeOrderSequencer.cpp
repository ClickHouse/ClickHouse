#include <Processors/Streaming/ConsumeOrderSequencer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ConsumeOrderSequencer::addChunk(Chunk new_chunk)
{
    chunks.push_back(std::move(new_chunk));
}

ExtractResult ConsumeOrderSequencer::tryExtractNext([[maybe_unused]] size_t streams_count, [[maybe_unused]] size_t stream_index)
{
    if (chunks.empty())
        return NeedData{};

    auto chunk = std::move(chunks.front());
    chunks.pop_front();

    return Emit{std::move(chunk)};
}

void ConsumeOrderSequencer::recalcState()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "recalcState should not be called on ConsumeOrderSequencer");
}

}
