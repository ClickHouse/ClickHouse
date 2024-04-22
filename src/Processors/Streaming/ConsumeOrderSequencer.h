#pragma once

#include <list>

#include <Processors/Streaming/ISequencer.h>

namespace DB
{

class ConsumeOrderSequencer : public ISequencer
{
public:
    void addChunk(Chunk new_chunk) override;

    ExtractResult tryExtractNext(size_t streams_count, size_t stream_index) override;

    void recalcState() override;

private:
    std::list<Chunk> chunks;
};

}
