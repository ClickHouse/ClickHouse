#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Interpreters/SquashingTransform.h>

namespace DB
{

class SquashingChunksTransform : public ExceptionKeepingTransform
{
public:
    explicit SquashingChunksTransform(
        const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, bool reserve_memory = false);

    String getName() const override { return "SquashingTransform"; }

    void work() override;

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;
    void onFinish() override;


private:
    SquashingTransform squashing;
    Chunk cur_chunk;
    Chunk finish_chunk;
};

}
