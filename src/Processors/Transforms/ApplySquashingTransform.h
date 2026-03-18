#pragma once
#include <Interpreters/Squashing.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

class ApplySquashingTransform : public ExceptionKeepingTransform
{
public:
    explicit ApplySquashingTransform(const Block & header, const size_t min_block_size_rows, const size_t min_block_size_bytes)
        : ExceptionKeepingTransform(header, header, false)
        , squashing(header, min_block_size_rows, min_block_size_bytes)
    {
    }

    String getName() const override { return "ApplySquashingTransform"; }

    void work() override
    {
        if (stage == Stage::Exception)
        {
            data.chunk.clear();
            ready_input = false;
            return;
        }

        ExceptionKeepingTransform::work();
    }

protected:
    void onConsume(Chunk chunk) override
    {
        cur_chunk = Squashing::squash(std::move(chunk));
    }

    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        res.is_done = true;
        return res;
    }

private:
    Squashing squashing;
    Chunk cur_chunk;
};

}
