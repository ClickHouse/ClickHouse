#pragma once
#include <Interpreters/Squashing.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

class ApplySquashingTransform : public ExceptionKeepingTransform
{
public:
    explicit ApplySquashingTransform(const Block & header)
        : ExceptionKeepingTransform(header, header, false)
        , squashing(header)
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
        if (finish_chunk)
        {
            data.chunk = std::move(finish_chunk);
            ready_output = true;
        }
    }

protected:
    void onConsume(Chunk chunk) override
    {
        if (auto block = squashing.add(std::move(chunk)))
            cur_chunk.setColumns(block.getColumns(), block.rows());
    }

    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        res.is_done = true;
        return res;
    }
    void onFinish() override
    {
        auto block = squashing.add({});
        finish_chunk.setColumns(block.getColumns(), block.rows());
    }

private:
    ApplySquashing squashing;
    Chunk cur_chunk;
    Chunk finish_chunk;
};

}
