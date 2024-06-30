#include <Processors/Transforms/ApplySquashingTransform.h>

namespace DB
{

ApplySquashingTransform::ApplySquashingTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes)
    : ExceptionKeepingTransform(header, header, false), squashing(header, min_block_size_rows, min_block_size_bytes)
{
}

void ApplySquashingTransform::work()
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

void ApplySquashingTransform::onConsume(Chunk chunk)
{
    cur_chunk = DB::Squashing::squash(std::move(chunk));
}

ExceptionKeepingTransform::GenerateResult ApplySquashingTransform::onGenerate()
{
    return GenerateResult{
      .chunk = std::move(cur_chunk),
      .is_done = true
    };
}

void ApplySquashingTransform::onFinish()
{
    finish_chunk = DB::Squashing::squash({});
}

}
