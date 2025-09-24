#include <Processors/Transforms/ApplySquashingTransform.h>

namespace DB
{

ApplySquashingTransform::ApplySquashingTransform(SharedHeader header)
: ExceptionKeepingTransform(header, header, false)
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
}

ExceptionKeepingTransform::GenerateResult DB::ApplySquashingTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

void ApplySquashingTransform::onConsume(Chunk chunk)
{
    cur_chunk = Squashing::squash(std::move(chunk));
}

}
