#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

SinkToStorage::SinkToStorage(SharedHeader header) : ExceptionKeepingTransform(header, header, false) {}

void SinkToStorage::runOnceBeforeFirstWrite(const std::function<void()> & check)
{
    /// A sink that is not a part of a parallel group is the only writer of the query, so there is nothing to share.
    if (!insert_start_gate)
    {
        check();
        return;
    }

    /// If `check` throws, the flag stays unset and the next sink runs the check again. It observes the same
    /// state and throws the same error, which is what the query gets in either case.
    std::call_once(*insert_start_gate, check);
}

void SinkToStorage::onConsume(Chunk chunk)
{
    consume(chunk);
    cur_chunk = std::move(chunk);
}

SinkToStorage::GenerateResult SinkToStorage::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

}
