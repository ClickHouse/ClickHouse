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

    /// The gate runs the check for the first sink that gets here and makes every other sink of the query
    /// observe its outcome - including a failure, which every one of them rethrows - instead of running
    /// the check again against the state as it is a moment later.
    insert_start_gate->run(check);
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
