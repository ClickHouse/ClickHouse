#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

SinkToStorage::SinkToStorage(SharedHeader header) : ExceptionKeepingTransform(header, header, false) {}

void SinkToStorage::setCommitOrder(
    std::function<bool(size_t)> allowed,
    std::function<bool()> failed,
    std::function<void()> done,
    std::function<void()> fail,
    std::function<bool(size_t)> epoch_done,
    std::function<int()> schedule_fd,
    std::function<void()> drain,
    std::function<void()> signal)
{
    commit_order.emplace(CommitOrder{
        std::move(allowed),
        std::move(failed),
        std::move(done),
        std::move(fail),
        std::move(epoch_done),
        std::move(schedule_fd),
        std::move(drain),
        std::move(signal),
        0});
}

bool SinkToStorage::readyForNextChunk() const
{
    if (!commit_order || !commit_order->hold_next)
        return true;
    if (isCancelled() || commit_order->failed() || commit_order->epoch_done(commit_order->epoch))
    {
        commit_order->hold_next = false;
        return true;
    }
    return false;
}

void SinkToStorage::holdNextChunk()
{
    if (commit_order)
        commit_order->hold_next = true;
}

bool SinkToStorage::readyForCommit() const
{
    if (!commit_order || !orderedCommitPending())
        return true;
    if (isCancelled() || commit_order->failed())
        return true;
    return commit_order->allowed(commit_order->epoch);
}

int SinkToStorage::commitWaitFD() const
{
    if (!commit_order)
        return -1;
    return commit_order->schedule_fd();
}

void SinkToStorage::drainCommitWait()
{
    if (commit_order)
        commit_order->drain();
}

void SinkToStorage::signalCommitWait()
{
    if (commit_order)
        commit_order->signal();
}

void SinkToStorage::finishCommitStep()
{
    if (!commit_order)
        return;
    commit_order->done();
    ++commit_order->epoch;
}

void SinkToStorage::failCommitOrder()
{
    if (commit_order)
        commit_order->fail();
}

void SinkToStorage::onConsume(Chunk chunk)
{
    consume(chunk);
    cur_chunk = std::move(chunk);
}

SinkToStorage::GenerateResult SinkToStorage::onGenerate()
{
    try
    {
        if (isCancelled() || (commit_order && commit_order->failed()))
            abandonDeferredChunk();
        else
            commitDeferredChunk();
    }
    catch (...)
    {
        failCommitOrder();
        abandonDeferredChunk();
        throw;
    }

    GenerateResult res;
    res.chunk = std::move(cur_chunk);
    res.is_done = true;
    return res;
}

}
