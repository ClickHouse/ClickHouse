#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

SinkToStorage::SinkToStorage(SharedHeader header) : ExceptionKeepingTransform(header, header, false) {}

void SinkToStorage::setInsertStartGate(InsertStartGatePtr gate)
{
    insert_start_gate = std::move(gate);
    if (!insert_start_gate)
        return;

    if (auto check = getPreWriteCheck(); check.check)
        insert_start_gate->addCheck(std::move(check));
}

void SinkToStorage::runPreWriteCheck()
{
    /// A sink that is not a part of a parallel group is the only writer of the query, so there is nothing to share.
    if (!insert_start_gate)
    {
        if (auto check = getPreWriteCheck(); check.check)
            check.check();
        return;
    }

    /// The gate runs the checks for the first sink that gets here and makes every other sink of the query
    /// observe its outcome - including a failure, which every one of them rethrows - instead of running
    /// the checks again against the state as it is a moment later.
    insert_start_gate->run();
}

void SinkToStorage::runHiddenPreWriteChecks(const StorageID * forwarded_to) const
{
    for (const auto & hidden : hidden_pre_write_checks)
    {
        if (!hidden.enforced_here)
            continue;

        if (forwarded_to
            && (hidden.forwarded_to.getDatabaseName() != forwarded_to->getDatabaseName()
                || hidden.forwarded_to.getTableName() != forwarded_to->getTableName()))
            continue;

        /// The deferred check runs as a part of the decision of the gate, and the outcome is written under
        /// the lock of the gate that makes it, which `run` acquires as well.
        hidden.gate->run();
        if (hidden.outcome->exception)
            std::rethrow_exception(hidden.outcome->exception);
    }
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
