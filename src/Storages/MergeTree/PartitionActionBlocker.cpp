#include <Storages/MergeTree/PartitionActionBlocker.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <shared_mutex>

namespace DB
{

ActionLock PartitionActionBlocker::cancelForPartition(const std::string & partition_id)
{
    std::unique_lock lock(state->mutex);
    const size_t prev_size = state->partition_blockers.size();

    ActionLock result = state->partition_blockers[partition_id].cancel();

    /// Cleanup stale `ActionBlocker` instances once in a while, to prevent unbound growth.
    ++state->cleanup_counter;
    if (prev_size != state->partition_blockers.size() && state->cleanup_counter > 32) // 32 is arbitrary.
        compactPartitionBlockersLocked();

    return result;
}

bool PartitionActionBlocker::isCancelledForPartition(const std::string & partition_id) const
{
    return state->isCancelledForPartition(partition_id);
}

CancellationRequestPredicate PartitionActionBlocker::getCancellationRequestPredicate(std::string partition_id_) const
{
    return [shared_state = state, partition_id = std::move(partition_id_)]
    {
        return shared_state->isCancelledForPartition(partition_id) ? CancellationRequest::StopWork : CancellationRequest::None;
    };
}

bool PartitionActionBlocker::State::isCancelledForPartition(const std::string & partition_id) const
{
    if (global_blocker.isCancelled())
        return true;

    std::shared_lock lock(mutex);
    auto p = partition_blockers.find(partition_id);
    return p != partition_blockers.end() && p->second.isCancelled();
}

size_t PartitionActionBlocker::countPartitionBlockers() const
{
    std::shared_lock lock(state->mutex);
    return state->partition_blockers.size();
}

void PartitionActionBlocker::compactPartitionBlockers()
{
    std::unique_lock lock(state->mutex);

    compactPartitionBlockersLocked();
}

void PartitionActionBlocker::compactPartitionBlockersLocked()
{
    std::erase_if(state->partition_blockers, [](const auto & p)
    {
        return !p.second.isCancelled();
    });
    state->cleanup_counter = 0;
}

std::string PartitionActionBlocker::formatDebug() const
{
    std::shared_lock lock(state->mutex);

    WriteBufferFromOwnString out;
    out << "Global lock: " << state->global_blocker.getCounter().load()
            << "\n"
            << state->partition_blockers.size() << " live partition locks: {";

    size_t i = 0;
    for (const auto & p : state->partition_blockers)
    {
        out << "\n\t" << DB::double_quote << p.first << ": " << p.second.getCounter().load();

        if (++i < state->partition_blockers.size())
            out << ", ";
        else
            out << "\n";
    }
    out << "}";

    return out.str();
}

}
