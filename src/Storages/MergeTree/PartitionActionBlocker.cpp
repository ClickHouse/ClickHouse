#include <Storages/MergeTree/PartitionActionBlocker.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

ActionLock PartitionActionBlocker::cancelForPartition(const std::string & partition_id)
{
    std::unique_lock lock(mutex);
    const size_t prev_size = partition_blockers.size();

    ActionLock result = partition_blockers[partition_id].cancel();

    /// Cleanup stale `ActionBlocker` instances once in a while, to prevent unbound growth.
    ++cleanup_counter;
    if (prev_size != partition_blockers.size() && cleanup_counter > 32) // 32 is arbitrary.
        compactPartitionBlockersLocked();

    return result;
}

bool PartitionActionBlocker::isCancelledForPartition(const std::string & partition_id) const
{
    if (isCancelled())
        return true;

    std::shared_lock lock(mutex);
    return isCancelledForPartitionOnlyLocked(partition_id);
}

bool PartitionActionBlocker::isCancelledForPartitionOnlyLocked(const std::string & partition_id) const
{
    auto p = partition_blockers.find(partition_id);
    return p != partition_blockers.end() && p->second.isCancelled();
}

size_t PartitionActionBlocker::countPartitionBlockers() const
{
    std::shared_lock lock(mutex);
    return partition_blockers.size();
}

void PartitionActionBlocker::compactPartitionBlockers()
{
    std::unique_lock lock(mutex);

    compactPartitionBlockersLocked();
}

void PartitionActionBlocker::compactPartitionBlockersLocked()
{
    std::erase_if(partition_blockers, [](const auto & p)
    {
        return !p.second.isCancelled();
    });
    cleanup_counter = 0;
}

std::string PartitionActionBlocker::formatDebug() const
{
    std::shared_lock lock(mutex);

    WriteBufferFromOwnString out;
    out << "Global lock: " << global_blocker.getCounter().load()
            << "\n"
            << partition_blockers.size() << " live partition locks: {";

    size_t i = 0;
    for (const auto & p : partition_blockers)
    {
        out << "\n\t" << DB::double_quote << p.first << ": " << p.second.getCounter().load();

        if (++i < partition_blockers.size())
            out << ", ";
        else
            out << "\n";
    }
    out << "}";

    return out.str();
}

}
