#include <Storages/MergeTree/ReplicatedMergeTreeAltersSequence.h>

namespace DB
{

int ReplicatedMergeTreeAltersSequence::getHeadAlterVersion(std::unique_lock<SharedMutex> & /*state_lock*/) const
{
    /// If queue empty, than we don't have version
    if (!queue_state.empty())
        return queue_state.begin()->first;
    return -1;
}

void ReplicatedMergeTreeAltersSequence::addMutationForAlter(int alter_version, std::lock_guard<SharedMutex> & /*state_lock*/)
{
    /// Metadata alter can be added before, or
    /// maybe already finished if we startup after metadata alter was finished.
    if (!queue_state.contains(alter_version))
        queue_state.emplace(alter_version, AlterState{.metadata_finished=true, .data_finished=false});
    else
        queue_state[alter_version].data_finished = false;
}

void ReplicatedMergeTreeAltersSequence::addMetadataAlter(
    int alter_version, std::lock_guard<SharedMutex> & /*state_lock*/)
{
    /// Data alter (mutation) always added before. See ReplicatedMergeTreeQueue::pullLogsToQueue.
    /// So mutation already added to this sequence or doesn't exist.
    if (!queue_state.contains(alter_version))
        queue_state.emplace(alter_version, AlterState{.metadata_finished=false, .data_finished=true});
    else
        queue_state[alter_version].metadata_finished = false;
}

void ReplicatedMergeTreeAltersSequence::finishMetadataAlter(int alter_version, std::unique_lock<SharedMutex> & /*state_lock*/)
{
    /// The queue may contain several `ALTER_METADATA` entries with the same `alter_version`: a replica that clones
    /// another one prepends a dummy `ALTER_METADATA` entry to its own queue (see
    /// `StorageReplicatedMergeTree::cloneMetadataIfNeeded`), and the copied queue of the source replica may already
    /// contain an entry with that very version. Every such entry finishes the same alter, and all completions but the
    /// first one have nothing left to do. Note that we must not use `operator[]` here: it would insert the alter back
    /// into the sequence as unfinished, and, being the smallest version, it would block every later metadata alter
    /// forever.
    auto it = queue_state.find(alter_version);
    if (it == queue_state.end())
        return;

    /// Alters have to be finished in order
    chassert(it == queue_state.begin());

    /// If data stage finished (or was never added) than we can remove this alter
    if (it->second.data_finished)
        queue_state.erase(it);
    else
        it->second.metadata_finished = true;
}

void ReplicatedMergeTreeAltersSequence::finishDataAlter(int alter_version, std::lock_guard<SharedMutex> & /*state_lock*/)
{
    /// Queue can be empty after load of finished mutation without move of mutation pointer
    if (queue_state.empty())
        return;

    /// Mutations may finish multiple times (for example, after server restart, before update of mutation pointer)
    if (alter_version >= queue_state.begin()->first)
    {
        /// All alter versions bigger than head must present in queue.
        chassert(queue_state.contains(alter_version));

        if (queue_state[alter_version].metadata_finished)
            queue_state.erase(alter_version);
        else
            queue_state[alter_version].data_finished = true;
    }
}

bool ReplicatedMergeTreeAltersSequence::canExecuteDataAlter(int alter_version, std::unique_lock<SharedMutex> & /*state_lock*/) const
{
    /// Queue maybe empty when we start after server shutdown
    /// and have some MUTATE_PART records in queue
    if (queue_state.empty())
        return true;

    /// All versions smaller than head, can be executed
    if (alter_version < queue_state.begin()->first)
        return true;

    return queue_state.at(alter_version).metadata_finished;
}

bool ReplicatedMergeTreeAltersSequence::canExecuteMetaAlter(int alter_version, std::unique_lock<SharedMutex> & /*state_lock*/) const
{
    /// The sequence is empty when the alter is already finished, which happens when the queue holds several
    /// `ALTER_METADATA` entries with the same version (see `finishMetadataAlter`). Executing such an entry is a no-op,
    /// so let it through instead of postponing it forever.
    if (queue_state.empty())
        return true;

    /// All versions smaller than head are finished already, they can be executed
    if (alter_version < queue_state.begin()->first)
        return true;

    /// We can execute only alters of metadata which are in head.
    return queue_state.begin()->first == alter_version;
}
}
