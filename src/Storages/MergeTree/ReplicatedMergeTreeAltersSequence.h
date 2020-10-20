#pragma once

#include <deque>
#include <mutex>
#include <map>

namespace DB
{
/// ALTERs in StorageReplicatedMergeTree have to be executed sequentially (one
/// by one). But ReplicatedMergeTreeQueue execute all entries almost
/// concurrently. The only dependency between entries is data parts, but they are
/// not suitable in alters case.
///
/// This class stores information about current alters in
/// ReplicatedMergeTreeQueue, and control their order of execution. Actually
/// it's a part of ReplicatedMergeTreeQueue and shouldn't be used directly by
/// other classes, also methods have to be called under ReplicatedMergeTreeQueue
/// state lock.
class ReplicatedMergeTreeAltersSequence
{
private:
    /// In general case alter consist of two stages Alter data and alter
    /// metadata. First we alter storage metadata and then we can apply
    /// corresponding data changes (MUTATE_PART). After that, we can remove
    /// alter from this sequence (alter is processed).
    struct AlterState
    {
        bool metadata_finished = false;
        bool data_finished = false;
    };

private:
    /// alter_version -> AlterState.
    std::map<int, AlterState> queue_state;

public:

    /// Add mutation for alter (alter data stage).
    void addMutationForAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/);

    /// Add metadata for alter (alter metadata stage). If have_mutation=true, than we expect, that
    /// corresponding mutation will be added.
    void addMetadataAlter(int alter_version, bool have_mutation, std::lock_guard<std::mutex> & /*state_lock*/);

    /// Finish metadata alter. If corresponding data alter finished, than we can remove
    /// alter from sequence.
    void finishMetadataAlter(int alter_version, std::unique_lock <std::mutex> & /*state_lock*/);

    /// Finish data alter. If corresponding metadata alter finished, than we can remove
    /// alter from sequence.
    void finishDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/);

    /// Check that we can execute this data alter. If it's metadata stage finished.
    bool canExecuteDataAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const;

    /// Check that we can execute metadata alter with version.
    bool canExecuteMetaAlter(int alter_version, std::lock_guard<std::mutex> & /*state_lock*/) const;

    /// Just returns smallest alter version in sequence (first entry)
    int getHeadAlterVersion(std::lock_guard<std::mutex> & /*state_lock*/) const;
};

}
