#include <gtest/gtest.h>

#include <Storages/MergeTree/ReplicatedMergeTreeAltersSequence.h>

using namespace DB;

namespace
{

/// `ReplicatedMergeTreeAltersSequence` expects its caller to hold the queue state lock. The locks are
/// not used for anything else, so a private mutex is enough to drive the sequence from a test.
class SequenceUnderLock
{
public:
    void addMutationForAlter(int alter_version)
    {
        std::lock_guard lock(mutex);
        sequence.addMutationForAlter(alter_version, lock);
    }

    void addMetadataAlter(int alter_version)
    {
        std::lock_guard lock(mutex);
        sequence.addMetadataAlter(alter_version, lock);
    }

    void finishMetadataAlter(int alter_version)
    {
        std::unique_lock lock(mutex);
        sequence.finishMetadataAlter(alter_version, lock);
    }

    void finishDataAlter(int alter_version)
    {
        std::lock_guard lock(mutex);
        sequence.finishDataAlter(alter_version, lock);
    }

    bool canExecuteMetaAlter(int alter_version)
    {
        std::unique_lock lock(mutex);
        return sequence.canExecuteMetaAlter(alter_version, lock);
    }

    bool canExecuteDataAlter(int alter_version)
    {
        std::unique_lock lock(mutex);
        return sequence.canExecuteDataAlter(alter_version, lock);
    }

    int getHeadAlterVersion()
    {
        std::unique_lock lock(mutex);
        return sequence.getHeadAlterVersion(lock);
    }

private:
    SharedMutex mutex;
    ReplicatedMergeTreeAltersSequence sequence;
};

}

TEST(ReplicatedMergeTreeAltersSequence, AltersAreExecutedInOrder)
{
    SequenceUnderLock sequence;

    sequence.addMutationForAlter(1);
    sequence.addMetadataAlter(1);
    sequence.addMutationForAlter(2);
    sequence.addMetadataAlter(2);

    ASSERT_EQ(sequence.getHeadAlterVersion(), 1);
    ASSERT_TRUE(sequence.canExecuteMetaAlter(1));
    ASSERT_FALSE(sequence.canExecuteMetaAlter(2));
    /// The data alter has to wait for the metadata of the same version.
    ASSERT_FALSE(sequence.canExecuteDataAlter(1));

    sequence.finishMetadataAlter(1);
    ASSERT_TRUE(sequence.canExecuteDataAlter(1));
    ASSERT_FALSE(sequence.canExecuteMetaAlter(2));

    sequence.finishDataAlter(1);
    ASSERT_EQ(sequence.getHeadAlterVersion(), 2);
    ASSERT_TRUE(sequence.canExecuteMetaAlter(2));
}

/// A replica created by cloning another one ends up with two `ALTER_METADATA` entries of the same
/// version in its queue: the dummy one prepended by `StorageReplicatedMergeTree::cloneMetadataIfNeeded`
/// and the one copied from the source replica's queue. Both of them finish the same alter, and that
/// used to leave the finished alter in the sequence forever, blocking every later metadata alter --
/// the replication queue got stuck and `SYSTEM SYNC REPLICA` hung until `receive_timeout`.
TEST(ReplicatedMergeTreeAltersSequence, DuplicateMetadataAlterVersionDoesNotBlockLaterAlters)
{
    SequenceUnderLock sequence;

    sequence.addMetadataAlter(7);
    sequence.addMetadataAlter(7);

    /// The next alter is pulled from the log while both entries are still in the queue.
    sequence.addMutationForAlter(8);
    sequence.addMetadataAlter(8);

    /// Both entries of version 7 are picked up by the background pool before either of them completes.
    ASSERT_TRUE(sequence.canExecuteMetaAlter(7));
    sequence.finishMetadataAlter(7);
    sequence.finishMetadataAlter(7);

    /// Version 7 is done, so version 8 is at the head and can be executed.
    ASSERT_EQ(sequence.getHeadAlterVersion(), 8);
    ASSERT_TRUE(sequence.canExecuteMetaAlter(8));

    sequence.finishDataAlter(8);
    sequence.finishMetadataAlter(8);

    /// And so can the alter after it.
    sequence.addMutationForAlter(9);
    sequence.addMetadataAlter(9);
    ASSERT_EQ(sequence.getHeadAlterVersion(), 9);
    ASSERT_TRUE(sequence.canExecuteMetaAlter(9));
}

/// The same two entries, but the second one is checked only after the first one has completed.
TEST(ReplicatedMergeTreeAltersSequence, DuplicateMetadataAlterVersionIsExecutableAfterTheFirstOne)
{
    SequenceUnderLock sequence;

    sequence.addMetadataAlter(7);
    sequence.addMetadataAlter(7);
    sequence.addMutationForAlter(8);
    sequence.addMetadataAlter(8);

    sequence.finishMetadataAlter(7);

    /// Executing the second entry of version 7 is a no-op, but it still has to be executed to leave the queue.
    ASSERT_TRUE(sequence.canExecuteMetaAlter(7));
    sequence.finishMetadataAlter(7);

    ASSERT_EQ(sequence.getHeadAlterVersion(), 8);
    ASSERT_TRUE(sequence.canExecuteMetaAlter(8));
}

/// The duplicated alter is the only one in the queue, so the sequence becomes empty in the middle.
TEST(ReplicatedMergeTreeAltersSequence, DuplicateMetadataAlterVersionAsTheOnlyEntry)
{
    SequenceUnderLock sequence;

    sequence.addMetadataAlter(7);
    sequence.addMetadataAlter(7);

    sequence.finishMetadataAlter(7);
    ASSERT_EQ(sequence.getHeadAlterVersion(), -1);

    ASSERT_TRUE(sequence.canExecuteMetaAlter(7));
    sequence.finishMetadataAlter(7);
    ASSERT_EQ(sequence.getHeadAlterVersion(), -1);
}
