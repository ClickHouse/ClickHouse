#include <Interpreters/DDLTask.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMutationEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumAddedParts.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/ZooKeeper/IKeeper.h>


/// The entries that the replicas of a table, and the hosts of a cluster, read from Keeper: written by
/// another replica or host, or by anyone who can write to Keeper. The first byte selects the entry, the
/// second the format version of the part names; the rest is the entry. An entry that is read is written
/// again and read back, because a replica that copies an entry from the log must read what it wrote.

using namespace DB;

namespace
{

/// Writing the entry read from `text` must give text that reads back to the same entry.
template <typename Parse>
void checkRoundTrip(const String & text, Parse && parse)
{
    String written = parse(text);
    String written_again;
    try
    {
        written_again = parse(written);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: the values of a fuzzed entry may be ones that no server writes.
        return;
    }
    if (written != written_again)
        __builtin_trap();
}

}

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    MainThreadStatus::getInstance();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    if (size < 2)
        return 0;

    const String text(reinterpret_cast<const char *>(data + 2), size - 2);
    const MergeTreeDataFormatVersion format_version
        = (data[1] & 1) ? MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING : MERGE_TREE_DATA_OLD_FORMAT_VERSION;

    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        switch (data[0] % 7)
        {
            case 0:
                /// `log/log-*` and `replicas/*/queue/queue-*` of a replicated table.
                checkRoundTrip(text, [&](const String & s)
                {
                    auto entry = ReplicatedMergeTreeLogEntry::parse(s, Coordination::Stat{}, format_version);
                    /// What `ReplicatedMergeTreeQueue::insertUnlocked` and the queue do with a new entry.
                    std::ignore = entry->getVirtualPartNames(format_version);
                    std::ignore = entry->getDropRange(format_version);
                    std::ignore = entry->isDropPart(format_version);
                    return entry->toString();
                });
                break;
            case 1:
                /// `mutations/*` of a replicated table.
                checkRoundTrip(text, [](const String & s) { return ReplicatedMergeTreeMutationEntry::parse(s, "0000000000").toString(); });
                break;
            case 2:
                /// `task_queue/ddl/query-*` of `ON CLUSTER` queries and `log/query-*` of a `Replicated` database.
                checkRoundTrip(text, [](const String & s)
                {
                    DDLLogEntry entry;
                    entry.parse(s);
                    return entry.toString();
                });
                break;
            case 3:
                /// `quorum/status` of an insert with `insert_quorum`.
                checkRoundTrip(text, [](const String & s) { return ReplicatedMergeTreeQuorumEntry(s).toString(); });
                break;
            case 4:
                /// `replicas/*/host`: where another replica sends the parts it is asked for.
                checkRoundTrip(text, [](const String & s) { return ReplicatedMergeTreeAddress(s).toString(); });
                break;
            case 5:
            {
                /// `quorum/last_part` of the partitions with a quorum insert. Not written again: the parts are an unordered
                /// map, which can write them in another order, and the reader unescapes what the writer does not escape.
                ReplicatedMergeTreeQuorumAddedParts parts(format_version);
                parts.fromString(text);
                std::ignore = parts.getMaxInsertedBlocks();
                break;
            }
            case 6:
                /// `metadata` of a replicated table, compared with the local one when a replica starts.
                checkRoundTrip(text, [](const String & s) { return ReplicatedMergeTreeTableMetadata::parseRaw(s).toString(); });
                break;
            default:
                break;
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed entries are rejected with exceptions.
    }

    return 0;
}
