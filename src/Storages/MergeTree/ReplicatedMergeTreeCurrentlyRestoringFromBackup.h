#pragma once

#include <Core/Types.h>
#include <base/scope_guard.h>


namespace Poco { class Logger; }

namespace Coordination
{
    struct WatchResponse;
    using WatchCallback = std::function<void(const WatchResponse &)>;
}


namespace DB
{
class StorageReplicatedMergeTree;
struct MergeTreePartInfo;
struct MutationInfoFromBackup;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class ZooKeeperWithFaultInjection;
using ZooKeeperWithFaultInjectionPtr = std::shared_ptr<ZooKeeperWithFaultInjection>;

/// Allocates block numbers and protects currently restoring parts from merges and mutations until the restore process is done.
class ReplicatedMergeTreeCurrentlyRestoringFromBackup
{
public:
    ReplicatedMergeTreeCurrentlyRestoringFromBackup(StorageReplicatedMergeTree & storage_);
    ~ReplicatedMergeTreeCurrentlyRestoringFromBackup();

    /// Whether this part is being restored from a backup?
    bool containsPart(const MergeTreePartInfo & part_info) const;

    /// Whether this mutation is being restored from a backup?
    bool containsMutation(const String & mutation_name) const;

    /// Allocates block numbers to restore a ReplicatedMergeTree table.
    /// The function also corrects block numbers in specified part infos and mutation infos.
    /// A returned `scope_guard` must be kept to protect currently restoring parts from merges and mutations.
    /// `zookeeper_path_for_checking_` is set to some path which can be used to check later that this allocation is still valid.
    scope_guard allocateBlockNumbers(
        std::vector<MergeTreePartInfo> & part_infos_,
        std::vector<MutationInfoFromBackup> & mutation_infos_,
        bool check_table_is_empty_,
        String & zookeeper_path_for_checking_,
        const ContextPtr & context_);

    /// Rereads information about currently restoring parts from ZooKeeper.
    /// Unlike allocateBlockNumbers(), this function doesn't use retries: if it can't connect to ZooKeeper it just throws an exception.
    void update(const ZooKeeperWithFaultInjectionPtr & zookeeper, Coordination::WatchCallback watch_callback = {});

private:
    class BlockNumbersAllocator;
    class CurrentlyRestoringInfo;

    StorageReplicatedMergeTree & storage;
    Poco::Logger * const log;
    std::unique_ptr<CurrentlyRestoringInfo> currently_restoring_info;
    std::unique_ptr<BlockNumbersAllocator> block_numbers_allocator;
};

/// Used as a prefix for zookeeper nodes created in the "mutations" folder to allocate mutation numbers.
const constexpr std::string_view kMutationPlaceholderPrefix = "mutation-placeholder-";

}
