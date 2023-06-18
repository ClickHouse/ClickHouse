#pragma once

#include <Core/Types.h>
#include <base/scope_guard.h>


namespace Poco { class Logger; }

namespace DB
{
class StorageMergeTree;
struct MergeTreePartInfo;
struct MutationInfoFromBackup;

/// Allocates block numbers and protects currently restoring parts from merges and mutations until the restore process is done.
class MergeTreeCurrentlyRestoringFromBackup
{
public:
    MergeTreeCurrentlyRestoringFromBackup(StorageMergeTree & storage_);
    ~MergeTreeCurrentlyRestoringFromBackup();

    /// Whether this part is being restored from a backup?
    bool containsPart(const MergeTreePartInfo & part_info) const;

    /// Whether this mutation is being restored from a backup.
    bool containsMutation(Int64 mutation_number) const;

    /// Allocates block numbers to restore a MergeTree table.
    scope_guard allocateBlockNumbers(
        std::vector<MergeTreePartInfo> & part_infos_,
        Strings & part_names_in_backup_,
        std::vector<MutationInfoFromBackup> & mutation_infos_,
        Strings & mutation_names_in_backup_,
        bool check_no_parts_before_);

private:
    class BlockNumbersAllocator;
    class CurrentlyRestoringInfo;

    Poco::Logger * const log;
    std::unique_ptr<CurrentlyRestoringInfo> currently_restoring_info;
    std::unique_ptr<BlockNumbersAllocator> block_numbers_allocator;
};

}
