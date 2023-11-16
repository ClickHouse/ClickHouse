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

    /// Whether any parts are being restored from a backup?
    bool containsAnyParts() const;

    /// Whether this mutation is being restored from a backup.
    bool containsMutation(Int64 mutation_number) const;

    /// Allocates block numbers to restore a MergeTree table.
    scope_guard allocateBlockNumbers(
        std::vector<MergeTreePartInfo> & part_infos_, std::vector<MutationInfoFromBackup> & mutation_infos_, bool check_table_is_empty_);

    /// There can be various reasons to check that no parts exist.
    enum class CheckForNoPartsReason
    {
        /// No need to check that no parts exist.
        NONE,

        /// Non-empty tables are not allowed because of the restore command's settings: `SETTINGS allow_non_empty_tables = false`.
        NON_EMPTY_TABLE_IS_NOT_ALLOWED,

        /// We going to restore mutations, and mutations from a backup should never be applied to existing parts.
        RESTORING_MUTATIONS,
    };

    static CheckForNoPartsReason getCheckForNoPartsReason(bool check_table_is_empty_, bool has_mutations_to_restore_);

private:
    class BlockNumbersAllocator;
    class CurrentlyRestoringInfo;

    Poco::Logger * const log;
    std::unique_ptr<CurrentlyRestoringInfo> currently_restoring_info;
    std::unique_ptr<BlockNumbersAllocator> block_numbers_allocator;
};

}
