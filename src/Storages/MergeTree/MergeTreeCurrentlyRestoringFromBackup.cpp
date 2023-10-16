#include <Storages/MergeTree/MergeTreeCurrentlyRestoringFromBackup.h>

#include <Storages/MergeTree/MutationInfoFromBackup.h>
#include <Storages/MergeTree/calculateBlockNumbersForRestoring.h>
#include <Storages/StorageMergeTree.h>
#include <base/defines.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
}


namespace
{
    /// There can be various reasons to check that no parts exist.
    enum class NoPartsCheckReason
    {
        /// No need to check for no parts.
        NONE,

        /// Non-empty tables are not allowed because of the restore command's settings: `SETTINGS allow_non_empty_tables = false`.
        NON_EMPTY_TABLE_IS_NOT_ALLOWED,

        /// We going to restore mutations, and mutations from a backup should never be applied to existing parts.
        RESTORING_MUTATIONS,
    };
}


// Increases the block number increment to allocate block numbers.
class MergeTreeCurrentlyRestoringFromBackup::BlockNumbersAllocator
{
public:
    explicit BlockNumbersAllocator(
        StorageMergeTree & storage_, Poco::Logger * log_, const CurrentlyRestoringInfo & currently_restoring_info_)
        : storage(storage_), log(log_), currently_restoring_info(currently_restoring_info_)
    {
    }

    /// Allocates block numbers and recalculates `part_infos_` and `mutation_infos_`.
    void allocateBlockNumbers(
        std::vector<MergeTreePartInfo> & part_infos_,
        std::vector<MutationInfoFromBackup> & mutation_infos_,
        NoPartsCheckReason no_parts_check_reason_) const
    {
        Int64 base_increment_value = getCurrentIncrementValue();
        std::optional<AllocatedBlockNumbers> allocated_block_numbers;

        /// Check the table has no existing parts if it's necessary.
        /// If a backup contains mutations the table must have no existing parts,
        /// otherwise mutations from the backup could be applied to existing parts which is wrong.
        if (no_parts_check_reason_ != NoPartsCheckReason::NONE)
            checkNoPartsExist(base_increment_value, allocated_block_numbers, no_parts_check_reason_);

        LOG_INFO(log, "Increasing the increment to allocate block numbers for {} parts and {} mutations",
                 part_infos_.size(), mutation_infos_.size());

        /// This `do_allocate*` function will be called from calculateBlockNumbersForRestoringMergeTree() below.
        auto do_allocate_block_numbers = [&](size_t count)
        {
            auto lock = storage.lockParts();
            Int64 first = storage.increment.getMany(count);
            allocated_block_numbers.emplace(AllocatedBlockNumbers{.first = first, .count = count});
            return first;
        };

        /// Recalculate block numbers in `part_infos` and `mutation_infos`.
        calculateBlockNumbersForRestoringMergeTree(part_infos_, mutation_infos_, do_allocate_block_numbers);

        /// Check the table has no existing parts again to be sure no parts were added while we were allocating block numbers.
        if (no_parts_check_reason_ != NoPartsCheckReason::NONE)
            checkNoPartsExist(base_increment_value, allocated_block_numbers, no_parts_check_reason_);

        LOG_INFO(log, "The increment was incremented by {} to allocate block numbers",
                 allocated_block_numbers ? allocated_block_numbers->count : 0);
    }

private:
    StorageMergeTree & storage;
    Poco::Logger * log;
    const CurrentlyRestoringInfo & currently_restoring_info;

    String getTableName() const { return storage.getStorageID().getFullTableName(); }

    struct AllocatedBlockNumbers
    {
        Int64 first = 0;
        size_t count = 0;
    };

    /// Checks that there no parts exist in the table.
    /// The function also checks that no parts will be inserted / attached soon due to a concurrent process.
    void checkNoPartsExist(Int64 base_increment_value, const std::optional<AllocatedBlockNumbers> & ignore_block_numbers, NoPartsCheckReason reason) const
    {
        if (reason == NoPartsCheckReason::NONE)
            return;

        LOG_INFO(log, "Checking that no parts exist in the table before restoring its data (reason: {})", magic_enum::enum_name(reason));

        /// The order of checks is important:
        /// an INSERT command first allocates a block number, then it attaches it to the table.
        /// That's why it's more reliable to check first block numbers, then data parts.

        auto allocated_block_number = findAnyAllocatedBlockNumber(base_increment_value, ignore_block_numbers);

        auto part_name_in_storage = findAnyPartInStorage();
        if (part_name_in_storage)
            throwTableIsNotEmpty(reason, fmt::format("part {} exists", *part_name_in_storage));

        auto restoring_part_name = findAnyRestoringPart();
        if (restoring_part_name)
            throwTableIsNotEmpty(reason, fmt::format("concurrent RESTORE is creating part {}", *restoring_part_name));

        if (allocated_block_number)
            throwTableIsNotEmpty(reason, fmt::format("concurrent INSERT or ATTACH is using block number {}", *allocated_block_number));
    }

    /// Finds any part on the current replica.
    std::optional<String> findAnyPartInStorage() const
    {
        if (storage.getTotalActiveSizeInBytes() == 0)
            return {};

        auto parts_lock = storage.lockParts();

        auto parts = storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active, MergeTreeDataPartState::PreActive}, parts_lock);
        if (parts.empty())
            return {};

        return parts.front()->name;
    }

    Int64 getCurrentIncrementValue() const
    {
        return storage.increment.value.load();
    }

    /// Finds any locked block number excluding ones we have just allocated.
    std::optional<Int64> findAnyAllocatedBlockNumber(Int64 base_increment_value, const std::optional<AllocatedBlockNumbers> & ignore_block_numbers) const
    {
        Int64 base = base_increment_value;
        Int64 current = getCurrentIncrementValue();

        if (!ignore_block_numbers)
        {
            if (current < base)
            {
                throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "The block number increment changed in a unexpected way: base={}, current={}",
                                base, current);
            }

            if (current > base)
                return base + 1;
            return {};
        }

        Int64 ignore_first = ignore_block_numbers->first;
        Int64 ignore_count = ignore_block_numbers->count;

        if ((ignore_first < base + 1) || (ignore_first + ignore_count < ignore_first) || (current < ignore_first + ignore_count - 1))
        {
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                            "The block number increment changed in a unexpected way: base={}, ignore_first={}, ignore_count={}, current={}",
                            base, ignore_first, ignore_count, current);
        }

        if (ignore_first > base + 1)
            return base + 1;

        if (current >= ignore_first + ignore_count)
            return ignore_first + ignore_count;

        return {};
    }

    /// Finds any part which is already restoring by a concurrent RESTORE.
    std::optional<String> findAnyRestoringPart() const;

    /// Provides an extra description for error messages to make them more detailed.
    [[noreturn]] void throwTableIsNotEmpty(NoPartsCheckReason reason, const String & details) const
    {
        String message = fmt::format("Cannot restore the table {} because it already contains some data{}.",
                                     getTableName(), details.empty() ? "" : (" (" + details + ")"));
        if (reason == NoPartsCheckReason::NON_EMPTY_TABLE_IS_NOT_ALLOWED)
            message += "You can either truncate the table before restoring OR set \"allow_non_empty_tables=true\" to allow appending to "
                         "existing data in the table";
        else if (reason == NoPartsCheckReason::RESTORING_MUTATIONS)
            message += "Mutations cannot be restored if a table is non-empty already. You can either truncate the table before "
                         "restoring OR set \"restore_mutations=false\" to restore the table without restoring its mutations";
        throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "{}", message);
    }
};


/// Keeps information about currently restoring parts and mutations.
class MergeTreeCurrentlyRestoringFromBackup::CurrentlyRestoringInfo
{
public:
    explicit CurrentlyRestoringInfo(StorageMergeTree & storage_, Poco::Logger * log_) : storage(storage_), log(log_) { }

    /// Whether this part is being restored from a backup?
    bool containsPart(const MergeTreePartInfo & part_info) const
    {
        std::lock_guard lock{mutex};
        for (const auto & entry : entries)
        {
            if (entry->part_infos.contains(part_info))
                return true;
        }
        return false;
    }

    /// Finds any part which is already restoring.
    std::optional<String> findAnyRestoringPart() const
    {
        std::lock_guard lock{mutex};
        for (const auto & entry : entries)
        {
            if (!entry->part_infos.empty())
                return entry->part_infos.begin()->getPartNameAndCheckFormat(storage.format_version);
        }
        return {};
    }

    /// Whether this mutation is being restored from a backup.
    bool containsMutation(Int64 mutation_number) const
    {
        std::lock_guard lock{mutex};
        for (const auto & entry : entries)
        {
            if (entry->mutation_numbers.contains(mutation_number))
                return true;
        }
        return false;
    }

    /// Stores information about parts and mutations we're going to restore.
    /// A returned `scope_guard` is used to control how long this information should be kept.
    scope_guard addEntry(const std::vector<MergeTreePartInfo> & part_infos_, const std::vector<MutationInfoFromBackup> & mutation_infos_)
    {
        /// Make an entry to keep information about parts and mutations being restored.
        auto entry = std::make_unique<Entry>();
        auto * entry_rawptr = entry.get();

        std::copy(part_infos_.begin(), part_infos_.end(), std::inserter(entry->part_infos, entry->part_infos.end()));

        for (const auto & mutation_info : mutation_infos_)
            entry->mutation_numbers.insert(mutation_info.number);

        /// This `scope_guard` is used to remove the entry when we're done with the current RESTORE process.
        scope_guard remove_entry = [this, entry_rawptr]
        {
            LOG_INFO(log, "Removing info about currently restoring parts");
            std::lock_guard lock{mutex};
            auto it = entries.find(entry_rawptr);
            if (it != entries.end())
                entries.erase(it);
        };

        {
            /// Store the entry.
            LOG_INFO(log, "Storing info about currently restoring parts");
            std::lock_guard lock{mutex};
            entries.emplace(std::move(entry));
        }

        return remove_entry;
    }

private:
    StorageMergeTree & storage;
    Poco::Logger * const log;

    /// Represents parts and mutations restored by one RESTORE command to this table.
    struct Entry
    {
        std::set<MergeTreePartInfo> part_infos;
        std::unordered_set<Int64> mutation_numbers;
    };

    struct EntryPtrCompare
    {
        using is_transparent = void;
        template <class P, class Q>
        bool operator()(const P & left, const Q & right) const
        {
            return std::to_address(left) < std::to_address(right);
        }
    };

    std::set<std::unique_ptr<Entry>, EntryPtrCompare> entries TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};


std::optional<String> MergeTreeCurrentlyRestoringFromBackup::BlockNumbersAllocator::findAnyRestoringPart() const
{
    return currently_restoring_info.findAnyRestoringPart();
}


MergeTreeCurrentlyRestoringFromBackup::MergeTreeCurrentlyRestoringFromBackup(StorageMergeTree & storage_)
    : log(&Poco::Logger::get(storage_.getLogName() + " (RestorerFromBackup)"))
    , currently_restoring_info(std::make_unique<CurrentlyRestoringInfo>(storage_, log))
    , block_numbers_allocator(std::make_unique<BlockNumbersAllocator>(storage_, log, *currently_restoring_info))
{
}

MergeTreeCurrentlyRestoringFromBackup::~MergeTreeCurrentlyRestoringFromBackup() = default;

bool MergeTreeCurrentlyRestoringFromBackup::containsPart(const MergeTreePartInfo & part_info) const
{
    return currently_restoring_info->containsPart(part_info);
}

bool MergeTreeCurrentlyRestoringFromBackup::containsMutation(Int64 mutation_number) const
{
    return currently_restoring_info->containsMutation(mutation_number);
}

scope_guard MergeTreeCurrentlyRestoringFromBackup::allocateBlockNumbers(
    std::vector<MergeTreePartInfo> & part_infos_,
    std::vector<MutationInfoFromBackup> & mutation_infos_,
    bool check_no_parts_before_)
{
    auto no_parts_check_reason = NoPartsCheckReason::NONE;
    if (check_no_parts_before_)
        no_parts_check_reason = NoPartsCheckReason::NON_EMPTY_TABLE_IS_NOT_ALLOWED;
    else if (!mutation_infos_.empty())
        no_parts_check_reason = NoPartsCheckReason::RESTORING_MUTATIONS;

    /// Create temporary zookeeper nodes to allocate block numbers and mutation numbers.
    block_numbers_allocator->allocateBlockNumbers(part_infos_, mutation_infos_, no_parts_check_reason);

    /// Store information about parts and mutations we're going to restore in memory and ZooKeeper.
    return currently_restoring_info->addEntry(part_infos_, mutation_infos_);
}

}
