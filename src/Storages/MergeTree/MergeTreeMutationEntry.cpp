#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/PartitionIds.h>
#include <Common/logger_useful.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/TransactionManager.h>
#include <Parsers/ASTAlterQuery.h>
#include <Backups/BackupEntryFromMemory.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String MergeTreeMutationEntry::versionToFileName(UInt64 block_number_)
{
    chassert(block_number_);
    return fmt::format("mutation_{}.txt", block_number_);
}

UInt64 MergeTreeMutationEntry::tryParseFileName(const String & file_name_)
{
    UInt64 maybe_block_number = 0;
    ReadBufferFromString file_name_buf(file_name_);
    if (!checkString("mutation_", file_name_buf))
        return 0;
    if (!tryReadIntText(maybe_block_number, file_name_buf))
        return 0;
    if (!checkString(".txt", file_name_buf))
        return 0;
    chassert(maybe_block_number);
    return maybe_block_number;
}

UInt64 MergeTreeMutationEntry::parseFileName(const String & file_name_)
{
    if (UInt64 maybe_block_number = tryParseFileName(file_name_))
        return maybe_block_number;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Cannot parse mutation version from file name, expected 'mutation_<UInt64>.txt', got '{}'",
                    file_name_);
}

MergeTreeMutationEntry::MergeTreeMutationEntry(
    MutationCommands commands_,
    DiskPtr disk_,
    const String & path_prefix_,
    UInt64 tmp_number,
    PartitionIds && partition_ids_,
    const TransactionID & tid_,
    const WriteSettings & settings)
    : create_time(time(nullptr))
    , commands(std::make_shared<MutationCommands>(std::move(commands_)))
    , disk(std::move(disk_))
    , path_prefix(path_prefix_)
    , file_name("tmp_mutation_" + toString(tmp_number) + ".txt")
    , is_temp(true)
    , partition_ids(std::move(partition_ids_))
    , tid(tid_)
{
    try
    {
        auto out = disk->writeFile(std::filesystem::path(path_prefix) / file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, settings);
        *out << "format version: 1\n"
            << "create time: " << LocalDateTime(create_time, DateLUT::serverTimezoneInstance()) << "\n";
        *out << "commands: ";
        commands->writeText(*out, /* with_pure_metadata_commands = */ false);
        *out << "\n";
        if (tid.isNonTransactional())
        {
            csn = Tx::NonTransactionalCSN;
        }
        else
        {
            *out << "tid: ";
            TransactionID::write(tid, *out);
            *out << "\n";
        }
        out->finalize();
        out->sync();
    }
    catch (...)
    {
        removeFile();
        throw;
    }
}

void MergeTreeMutationEntry::commit(UInt64 block_number_)
{
    chassert(block_number_);
    block_number = block_number_;
    String new_file_name = versionToFileName(block_number);
    disk->moveFile(path_prefix + file_name, path_prefix + new_file_name);
    is_temp = false;
    file_name = new_file_name;
}

void MergeTreeMutationEntry::removeFile()
{
    if (!file_name.empty())
    {
        if (!disk->existsFile(path_prefix + file_name))
            return;

        disk->removeFileIfExists(path_prefix + file_name);
        file_name.clear();
    }
}

void MergeTreeMutationEntry::writeCSN(CSN csn_)
{
    csn = csn_;
    auto out = disk->writeFile(path_prefix + file_name, 256, WriteMode::Append);
    *out << "csn: " << csn << "\n";
    out->finalize();
}

MergeTreeMutationEntry::MergeTreeMutationEntry(
    DiskPtr disk_, const String & path_prefix_, const String & file_name_, const StorageMergeTree * storage_, ContextPtr context_)
    : commands(std::make_shared<MutationCommands>())
    , disk(std::move(disk_))
    , path_prefix(path_prefix_)
    , file_name(file_name_)
    , is_temp(false)
    , is_registered(true)
{
    block_number = parseFileName(file_name);
    auto buf = disk->readFile(path_prefix + file_name, getReadSettings());

    *buf >> "format version: 1\n";

    LocalDateTime create_time_dt;
    *buf >> "create time: " >> create_time_dt >> "\n";
    create_time = makeDateTime(DateLUT::serverTimezoneInstance(),
        create_time_dt.year(), create_time_dt.month(), create_time_dt.day(),
        create_time_dt.hour(), create_time_dt.minute(), create_time_dt.second());

    *buf >> "commands: ";
    commands->readText(*buf, false);
    *buf >> "\n";

    if (buf->eof())
    {
        tid = Tx::NonTransactionalTID;
        csn = Tx::NonTransactionalCSN;
    }
    else
    {
        *buf >> "tid: ";
        tid = TransactionID::read(*buf);
        *buf >> "\n";

        if (!buf->eof())
        {
            *buf >> "csn: " >> csn >> "\n";
        }
    }

    assertEOF(*buf);

    /// A file written by a binary that did not pin the partition scope yet still carries the
    /// original `IN PARTITION <value>` literal, which has to be decoded through the current
    /// table metadata. That only works while the partition key is the same as when the mutation
    /// was created, so such a file is upgraded to the pinned `IN PARTITION ID` form right after
    /// loading (see `upgradeFileWithResolvedPartitionScope`); otherwise a later key-safe
    /// partition key type change (e.g. `Enum8 -> Int8`) would leave the table unloadable.
    const bool has_unresolved_partition_scope = MergeTreeData::hasUnresolvedPartitionScope(*commands);

    /// A binary without this feature applied every mutation, `IN PARTITION` or not, to all parts,
    /// so its mutation is finished once no part has a lower data version, and no new part can get
    /// one. Such a mutation is never executed again, and the partition key may have changed since
    /// it finished (e.g. `MODIFY COLUMN p Int8` of an `Enum8` key before the upgrade), so its
    /// literals are left as they are instead of being decoded: the entry stays global and the file
    /// is not rewritten.
    if (has_unresolved_partition_scope && storage_->areAllPartsMutatedTo(static_cast<Int64>(block_number)))
        return;

    if (!has_unresolved_partition_scope)
    {
        partition_ids = storage_->rewritePartitionScopeToIds(*commands, context_);
        return;
    }

    /// A pending legacy mutation may have become undecodable already, if the partition key type
    /// was changed by the previous binary (e.g. `MODIFY COLUMN p Int8` of an `Enum8` key while the
    /// mutation was pending). Failing here would make the whole table unloadable, so the mutation
    /// could not even be killed. Instead, the entry is loaded in the state it had before this
    /// migration existed: global and with the original literals, so its execution fails with the
    /// same error as before, and it is visible in `system.mutations` and can be killed. The scope
    /// is resolved on a copy, so a failure in the middle does not leave the commands half rewritten.
    MutationCommands resolved_commands = *commands;
    try
    {
        partition_ids = storage_->rewritePartitionScopeToIds(resolved_commands, context_);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("MergeTreeMutationEntry"), fmt::format(
            "Cannot resolve the partition scope of the legacy mutation {}{}; "
            "it is loaded with the original literals, and if its execution fails, it has to be killed",
            path_prefix, file_name));
        return;
    }

    *commands = std::move(resolved_commands);
    needs_file_upgrade = true;
}

void MergeTreeMutationEntry::upgradeFileWithResolvedPartitionScope(const WriteSettings & settings)
{
    chassert(needs_file_upgrade);

    /// Write the replacement into a temporary file first: a crash in the middle of a plain
    /// rewrite would corrupt the file and make the table unloadable. Leftover temporary
    /// files are removed by `StorageMergeTree::loadMutations`.
    String tmp_file_name = "tmp_mutation_upgrade_" + toString(block_number) + ".txt";
    auto out = disk->writeFile(std::filesystem::path(path_prefix) / tmp_file_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, settings);
    *out << "format version: 1\n"
        << "create time: " << LocalDateTime(create_time, DateLUT::serverTimezoneInstance()) << "\n";
    *out << "commands: ";
    commands->writeText(*out, /* with_pure_metadata_commands = */ false);
    *out << "\n";
    if (!tid.isNonTransactional())
    {
        *out << "tid: ";
        TransactionID::write(tid, *out);
        *out << "\n";
        if (csn != Tx::UnknownCSN)
            *out << "csn: " << csn << "\n";
    }
    out->finalize();
    out->sync();

    disk->replaceFile(std::filesystem::path(path_prefix) / tmp_file_name, std::filesystem::path(path_prefix) / file_name);
    needs_file_upgrade = false;
}

MergeTreeMutationEntry::MergeTreeMutationEntry(MergeTreeMutationEntry && other) noexcept
    : create_time(other.create_time)
    , commands(std::move(other.commands))
    , disk(std::move(other.disk))
    , path_prefix(std::move(other.path_prefix))
    , file_name(std::exchange(other.file_name, {}))
    , is_temp(std::exchange(other.is_temp, false))
    , is_registered(std::exchange(other.is_registered, false))
    , is_done(other.is_done)
    , block_number(other.block_number)
    , latest_failed_part(std::move(other.latest_failed_part))
    , latest_failed_part_info(std::move(other.latest_failed_part_info))
    , latest_fail_time(other.latest_fail_time)
    , latest_fail_reason(std::move(other.latest_fail_reason))
    , latest_fail_error_code_name(std::move(other.latest_fail_error_code_name))
    , partition_ids(std::move(other.partition_ids))
    , needs_file_upgrade(std::exchange(other.needs_file_upgrade, false))
    , tid(other.tid)
    , csn(other.csn)
{
}

MergeTreeMutationEntry::~MergeTreeMutationEntry()
{
    if (file_name.empty())
        return;

    if (is_temp && startsWith(file_name, "tmp_"))
    {
        try
        {
            removeFile();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        return;
    }

    /// Committed to disk but never registered in `current_mutations_by_version`.
    /// Remove the orphaned `mutation_*.txt` so it is not replayed on restart.
    /// Mirrors the visibility of `killMutation` and `clearOldMutations`, which
    /// log permanent mutation-file removals from their callers. See #80648.
    if (!is_temp && !is_registered)
    {
        try
        {
            LOG_INFO(getLogger("MergeTreeMutationEntry"),
                "Removing orphaned mutation file {} (block number {}); registration was not completed",
                path_prefix + file_name, block_number);
            removeFile();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

std::shared_ptr<const IBackupEntry> MergeTreeMutationEntry::backup() const
{
    WriteBufferFromOwnString out;
    out << "block number: " << block_number << "\n";

    out << "commands: ";
    commands->writeText(out, /* with_pure_metadata_commands = */ false);
    out << "\n";

    return std::make_shared<BackupEntryFromMemory>(out.str());
}

}
