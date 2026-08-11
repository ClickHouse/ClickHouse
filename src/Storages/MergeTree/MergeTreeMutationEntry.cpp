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
#include <Interpreters/TransactionLog.h>
#include <Parsers/ASTAlterQuery.h>
#include <Backups/BackupEntryFromMemory.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
}

namespace
{

/// The commands in the order in which `MutationCommands::writeText` serializes them here
/// (pure metadata commands are not serialized, see `MutationCommands::ast`). `readText` parses
/// exactly these commands back, so this order is used to associate the per-command partition
/// ids persisted next to the commands with them.
std::vector<const MutationCommand *> commandsInTextForm(const MutationCommands & commands)
{
    std::vector<const MutationCommand *> result;
    result.reserve(commands.size());
    for (const auto & command : commands)
        if (!command.isPureMetadataCommand() && command.ast())
            result.push_back(&command);
    return result;
}

/// Persist the resolved partition scope of the commands so that neither loading the mutation
/// nor executing it has to resolve the `IN PARTITION` literal through the current table
/// metadata again. Resolving it again can throw after a safe partition key type change
/// (e.g. `Enum8 -> Int8`), making an otherwise valid pending mutation block table loading or
/// fail during execution.
/// There is one (possibly empty, for a command without `IN PARTITION`) partition id per
/// command serialized by `MutationCommands::writeText`, in the same order: `writeText` skips
/// pure metadata commands, so they are skipped here too (see `MutationCommands::ast`).
void writePartitionIdsOfCommands(const MutationCommands & commands, WriteBuffer & out)
{
    auto persisted_commands = commandsInTextForm(commands);
    out << "partition ids: " << persisted_commands.size();
    for (const auto * command : persisted_commands)
    {
        out << " ";
        writeQuotedString(command->resolved_partition_id.value_or(""), out);
    }
    out << "\n";
}

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
        writePartitionIdsOfCommands(*commands, *out);
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
    DiskPtr disk_, const String & path_prefix_, const String & file_name_, StorageMergeTree * storage_, ContextPtr context_)
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

    /// `partition ids` is an optional line written after the commands (see the writing constructor above).
    /// It holds one partition id per command, in the same order (an empty string for a command that is
    /// not partition-scoped). Older mutation files (written before this line was introduced) do not
    /// contain it; for those we fall back to resolving the affected partitions from the current table
    /// metadata below. The `tid` and `csn` lines never start with 'p', so `checkString` cannot partially
    /// consume them for old-format files.
    bool partition_ids_loaded = false;
    if (!buf->eof() && checkString("partition ids: ", *buf))
    {
        size_t num_partition_ids = 0;
        readIntText(num_partition_ids, *buf);
        if (num_partition_ids != commands->size())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Mutation file {} contains partition ids of {} commands, while {} commands were read from it",
                file_name, num_partition_ids, commands->size());

        bool all_commands_are_partition_scoped = num_partition_ids != 0;
        partition_ids.reserve(num_partition_ids);
        for (size_t i = 0; i < num_partition_ids; ++i)
        {
            assertChar(' ', *buf);
            String partition_id;
            readQuotedString(partition_id, *buf);
            if (partition_id.empty())
            {
                all_commands_are_partition_scoped = false;
                continue;
            }
            (*commands)[i].resolved_partition_id = partition_id;
            partition_ids.insert(partition_id);
        }

        /// A single command without `IN PARTITION` makes the whole mutation global.
        if (!all_commands_are_partition_scoped)
            partition_ids.clear();

        *buf >> "\n";
        partition_ids_loaded = true;
    }

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

    if (!partition_ids_loaded)
    {
        partition_ids = storage_->resolvePartitionIdsForCommands(*commands, context_);

        /// The resolution above decoded the `IN PARTITION` literals of the commands through
        /// the current table metadata. That is only guaranteed to work while the partition
        /// key stays the same as when the mutation was created, so the file has to be
        /// upgraded to persist the resolved scope (see `upgradeFileWithResolvedPartitionScope`).
        /// Files whose commands are not partition-scoped decode nothing and need no upgrade.
        for (const auto & command : *commands)
            if (command.resolved_partition_id)
                needs_file_upgrade = true;
    }
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
    writePartitionIdsOfCommands(*commands, *out);
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

    /// Keep the resolved partition scope in the backup so it is not lost with the mutation file.
    writePartitionIdsOfCommands(*commands, out);

    return std::make_shared<BackupEntryFromMemory>(out.str());
}

}
