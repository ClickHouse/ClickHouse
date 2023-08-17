#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <Storages/StorageMergeTree.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <Interpreters/TransactionLog.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Parsers/ASTPartition.h>
#include <Interpreters/Context.h>

#include <Common/logger_useful.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String MergeTreeMutationEntry::versionToFileName(UInt64 block_number_)
{
    assert(block_number_);
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
    assert(maybe_block_number);
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
    const WriteSettings & settings
)
    : create_time(time(nullptr))
    , commands(std::move(commands_))
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
        commands.writeText(*out, /* with_pure_metadata_commands = */ false);
        *out << "\n";
        if (tid.isPrehistoric())
        {
            csn = Tx::PrehistoricCSN;
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
    assert(block_number_);
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
        if (!disk->exists(path_prefix + file_name))
            return;

        disk->removeFile(path_prefix + file_name);
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
    DiskPtr disk_,
    const String & path_prefix_,
    const String & file_name_,
    StorageMergeTree * storage_,
    ContextPtr context_)
    : disk(std::move(disk_))
    , path_prefix(path_prefix_)
    , file_name(file_name_)
    , is_temp(false)
{
    block_number = parseFileName(file_name);
    auto buf = disk->readFile(path_prefix + file_name);

    *buf >> "format version: 1\n";
    // auto format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;


    LocalDateTime create_time_dt;
    *buf >> "create time: " >> create_time_dt >> "\n";
    create_time = DateLUT::serverTimezoneInstance().makeDateTime(
        create_time_dt.year(), create_time_dt.month(), create_time_dt.day(),
        create_time_dt.hour(), create_time_dt.minute(), create_time_dt.second());

    *buf >> "commands: ";
    commands.readText(*buf);
    *buf >> "\n";

    if (buf->eof())
    {
        tid = Tx::PrehistoricTID;
        csn = Tx::PrehistoricCSN;
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

    for (const auto & command : commands)
    {
        if (!command.partition)
        {
            partition_ids.clear();
            break;
        }

        // const auto & partition_ast = command.partition->as<ASTPartition &>();


        auto partition_id = storage_->getPartitionIDFromQuery(command.partition, context_);
        partition_ids.push_back(partition_id);
        LOG_TRACE(&Poco::Logger::get("MergeTreeMutationEntry"), "ctor: adding {}", partition_id);

#if 0
        // partition_ast.all  ??
        if (!partition_ast.value  && !partition_ast.id.empty())
        {
            // MergeTreePartInfo::validatePartitionID(partition_ast.id, format_version);
            partition_ids.push_back(partition_ast.id);
            LOG_TRACE(&Poco::Logger::get("MergeTreeMutationEntry"), "ctor: adding {}", partition_ast.id);
        }
#endif
    }

    compactPartitionIds(partition_ids);

        // if (partition_ids->size() > 1)
        // {
        //     std::sort(partition_ids->begin(), partition_ids->end());
        //     auto last = std::unique(partition_ids->begin(), partition_ids->end());
        //     partition_ids->erase(last, partition_ids->end());
        // }
        // partition_ids->shrink_to_fit();
}

MergeTreeMutationEntry::~MergeTreeMutationEntry()
{
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
    }
}

bool MergeTreeMutationEntry::affectsPartition(const String & partition_id) const
{
    bool affected = partition_ids.empty() || containsInPartitionIds(partition_ids, partition_id);
    LOG_TRACE(&Poco::Logger::get("MergeTreeMutationEntry"), "Partition {} {}affected by mutation {}",
        partition_id, affected?"":"not ", block_number);
    return affected;
}

std::shared_ptr<const IBackupEntry> MergeTreeMutationEntry::backup() const
{
    WriteBufferFromOwnString out;
    out << "block number: " << block_number << "\n";

    out << "commands: ";
    commands.writeText(out, /* with_pure_metadata_commands = */ false);
    out << "\n";

    return std::make_shared<BackupEntryFromMemory>(out.str());
}

}
