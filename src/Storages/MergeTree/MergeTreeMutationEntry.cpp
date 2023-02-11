#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>

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
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse mutation version from file name, expected 'mutation_<UInt64>.txt', got '{}'", file_name_);
}

MergeTreeMutationEntry::MergeTreeMutationEntry(MutationCommands commands_, DiskPtr disk_, const String & path_prefix_, UInt64 tmp_number)
    : create_time(time(nullptr))
    , commands(std::move(commands_))
    , disk(std::move(disk_))
    , path_prefix(path_prefix_)
    , file_name("tmp_mutation_" + toString(tmp_number) + ".txt")
    , is_temp(true)
{
    try
    {
        auto out = disk->writeFile(path_prefix + file_name);
        *out << "format version: 1\n"
            << "create time: " << LocalDateTime(create_time) << "\n";
        *out << "commands: ";
        commands.writeText(*out);
        *out << "\n";
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

MergeTreeMutationEntry::MergeTreeMutationEntry(DiskPtr disk_, const String & path_prefix_, const String & file_name_)
    : disk(std::move(disk_))
    , path_prefix(path_prefix_)
    , file_name(file_name_)
    , is_temp(false)
{
    block_number = parseFileName(file_name);
    auto buf = disk->readFile(path_prefix + file_name);

    *buf >> "format version: 1\n";

    LocalDateTime create_time_dt;
    *buf >> "create time: " >> create_time_dt >> "\n";
    create_time = DateLUT::instance().makeDateTime(
        create_time_dt.year(), create_time_dt.month(), create_time_dt.day(),
        create_time_dt.hour(), create_time_dt.minute(), create_time_dt.second());

    *buf >> "commands: ";
    commands.readText(*buf);
    *buf >> "\n";

    assertEOF(*buf);
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

}
