#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>

#include <utility>


namespace DB
{

MergeTreeMutationEntry::MergeTreeMutationEntry(MutationCommands commands_, DiskPtr disk_, const String & path_prefix_, Int64 tmp_number)
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

void MergeTreeMutationEntry::commit(Int64 block_number_)
{
    block_number = block_number_;
    String new_file_name = "mutation_" + toString(block_number) + ".txt";
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
    ReadBufferFromString file_name_buf(file_name);
    file_name_buf >> "mutation_" >> block_number >> ".txt";
    assertEOF(file_name_buf);

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
