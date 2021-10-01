#include <Storages/MergeTree/MergeTreeMutationEntry.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>

#include <utility>

namespace DB
{

namespace
{
template <bool is_temporary>
String getFileName(Int64 tmp_number)
{
    return fmt::format(is_temporary
        ? "tmp_mutation_{}.txt"
        : "mutation_{}.txt",
        tmp_number);
}
}

MergeTreeMutationEntry::MergeTreeMutationEntry(
    DiskPtr disk_, std::string_view path_prefix_, MutationType type_, MutationCommands commands_, Int64 tmp_number)
    : type(type_)
    , create_time(time(nullptr))
    , commands(std::move(commands_))
    , disk(std::move(disk_))
    , path_prefix(path_prefix_)
    , file_name(getFileName<true>(tmp_number))
    , is_temp(true)
{
    try
    {
        auto out = disk->writeFile(path_prefix + file_name);

        *out << "format version: 2\n"
             << "type: " << type << "\n"
             << "create time: " << LocalDateTime(create_time)
             << "\ncommands: ";

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

MergeTreeMutationEntry::MergeTreeMutationEntry(
    DiskPtr disk_, std::string_view path_prefix_, std::string_view file_name_)
    : disk(std::move(disk_))
    , path_prefix(path_prefix_)
    , file_name(file_name_)
    , is_temp(false)
{
    ReadBufferFromString file_name_buf(file_name);
    file_name_buf >> "mutation_" >> block_number >> ".txt";
    assertEOF(file_name_buf);

    auto buf = disk->readFile(path_prefix + file_name);

    int format_version;

    *buf >> "format version: " >> format_version >> "\n";

    assert(format_version <= 2);

    String type_str;

    if (format_version == 2)
        *buf >> "type: " >> type_str >> "\n";

    type = *magic_enum::enum_cast<MutationType>(type_str); // FIXME ignore invalid types as for now

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

void MergeTreeMutationEntry::commit(Int64 block_number_)
{
    block_number = block_number_;
    const String new_file_name = getFileName<false>(block_number);

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
