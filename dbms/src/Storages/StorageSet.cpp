#include <Storages/StorageSet.h>
#include <Storages/StorageFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/Set.h>
#include <Poco/DirectoryIterator.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace ErrorCodes
{
    extern const int INCORRECT_FILE_NAME;
}


class SetOrJoinBlockOutputStream : public IBlockOutputStream
{
public:
    SetOrJoinBlockOutputStream(StorageSetOrJoinBase & table_,
        const String & backup_path_, const String & backup_tmp_path_, const String & backup_file_name_);

    Block getHeader() const override { return table.getSampleBlock(); }
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    StorageSetOrJoinBase & table;
    String backup_path;
    String backup_tmp_path;
    String backup_file_name;
    WriteBufferFromFile backup_buf;
    CompressedWriteBuffer compressed_backup_buf;
    NativeBlockOutputStream backup_stream;
};


SetOrJoinBlockOutputStream::SetOrJoinBlockOutputStream(StorageSetOrJoinBase & table_,
    const String & backup_path_, const String & backup_tmp_path_, const String & backup_file_name_)
    : table(table_),
    backup_path(backup_path_), backup_tmp_path(backup_tmp_path_),
    backup_file_name(backup_file_name_),
    backup_buf(backup_tmp_path + backup_file_name),
    compressed_backup_buf(backup_buf),
    backup_stream(compressed_backup_buf, 0, table.getSampleBlock())
{
}

void SetOrJoinBlockOutputStream::write(const Block & block)
{
    /// Sort columns in the block. This is necessary, since Set and Join count on the same column order in different blocks.
    Block sorted_block = block.sortColumns();

    table.insertBlock(sorted_block);
    backup_stream.write(sorted_block);
}

void SetOrJoinBlockOutputStream::writeSuffix()
{
    backup_stream.flush();
    compressed_backup_buf.next();
    backup_buf.next();

    Poco::File(backup_tmp_path + backup_file_name).renameTo(backup_path + backup_file_name);
}



BlockOutputStreamPtr StorageSetOrJoinBase::write(const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    ++increment;
    return std::make_shared<SetOrJoinBlockOutputStream>(*this, path, path + "tmp/", toString(increment) + ".bin");
}


StorageSetOrJoinBase::StorageSetOrJoinBase(
    const String & path_,
    const String & table_name_,
    const ColumnsDescription & columns_)
    : IStorage{columns_}, table_name(table_name_)
{
    if (path_.empty())
        throw Exception("Join and Set storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    path = path_ + escapeForFileName(table_name_) + '/';
}



StorageSet::StorageSet(
    const String & path_,
    const String & name_,
    const ColumnsDescription & columns_)
    : StorageSetOrJoinBase{path_, name_, columns_},
    set(std::make_shared<Set>(SizeLimits(), false))
{
    Block header = getSampleBlock();
    header = header.sortColumns();
    set->setHeader(header);

    restore();
}


void StorageSet::insertBlock(const Block & block) { set->insertFromBlock(block); }
size_t StorageSet::getSize() const { return set->getTotalRowCount(); }


void StorageSet::truncate(const ASTPtr &)
{
    Poco::File(path).remove(true);
    Poco::File(path).createDirectories();
    Poco::File(path + "tmp/").createDirectories();

    Block header = getSampleBlock();
    header = header.sortColumns();

    increment = 0;
    set = std::make_shared<Set>(SizeLimits(), false);
    set->setHeader(header);
};


void StorageSetOrJoinBase::restore()
{
    Poco::File tmp_dir(path + "tmp/");
    if (!tmp_dir.exists())
    {
        tmp_dir.createDirectories();
        return;
    }

    static const auto file_suffix = ".bin";
    static const auto file_suffix_size = strlen(".bin");

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator dir_it(path); dir_end != dir_it; ++dir_it)
    {
        const auto & name = dir_it.name();

        if (dir_it->isFile()
            && endsWith(name, file_suffix)
            && dir_it->getSize() > 0)
        {
            /// Calculate the maximum number of available files with a backup to add the following files with large numbers.
            UInt64 file_num = parse<UInt64>(name.substr(0, name.size() - file_suffix_size));
            if (file_num > increment)
                increment = file_num;

            restoreFromFile(dir_it->path());
        }
    }
}


void StorageSetOrJoinBase::restoreFromFile(const String & file_path)
{
    ReadBufferFromFile backup_buf(file_path);
    CompressedReadBuffer compressed_backup_buf(backup_buf);
    NativeBlockInputStream backup_stream(compressed_backup_buf, 0);

    backup_stream.readPrefix();
    while (Block block = backup_stream.read())
        insertBlock(block);
    backup_stream.readSuffix();

    /// TODO Add speed, compressed bytes, data volume in memory, compression ratio ... Generalize all statistics logging in project.
    LOG_INFO(&Logger::get("StorageSetOrJoinBase"), std::fixed << std::setprecision(2)
        << "Loaded from backup file " << file_path << ". "
        << backup_stream.getProfileInfo().rows << " rows, "
        << backup_stream.getProfileInfo().bytes / 1048576.0 << " MiB. "
        << "State has " << getSize() << " unique rows.");
}


void StorageSetOrJoinBase::rename(const String & new_path_to_db, const String & /*new_database_name*/, const String & new_table_name)
{
    /// Rename directory with data.
    String new_path = new_path_to_db + escapeForFileName(new_table_name);
    Poco::File(path).renameTo(new_path);

    path = new_path + "/";
    table_name = new_table_name;
}


void registerStorageSet(StorageFactory & factory)
{
    factory.registerStorage("Set", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageSet::create(args.data_path, args.table_name, args.columns);
    });
}


}
