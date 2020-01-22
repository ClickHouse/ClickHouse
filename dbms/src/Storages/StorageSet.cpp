#include <Storages/StorageSet.h>
#include <Storages/StorageFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/Set.h>
#include <filesystem>

namespace fs = std::filesystem;

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
    table.finishInsert();
    backup_stream.flush();
    compressed_backup_buf.next();
    backup_buf.next();

    fs::rename(backup_tmp_path + backup_file_name, backup_path + backup_file_name);
}



BlockOutputStreamPtr StorageSetOrJoinBase::write(const ASTPtr & /*query*/, const Context & /*context*/)
{
    UInt64 id = ++increment;
    return std::make_shared<SetOrJoinBlockOutputStream>(*this, path, path + "tmp/", toString(id) + ".bin");
}


StorageSetOrJoinBase::StorageSetOrJoinBase(
    const String & relative_path_,
    const String & database_name_,
    const String & table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : table_name(table_name_), database_name(database_name_)
{
    setColumns(columns_);
    setConstraints(constraints_);

    if (relative_path_.empty())
        throw Exception("Join and Set storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    base_path = context_.getPath();
    path = base_path + relative_path_;
}



StorageSet::StorageSet(
    const String & relative_path_,
    const String & database_name_,
    const String & table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : StorageSetOrJoinBase{relative_path_, database_name_, table_name_, columns_, constraints_, context_},
    set(std::make_shared<Set>(SizeLimits(), false))
{
    Block header = getSampleBlock();
    header = header.sortColumns();
    set->setHeader(header);

    restore();
}


void StorageSet::insertBlock(const Block & block) { set->insertFromBlock(block); }
void StorageSet::finishInsert() { set->finishInsert(); }
size_t StorageSet::getSize() const { return set->getTotalRowCount(); }


void StorageSet::truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &)
{
    fs::remove_all(path);
    fs::create_directories(path + "tmp/");

    Block header = getSampleBlock();
    header = header.sortColumns();

    increment = 0;
    set = std::make_shared<Set>(SizeLimits(), false);
    set->setHeader(header);
}


void StorageSetOrJoinBase::restore()
{
    fs::path tmp_dir(path + "tmp/");
    if (!fs::exists(tmp_dir))
    {
        fs::create_directories(tmp_dir);
        return;
    }

    static const auto file_suffix = ".bin";

    fs::directory_iterator dir_end;
    for (fs::directory_iterator dir_it(path); dir_end != dir_it; ++dir_it)
    {
        const fs::path file_path = dir_it->path();
        const String& file_path_str{file_path};

        if (dir_it->is_regular_file()
            && file_path.extension() == file_suffix
            && dir_it->file_size() > 0)
        {
            /// Calculate the maximum number of available files with a backup to add the following files with large numbers.
            UInt64 file_num = parse<UInt64>(file_path.stem().string());
            if (file_num > increment)
                increment = file_num;

            restoreFromFile(file_path_str);
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

    finishInsert();
    backup_stream.readSuffix();

    /// TODO Add speed, compressed bytes, data volume in memory, compression ratio ... Generalize all statistics logging in project.
    LOG_INFO(&Logger::get("StorageSetOrJoinBase"), std::fixed << std::setprecision(2)
        << "Loaded from backup file " << file_path << ". "
        << backup_stream.getProfileInfo().rows << " rows, "
        << backup_stream.getProfileInfo().bytes / 1048576.0 << " MiB. "
        << "State has " << getSize() << " unique rows.");
}


void StorageSetOrJoinBase::rename(
    const String & new_path_to_table_data, const String & new_database_name, const String & new_table_name, TableStructureWriteLockHolder &)
{
    /// Rename directory with data.
    String new_path = base_path + new_path_to_table_data;
    fs::rename(path, new_path);

    path = new_path;
    table_name = new_table_name;
    database_name = new_database_name;
}


void registerStorageSet(StorageFactory & factory)
{
    factory.registerStorage("Set", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageSet::create(args.relative_data_path, args.database_name, args.table_name, args.columns, args.constraints, args.context);
    });
}


}
