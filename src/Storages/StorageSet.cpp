#include <Storages/StorageSet.h>
#include <Storages/StorageFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <Common/formatReadable.h>
#include <Common/escapeForFileName.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/Set.h>
#include <Interpreters/Context.h>
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
    SetOrJoinBlockOutputStream(
        StorageSetOrJoinBase & table_, const StorageMetadataPtr & metadata_snapshot_,
        const String & backup_path_, const String & backup_tmp_path_,
        const String & backup_file_name_);

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }
    void write(const Block & block) override;
    void writeSuffix() override;

private:
    StorageSetOrJoinBase & table;
    StorageMetadataPtr metadata_snapshot;
    String backup_path;
    String backup_tmp_path;
    String backup_file_name;
    WriteBufferFromFile backup_buf;
    CompressedWriteBuffer compressed_backup_buf;
    NativeBlockOutputStream backup_stream;
};


SetOrJoinBlockOutputStream::SetOrJoinBlockOutputStream(
    StorageSetOrJoinBase & table_,
    const StorageMetadataPtr & metadata_snapshot_,
    const String & backup_path_,
    const String & backup_tmp_path_,
    const String & backup_file_name_)
    : table(table_)
    , metadata_snapshot(metadata_snapshot_)
    , backup_path(backup_path_)
    , backup_tmp_path(backup_tmp_path_)
    , backup_file_name(backup_file_name_)
    , backup_buf(backup_tmp_path + backup_file_name)
    , compressed_backup_buf(backup_buf)
    , backup_stream(compressed_backup_buf, 0, metadata_snapshot->getSampleBlock())
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

    Poco::File(backup_tmp_path + backup_file_name).renameTo(backup_path + backup_file_name);
}


BlockOutputStreamPtr StorageSetOrJoinBase::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    UInt64 id = ++increment;
    return std::make_shared<SetOrJoinBlockOutputStream>(*this, metadata_snapshot, path, path + "tmp/", toString(id) + ".bin");
}


StorageSetOrJoinBase::StorageSetOrJoinBase(
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    setInMemoryMetadata(storage_metadata);


    if (relative_path_.empty())
        throw Exception("Join and Set storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    base_path = context_.getPath();
    path = base_path + relative_path_;
}


StorageSet::StorageSet(
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const Context & context_)
    : StorageSetOrJoinBase{relative_path_, table_id_, columns_, constraints_, context_},
    set(std::make_shared<Set>(SizeLimits(), false, true))
{

    Block header = getInMemoryMetadataPtr()->getSampleBlock();
    header = header.sortColumns();
    set->setHeader(header);

    restore();
}


void StorageSet::insertBlock(const Block & block) { set->insertFromBlock(block); }
void StorageSet::finishInsert() { set->finishInsert(); }
size_t StorageSet::getSize() const { return set->getTotalRowCount(); }


void StorageSet::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, const Context &, TableExclusiveLockHolder &)
{
    Poco::File(path).remove(true);
    Poco::File(path).createDirectories();
    Poco::File(path + "tmp/").createDirectories();

    Block header = metadata_snapshot->getSampleBlock();
    header = header.sortColumns();

    increment = 0;
    set = std::make_shared<Set>(SizeLimits(), false, true);
    set->setHeader(header);
}


void StorageSetOrJoinBase::restore()
{
    Poco::File tmp_dir(path + "tmp/");
    if (!tmp_dir.exists())
    {
        tmp_dir.createDirectories();
        return;
    }

    static const char * file_suffix = ".bin";
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

    finishInsert();
    backup_stream.readSuffix();

    /// TODO Add speed, compressed bytes, data volume in memory, compression ratio ... Generalize all statistics logging in project.
    LOG_INFO(&Poco::Logger::get("StorageSetOrJoinBase"), "Loaded from backup file {}. {} rows, {}. State has {} unique rows.",
        file_path, backup_stream.getProfileInfo().rows, ReadableSize(backup_stream.getProfileInfo().bytes), getSize());
}


void StorageSetOrJoinBase::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    /// Rename directory with data.
    String new_path = base_path + new_path_to_table_data;
    Poco::File(path).renameTo(new_path);

    path = new_path;
    renameInMemory(new_table_id);
}


void registerStorageSet(StorageFactory & factory)
{
    factory.registerStorage("Set", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageSet::create(args.relative_data_path, args.table_id, args.columns, args.constraints, args.context);
    });
}


}
