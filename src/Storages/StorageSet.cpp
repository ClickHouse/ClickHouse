#include <Storages/StorageSet.h>
#include <Storages/StorageFactory.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Disks/IDisk.h>
#include <Common/formatReadable.h>
#include <Common/StringUtils/StringUtils.h>
#include <Interpreters/Set.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Parsers/ASTCreateQuery.h>
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


class SetOrJoinSink : public SinkToStorage, WithContext
{
public:
    SetOrJoinSink(
        ContextPtr ctx, StorageSetOrJoinBase & table_, const StorageMetadataPtr & metadata_snapshot_,
        const String & backup_path_, const String & backup_tmp_path_,
        const String & backup_file_name_, bool persistent_);

    String getName() const override { return "SetOrJoinSink"; }
    void consume(Chunk chunk) override;
    void onFinish() override;

private:
    StorageSetOrJoinBase & table;
    StorageMetadataPtr metadata_snapshot;
    String backup_path;
    String backup_tmp_path;
    String backup_file_name;
    std::unique_ptr<WriteBufferFromFileBase> backup_buf;
    CompressedWriteBuffer compressed_backup_buf;
    NativeWriter backup_stream;
    bool persistent;
};


SetOrJoinSink::SetOrJoinSink(
    ContextPtr ctx,
    StorageSetOrJoinBase & table_,
    const StorageMetadataPtr & metadata_snapshot_,
    const String & backup_path_,
    const String & backup_tmp_path_,
    const String & backup_file_name_,
    bool persistent_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , WithContext(ctx)
    , table(table_)
    , metadata_snapshot(metadata_snapshot_)
    , backup_path(backup_path_)
    , backup_tmp_path(backup_tmp_path_)
    , backup_file_name(backup_file_name_)
    , backup_buf(table_.disk->writeFile(fs::path(backup_tmp_path) / backup_file_name))
    , compressed_backup_buf(*backup_buf)
    , backup_stream(compressed_backup_buf, 0, metadata_snapshot->getSampleBlock())
    , persistent(persistent_)
{
}

void SetOrJoinSink::consume(Chunk chunk)
{
    Block block = getHeader().cloneWithColumns(chunk.detachColumns());

    table.insertBlock(block, getContext());
    if (persistent)
        backup_stream.write(block);
}

void SetOrJoinSink::onFinish()
{
    table.finishInsert();
    if (persistent)
    {
        backup_stream.flush();
        compressed_backup_buf.next();
        backup_buf->next();
        backup_buf->finalize();

        table.disk->replaceFile(fs::path(backup_tmp_path) / backup_file_name, fs::path(backup_path) / backup_file_name);
    }
}


SinkToStoragePtr StorageSetOrJoinBase::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    UInt64 id = ++increment;
    return std::make_shared<SetOrJoinSink>(
        context, *this, metadata_snapshot, path, fs::path(path) / "tmp/", toString(id) + ".bin", persistent);
}


StorageSetOrJoinBase::StorageSetOrJoinBase(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool persistent_)
    : IStorage(table_id_), disk(disk_), persistent(persistent_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);


    if (relative_path_.empty())
        throw Exception("Join and Set storages require data path", ErrorCodes::INCORRECT_FILE_NAME);

    path = relative_path_;
}


StorageSet::StorageSet(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool persistent_)
    : StorageSetOrJoinBase{disk_, relative_path_, table_id_, columns_, constraints_, comment, persistent_}
    , set(std::make_shared<Set>(SizeLimits(), false, true))
{
    Block header = getInMemoryMetadataPtr()->getSampleBlock();
    set->setHeader(header.getColumnsWithTypeAndName());

    restore();
}


void StorageSet::insertBlock(const Block & block, ContextPtr) { set->insertFromBlock(block.getColumnsWithTypeAndName()); }
void StorageSet::finishInsert() { set->finishInsert(); }

size_t StorageSet::getSize(ContextPtr) const { return set->getTotalRowCount(); }
std::optional<UInt64> StorageSet::totalRows(const Settings &) const { return set->getTotalRowCount(); }
std::optional<UInt64> StorageSet::totalBytes(const Settings &) const { return set->getTotalByteCount(); }

void StorageSet::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &)
{
    disk->removeRecursive(path);
    disk->createDirectories(path);
    disk->createDirectories(fs::path(path) / "tmp/");

    Block header = metadata_snapshot->getSampleBlock();

    increment = 0;
    set = std::make_shared<Set>(SizeLimits(), false, true);
    set->setHeader(header.getColumnsWithTypeAndName());
}


void StorageSetOrJoinBase::restore()
{
    if (!disk->exists(fs::path(path) / "tmp/"))
    {
        disk->createDirectories(fs::path(path) / "tmp/");
        return;
    }

    static const char * file_suffix = ".bin";
    static const auto file_suffix_size = strlen(".bin");

    for (auto dir_it{disk->iterateDirectory(path)}; dir_it->isValid(); dir_it->next())
    {
        const auto & name = dir_it->name();
        const auto & file_path = dir_it->path();

        if (disk->isFile(file_path)
            && endsWith(name, file_suffix)
            && disk->getFileSize(file_path) > 0)
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
    ContextPtr ctx = nullptr;
    auto backup_buf = disk->readFile(file_path);
    CompressedReadBuffer compressed_backup_buf(*backup_buf);
    NativeReader backup_stream(compressed_backup_buf, 0);

    ProfileInfo info;
    while (Block block = backup_stream.read())
    {
        info.update(block);
        insertBlock(block, ctx);
    }

    finishInsert();

    /// TODO Add speed, compressed bytes, data volume in memory, compression ratio ... Generalize all statistics logging in project.
    LOG_INFO(&Poco::Logger::get("StorageSetOrJoinBase"), "Loaded from backup file {}. {} rows, {}. State has {} unique rows.",
        file_path, info.rows, ReadableSize(info.bytes), getSize(ctx));
}


void StorageSetOrJoinBase::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    /// Rename directory with data.
    disk->replaceFile(path, new_path_to_table_data);

    path = new_path_to_table_data;
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

        bool has_settings = args.storage_def->settings;
        SetSettings set_settings;
        if (has_settings)
            set_settings.loadFromQuery(*args.storage_def);

        DiskPtr disk = args.getContext()->getDisk(set_settings.disk);
        return std::make_shared<StorageSet>(
            disk, args.relative_data_path, args.table_id, args.columns, args.constraints, args.comment, set_settings.persistent);
    }, StorageFactory::StorageFeatures{ .supports_settings = true, });
}


}
