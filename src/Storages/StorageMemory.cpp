#include <cassert>
#include <Common/Exception.h>

#include <boost/noncopyable.hpp>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMemory.h>
#include <Storages/MemorySettings.h>
#include <DataTypes/ObjectUtils.h>

#include <IO/WriteHelpers.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Parsers/ASTCreateQuery.h>

#include <Common/FileChecker.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupEntryFromSmallFile.h>
#include <Backups/IBackup.h>
#include <Backups/IBackupEntriesLazyBatch.h>
#include <Backups/RestorerFromBackup.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <IO/copyData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_RESTORE_TABLE;
}

class MemorySink : public SinkToStorage
{
public:
    MemorySink(
        StorageMemory & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , storage_snapshot(storage_.getStorageSnapshot(metadata_snapshot_, context))
    {
    }

    String getName() const override { return "MemorySink"; }

    void consume(Chunk chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        storage_snapshot->metadata->check(block, true);
        if (!storage_snapshot->object_columns.empty())
        {
            auto extended_storage_columns = storage_snapshot->getColumns(
                GetColumnsOptions(GetColumnsOptions::AllPhysical).withExtendedObjects());

            convertDynamicColumnsToTuples(block, storage_snapshot);
        }

        if (storage.compress)
        {
            Block compressed_block;
            for (const auto & elem : block)
                compressed_block.insert({ elem.column->compress(), elem.type, elem.name });

            new_blocks.emplace_back(compressed_block);
        }
        else
        {
            new_blocks.emplace_back(block);
        }
    }

    void onFinish() override
    {
        size_t inserted_bytes = 0;
        size_t inserted_rows = 0;

        for (const auto & block : new_blocks)
        {
            inserted_bytes += block.allocatedBytes();
            inserted_rows += block.rows();
        }

        std::lock_guard lock(storage.mutex);

        auto new_data = std::make_unique<Blocks>(*(storage.data.get()));
        new_data->insert(new_data->end(), new_blocks.begin(), new_blocks.end());

        storage.data.set(std::move(new_data));
        storage.total_size_bytes.fetch_add(inserted_bytes, std::memory_order_relaxed);
        storage.total_size_rows.fetch_add(inserted_rows, std::memory_order_relaxed);
    }

private:
    Blocks new_blocks;

    StorageMemory & storage;
    StorageSnapshotPtr storage_snapshot;
};


StorageMemory::StorageMemory(
    const StorageID & table_id_,
    ColumnsDescription columns_description_,
    ConstraintsDescription constraints_,
    const String & comment,
    bool compress_)
    : IStorage(table_id_), data(std::make_unique<const Blocks>()), compress(compress_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_description_));
    storage_metadata.setConstraints(std::move(constraints_));
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

StorageSnapshotPtr StorageMemory::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr /*query_context*/) const
{
    auto snapshot_data = std::make_unique<SnapshotData>();
    snapshot_data->blocks = data.get();

    if (!hasDynamicSubcolumns(metadata_snapshot->getColumns()))
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, ColumnsDescription{}, std::move(snapshot_data));

    auto object_columns = getConcreteObjectColumns(
        snapshot_data->blocks->begin(),
        snapshot_data->blocks->end(),
        metadata_snapshot->getColumns(),
        [](const auto & block) -> const auto & { return block.getColumnsWithTypeAndName(); });

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::move(object_columns), std::move(snapshot_data));
}

void StorageMemory::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t num_streams)
{
    query_plan.addStep(std::make_unique<ReadFromMemoryStorageStep>(column_names, shared_from_this(), storage_snapshot, num_streams, delay_read_for_global_subqueries));
}


SinkToStoragePtr StorageMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/)
{
    return std::make_shared<MemorySink>(*this, metadata_snapshot, context);
}


void StorageMemory::drop()
{
    data.set(std::make_unique<Blocks>());
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);
}

static inline void updateBlockData(Block & old_block, const Block & new_block)
{
    for (const auto & it : new_block)
    {
        auto col_name = it.name;
        auto & col_with_type_name = old_block.getByName(col_name);
        col_with_type_name.column = it.column;
    }
}

void StorageMemory::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    /// Some validation will be added
}

void StorageMemory::mutate(const MutationCommands & commands, ContextPtr context)
{
    std::lock_guard lock(mutex);
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, context);

    /// When max_threads > 1, the order of returning blocks is uncertain,
    /// which will lead to inconsistency after updateBlockData.
    auto new_context = Context::createCopy(context);
    new_context->setSetting("max_streams_to_max_threads_ratio", 1);
    new_context->setSetting("max_threads", 1);

    MutationsInterpreter::Settings settings(true);
    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, new_context, settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    Blocks out;
    Block block;
    while (executor.pull(block))
    {
        if (compress)
            for (auto & elem : block)
                elem.column = elem.column->compress();

        out.push_back(block);
    }

    std::unique_ptr<Blocks> new_data;

    // all column affected
    if (interpreter->isAffectingAllColumns())
    {
        new_data = std::make_unique<Blocks>(out);
    }
    else
    {
        /// just some of the column affected, we need update it with new column
        new_data = std::make_unique<Blocks>(*(data.get()));
        auto data_it = new_data->begin();
        auto out_it = out.begin();

        while (data_it != new_data->end())
        {
            /// Mutation does not change the number of blocks
            assert(out_it != out.end());

            updateBlockData(*data_it, *out_it);
            ++data_it;
            ++out_it;
        }

        assert(out_it == out.end());
    }

    size_t rows = 0;
    size_t bytes = 0;
    for (const auto & buffer : *new_data)
    {
        rows += buffer.rows();
        bytes += buffer.bytes();
    }
    total_size_bytes.store(bytes, std::memory_order_relaxed);
    total_size_rows.store(rows, std::memory_order_relaxed);
    data.set(std::move(new_data));
}


void StorageMemory::truncate(
    const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    data.set(std::make_unique<Blocks>());
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);
}


namespace
{
    class MemoryBackup : public IBackupEntriesLazyBatch, boost::noncopyable
    {
    public:
        MemoryBackup(
            ContextPtr context_,
            const StorageMetadataPtr & metadata_snapshot_,
            const std::shared_ptr<const Blocks> blocks_,
            const String & data_path_in_backup,
            const DiskPtr & temp_disk_,
            const ReadSettings & read_settings_,
            UInt64 max_compress_block_size_)
            : context(context_)
            , metadata_snapshot(metadata_snapshot_)
            , blocks(blocks_)
            , temp_disk(temp_disk_)
            , read_settings(read_settings_)
            , max_compress_block_size(max_compress_block_size_)
        {
            fs::path data_path_in_backup_fs = data_path_in_backup;
            data_bin_pos = file_paths.size();
            file_paths.emplace_back(data_path_in_backup_fs / "data.bin");
            index_mrk_pos= file_paths.size();
            file_paths.emplace_back(data_path_in_backup_fs / "index.mrk");
            columns_txt_pos = file_paths.size();
            file_paths.emplace_back(data_path_in_backup_fs / "columns.txt");
            count_txt_pos = file_paths.size();
            file_paths.emplace_back(data_path_in_backup_fs / "count.txt");
            sizes_json_pos = file_paths.size();
            file_paths.emplace_back(data_path_in_backup_fs / "sizes.json");
        }

    private:
        size_t getSize() const override
        {
            return file_paths.size();
        }

        const String & getName(size_t i) const override
        {
            return file_paths[i];
        }

        BackupEntries generate() override
        {
            BackupEntries backup_entries;
            backup_entries.resize(file_paths.size());

            temp_dir_owner.emplace(temp_disk);
            fs::path temp_dir = temp_dir_owner->getRelativePath();
            temp_disk->createDirectories(temp_dir);

            /// Writing data.bin
            IndexForNativeFormat index;
            {
                auto data_file_path = temp_dir / fs::path{file_paths[data_bin_pos]}.filename();
                auto data_out_compressed = temp_disk->writeFile(data_file_path);
                auto data_out = std::make_unique<CompressedWriteBuffer>(*data_out_compressed, CompressionCodecFactory::instance().getDefaultCodec(), max_compress_block_size);
                NativeWriter block_out{*data_out, 0, metadata_snapshot->getSampleBlock(), false, &index};
                for (const auto & block : *blocks)
                    block_out.write(block);
                data_out->finalize();
                data_out.reset();
                data_out_compressed->finalize();
                data_out_compressed.reset();
                backup_entries[data_bin_pos] = {file_paths[data_bin_pos], std::make_shared<BackupEntryFromAppendOnlyFile>(temp_disk, data_file_path)};
            }

            /// Writing index.mrk
            {
                auto index_mrk_path = temp_dir / fs::path{file_paths[index_mrk_pos]}.filename();
                auto index_mrk_out_compressed = temp_disk->writeFile(index_mrk_path);
                auto index_mrk_out = std::make_unique<CompressedWriteBuffer>(*index_mrk_out_compressed);
                index.write(*index_mrk_out);
                index_mrk_out->finalize();
                index_mrk_out.reset();
                index_mrk_out_compressed->finalize();
                index_mrk_out_compressed.reset();
                backup_entries[index_mrk_pos] = {file_paths[index_mrk_pos], std::make_shared<BackupEntryFromAppendOnlyFile>(temp_disk, index_mrk_path)};
            }

            /// Writing columns.txt
            {
                auto columns_desc = metadata_snapshot->getColumns().getAllPhysical().toString();
                backup_entries[columns_txt_pos] = {file_paths[columns_txt_pos], std::make_shared<BackupEntryFromMemory>(columns_desc)};
            }

            /// Writing count.txt
            {
                size_t num_rows = 0;
                for (const auto & block : *blocks)
                    num_rows += block.rows();
                backup_entries[count_txt_pos] = {file_paths[count_txt_pos], std::make_shared<BackupEntryFromMemory>(toString(num_rows))};
            }

            /// Writing sizes.json
            {
                auto sizes_json_path = temp_dir / fs::path{file_paths[sizes_json_pos]}.filename();
                FileChecker file_checker{temp_disk, sizes_json_path};
                for (size_t i = 0; i != file_paths.size(); ++i)
                {
                    if (i == sizes_json_pos)
                        continue;
                    file_checker.update(temp_dir / fs::path{file_paths[i]}.filename());
                }
                file_checker.save();
                backup_entries[sizes_json_pos] = {file_paths[sizes_json_pos], std::make_shared<BackupEntryFromSmallFile>(temp_disk, sizes_json_path, read_settings)};
            }

            /// We don't need to keep `blocks` any longer.
            blocks.reset();
            metadata_snapshot.reset();

            return backup_entries;
        }

        ContextPtr context;
        StorageMetadataPtr metadata_snapshot;
        std::shared_ptr<const Blocks> blocks;
        DiskPtr temp_disk;
        std::optional<TemporaryFileOnDisk> temp_dir_owner;
        ReadSettings read_settings;
        UInt64 max_compress_block_size;
        Strings file_paths;
        size_t data_bin_pos, index_mrk_pos, columns_txt_pos, count_txt_pos, sizes_json_pos;
    };
}

void StorageMemory::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto temp_disk = backup_entries_collector.getContext()->getGlobalTemporaryVolume()->getDisk(0);
    const auto & read_settings = backup_entries_collector.getReadSettings();
    auto max_compress_block_size = backup_entries_collector.getContext()->getSettingsRef().max_compress_block_size;

    backup_entries_collector.addBackupEntries(std::make_shared<MemoryBackup>(
        backup_entries_collector.getContext(),
        getInMemoryMetadataPtr(),
        data.get(),
        data_path_in_backup,
        temp_disk,
        read_settings,
        max_compress_block_size)->getBackupEntries());
}

void StorageMemory::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto backup = restorer.getBackup();
    if (!backup->hasFiles(data_path_in_backup))
        return;

    if (!restorer.isNonEmptyTableAllowed() && total_size_bytes)
        RestorerFromBackup::throwTableIsNotEmpty(getStorageID());

    auto temp_disk = restorer.getContext()->getGlobalTemporaryVolume()->getDisk(0);

    restorer.addDataRestoreTask(
        [storage = std::static_pointer_cast<StorageMemory>(shared_from_this()), backup, data_path_in_backup, temp_disk]
        { storage->restoreDataImpl(backup, data_path_in_backup, temp_disk); });
}

void StorageMemory::restoreDataImpl(const BackupPtr & backup, const String & data_path_in_backup, const DiskPtr & temporary_disk)
{
    /// Our data are in the StripeLog format.

    fs::path data_path_in_backup_fs = data_path_in_backup;

    /// Reading index.mrk
    IndexForNativeFormat index;
    {
        String index_file_path = data_path_in_backup_fs / "index.mrk";
        if (!backup->fileExists(index_file_path))
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File {} in backup is required to restore table", index_file_path);

        auto in = backup->readFile(index_file_path);
        CompressedReadBuffer compressed_in{*in};
        index.read(compressed_in);
    }

    /// Reading data.bin
    Blocks new_blocks;
    size_t new_bytes = 0;
    size_t new_rows = 0;
    {
        String data_file_path = data_path_in_backup_fs / "data.bin";
        if (!backup->fileExists(data_file_path))
            throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "File {} in backup is required to restore table", data_file_path);

        auto in = backup->readFile(data_file_path);
        std::optional<TemporaryFileOnDisk> temp_data_file;
        if (!dynamic_cast<ReadBufferFromFileBase *>(in.get()))
        {
            temp_data_file.emplace(temporary_disk);
            auto out = std::make_unique<WriteBufferFromFile>(temp_data_file->getAbsolutePath());
            copyData(*in, *out);
            out.reset();
            in = createReadBufferFromFileBase(temp_data_file->getAbsolutePath(), {});
        }
        std::unique_ptr<ReadBufferFromFileBase> in_from_file{static_cast<ReadBufferFromFileBase *>(in.release())};
        CompressedReadBufferFromFile compressed_in{std::move(in_from_file)};
        NativeReader block_in{compressed_in, 0, index.blocks.begin(), index.blocks.end()};

        while (auto block = block_in.read())
        {
            new_bytes += block.bytes();
            new_rows += block.rows();
            new_blocks.push_back(std::move(block));
        }
    }

    /// Append old blocks with the new ones.
    auto old_blocks = data.get();
    Blocks old_and_new_blocks = *old_blocks;
    old_and_new_blocks.insert(old_and_new_blocks.end(), std::make_move_iterator(new_blocks.begin()), std::make_move_iterator(new_blocks.end()));

    /// Finish restoring.
    data.set(std::make_unique<Blocks>(std::move(old_and_new_blocks)));
    total_size_bytes += new_bytes;
    total_size_rows += new_rows;
}


std::optional<UInt64> StorageMemory::totalRows(const Settings &) const
{
    /// All modifications of these counters are done under mutex which automatically guarantees synchronization/consistency
    /// When run concurrently we are fine with any value: "before" or "after"
    return total_size_rows.load(std::memory_order_relaxed);
}

std::optional<UInt64> StorageMemory::totalBytes(const Settings &) const
{
    return total_size_bytes.load(std::memory_order_relaxed);
}

void registerStorageMemory(StorageFactory & factory)
{
    factory.registerStorage("Memory", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        bool has_settings = args.storage_def->settings;
        MemorySettings settings;
        if (has_settings)
            settings.loadFromQuery(*args.storage_def);

        return std::make_shared<StorageMemory>(args.table_id, args.columns, args.constraints, args.comment, settings.compress);
    },
    {
        .supports_settings = true,
        .supports_parallel_insert = true,
    });
}

}
