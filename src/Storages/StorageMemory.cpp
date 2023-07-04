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
#include <Columns/ColumnObject.h>

#include <IO/WriteHelpers.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Parsers/ASTCreateQuery.h>

#include <Common/FileChecker.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromImmutableFile.h>
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


class MemorySource : public ISource
{
    using InitializerFunc = std::function<void(std::shared_ptr<const Blocks> &)>;
public:

    MemorySource(
        Names column_names_,
        const StorageSnapshotPtr & storage_snapshot,
        std::shared_ptr<const Blocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {})
        : ISource(storage_snapshot->getSampleBlockForColumns(column_names_))
        , column_names_and_types(storage_snapshot->getColumnsByNames(
            GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withExtendedObjects(), column_names_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
    {
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (initializer_func)
        {
            initializer_func(data);
            initializer_func = {};
        }

        size_t current_index = getAndIncrementExecutionIndex();

        if (!data || current_index >= data->size())
        {
            return {};
        }

        const Block & src = (*data)[current_index];

        Columns columns;
        size_t num_columns = column_names_and_types.size();
        columns.reserve(num_columns);

        auto name_and_type = column_names_and_types.begin();
        for (size_t i = 0; i < num_columns; ++i)
        {
            columns.emplace_back(tryGetColumnFromBlock(src, *name_and_type));
            ++name_and_type;
        }

        fillMissingColumns(columns, src.rows(), column_names_and_types, /*metadata_snapshot=*/ nullptr);
        assert(std::all_of(columns.begin(), columns.end(), [](const auto & column) { return column != nullptr; }));

        return Chunk(std::move(columns), src.rows());
    }

private:
    size_t getAndIncrementExecutionIndex()
    {
        if (parallel_execution_index)
        {
            return (*parallel_execution_index)++;
        }
        else
        {
            return execution_index++;
        }
    }

    const NamesAndTypesList column_names_and_types;
    size_t execution_index = 0;
    std::shared_ptr<const Blocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
};


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

            convertObjectsToTuples(block, extended_storage_columns);
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

    if (!hasObjectColumns(metadata_snapshot->getColumns()))
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, ColumnsDescription{}, std::move(snapshot_data));

    auto object_columns = getObjectColumns(
        snapshot_data->blocks->begin(),
        snapshot_data->blocks->end(),
        metadata_snapshot->getColumns(),
        [](const auto & block) -> const auto & { return block.getColumnsWithTypeAndName(); });

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, object_columns, std::move(snapshot_data));
}

Pipe StorageMemory::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    storage_snapshot->check(column_names);

    const auto & snapshot_data = assert_cast<const SnapshotData &>(*storage_snapshot->data);
    auto current_data = snapshot_data.blocks;

    if (delay_read_for_global_subqueries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        return Pipe(std::make_shared<MemorySource>(
            column_names,
            storage_snapshot,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [current_data](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                data_to_initialize = current_data;
            }));
    }

    size_t size = current_data->size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        pipes.emplace_back(std::make_shared<MemorySource>(column_names, storage_snapshot, current_data, parallel_execution_index));
    }

    return Pipe::unitePipes(std::move(pipes));
}


SinkToStoragePtr StorageMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
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

    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, new_context, true);
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
            const StorageMetadataPtr & metadata_snapshot_,
            const std::shared_ptr<const Blocks> blocks_,
            const String & data_path_in_backup,
            const DiskPtr & temp_disk_,
            UInt64 max_compress_block_size_)
            : metadata_snapshot(metadata_snapshot_)
            , blocks(blocks_)
            , temp_disk(temp_disk_)
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
            fs::path temp_dir = temp_dir_owner->getPath();
            temp_disk->createDirectories(temp_dir);

            /// Writing data.bin
            IndexForNativeFormat index;
            {
                auto data_file_path = temp_dir / fs::path{file_paths[data_bin_pos]}.filename();
                auto data_out_compressed = temp_disk->writeFile(data_file_path);
                CompressedWriteBuffer data_out{*data_out_compressed, CompressionCodecFactory::instance().getDefaultCodec(), max_compress_block_size};
                NativeWriter block_out{data_out, 0, metadata_snapshot->getSampleBlock(), false, &index};
                for (const auto & block : *blocks)
                    block_out.write(block);
                backup_entries[data_bin_pos] = {file_paths[data_bin_pos], std::make_shared<BackupEntryFromImmutableFile>(temp_disk, data_file_path)};
            }

            /// Writing index.mrk
            {
                auto index_mrk_path = temp_dir / fs::path{file_paths[index_mrk_pos]}.filename();
                auto index_mrk_out_compressed = temp_disk->writeFile(index_mrk_path);
                CompressedWriteBuffer index_mrk_out{*index_mrk_out_compressed};
                index.write(index_mrk_out);
                backup_entries[index_mrk_pos] = {file_paths[index_mrk_pos], std::make_shared<BackupEntryFromImmutableFile>(temp_disk, index_mrk_path)};
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
                backup_entries[sizes_json_pos] = {file_paths[sizes_json_pos], std::make_shared<BackupEntryFromSmallFile>(temp_disk, sizes_json_path)};
            }

            /// We don't need to keep `blocks` any longer.
            blocks.reset();
            metadata_snapshot.reset();

            return backup_entries;
        }

        StorageMetadataPtr metadata_snapshot;
        std::shared_ptr<const Blocks> blocks;
        DiskPtr temp_disk;
        std::optional<TemporaryFileOnDisk> temp_dir_owner;
        UInt64 max_compress_block_size;
        Strings file_paths;
        size_t data_bin_pos, index_mrk_pos, columns_txt_pos, count_txt_pos, sizes_json_pos;
    };
}

void StorageMemory::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto temp_disk = backup_entries_collector.getContext()->getTemporaryVolume()->getDisk(0);
    auto max_compress_block_size = backup_entries_collector.getContext()->getSettingsRef().max_compress_block_size;
    backup_entries_collector.addBackupEntries(
        std::make_shared<MemoryBackup>(getInMemoryMetadataPtr(), data.get(), data_path_in_backup, temp_disk, max_compress_block_size)
            ->getBackupEntries());
}

void StorageMemory::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto backup = restorer.getBackup();
    if (!backup->hasFiles(data_path_in_backup))
        return;

    if (!restorer.isNonEmptyTableAllowed() && total_size_bytes)
        RestorerFromBackup::throwTableIsNotEmpty(getStorageID());

    auto temp_disk = restorer.getContext()->getTemporaryVolume()->getDisk(0);

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

        auto backup_entry = backup->readFile(index_file_path);
        auto in = backup_entry->getReadBuffer();
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

        auto backup_entry = backup->readFile(data_file_path);
        std::unique_ptr<ReadBuffer> in = backup_entry->getReadBuffer();
        std::optional<TemporaryFileOnDisk> temp_data_file;
        if (!dynamic_cast<ReadBufferFromFileBase *>(in.get()))
        {
            temp_data_file.emplace(temporary_disk);
            auto out = std::make_unique<WriteBufferFromFile>(temp_data_file->getPath());
            copyData(*in, *out);
            out.reset();
            in = createReadBufferFromFileBase(temp_data_file->getPath(), {});
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
