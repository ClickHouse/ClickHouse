#include <Storages/System/StorageSystemDetachedParts.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <IO/IOThreadPool.h>
#include <Interpreters/threadPoolCallbackRunner.h>

#include <mutex>

namespace DB
{

namespace
{

void partFilesOnDiskImpl(const DiskPtr & disk, const String & from, std::vector<String> & files)
{
    if (disk->isFile(from))
    {
        files.push_back(from);
    }
    else
    {
        for (auto it = disk->iterateDirectory(from); it->isValid(); it->next())
            partFilesOnDiskImpl(disk, fs::path(from) / it->name(), files);
    }
}

std::vector<String> partFilesOnDisk(const DiskPtr & disk, const String & from)
{
    std::vector<String> files;
    try
    {
        partFilesOnDiskImpl(disk, from, files);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    return files;
}

class SourceState
{
    std::mutex mutex;
    StoragesInfoStream stream;

public:
    SourceState(StoragesInfoStream && stream_)
        : stream(std::move(stream_))
    {}

    StoragesInfo next()
    {
        std::lock_guard lock(mutex);
        return stream.next();
    }
};

class DetachedPartsSource : public ISource
{
public:
    DetachedPartsSource(Block header_, std::shared_ptr<SourceState> state_, std::vector<UInt8> columns_mask_, UInt64 block_size_,
                        bool has_bytes_on_disk_column_)
        : ISource(std::move(header_))
        , state(state_)
        , columns_mask(std::move(columns_mask_))
        , block_size(block_size_)
        , has_bytes_on_disk_column(has_bytes_on_disk_column_)
    {}

    String getName() const override { return "DataPartsSource"; }

protected:
    Chunk generate() override
    {
        Chunk result;

        while (result.getNumRows() < block_size)
        {
            if (detached_parts.empty())
                getMoreParts();

            if (detached_parts.empty())
            {
                progress(result.getNumRows(), result.bytes());
                return result;
            }

            Chunk chunk = generateChunk(block_size - result.getNumRows());

            if (result)
                result.append(chunk);
            else
                result = std::move(chunk);
        }

        progress(result.getNumRows(), result.bytes());
        return result;
    }

private:
    std::shared_ptr<SourceState> state;
    const std::vector<UInt8> columns_mask;
    const UInt64 block_size;
    const bool has_bytes_on_disk_column;

    StoragesInfo current_info;
    DetachedPartsInfo detached_parts;

    void getMoreParts()
    {
        chassert(detached_parts.empty());

        current_info = state->next();
        if (!current_info)
            return;

        detached_parts = current_info.data->getDetachedParts();
    }

    Chunk generateChunk(size_t max_rows)
    {
        chassert(current_info);

        auto rows = std::min(max_rows, detached_parts.size());
        auto begin = detached_parts.size() - rows;

        std::vector<std::atomic<size_t>> parts_sizes(rows);

        if (has_bytes_on_disk_column)
        {
            std::vector<std::future<void>> futures;
            SCOPE_EXIT_SAFE({
                /// Exceptions are not propagated
                for (auto & future : futures)
                    if (future.valid())
                        future.wait();
                futures.clear();
            });

            for (auto p_id = begin; p_id < detached_parts.size(); ++p_id)
            {
                auto & p = detached_parts.at(p_id);
                DiskPtr & disk = p.disk;

                const String part_path = fs::path(MergeTreeData::DETACHED_DIR_NAME) / p.dir_name;
                const String relative_path = fs::path(current_info.data->getRelativeDataPath()) / part_path;
                auto * counter = &parts_sizes[p_id - begin];

                std::vector<String> listing = partFilesOnDisk(disk, relative_path);
                for (const auto & file : listing)
                {
                    futures.push_back(
                        scheduleFromThreadPool<void>(
                            [disk, file, counter] ()
                            {
                                size_t size = disk->getFileSize(file);
                                counter->fetch_add(size);
                            },
                            IOThreadPool::get(),
                            "DP_BytesOnDisk"));
                }
            }

            /// Exceptions are propagated
            for (auto & future : futures)
                future.get();
        }

        MutableColumns new_columns = getPort().getHeader().cloneEmptyColumns();

        for (auto p_id = begin; p_id < detached_parts.size(); ++p_id)
        {
            auto & p = detached_parts.at(p_id);

            size_t src_index = 0;
            size_t res_index = 0;
            String detached_part_path = fs::path(MergeTreeData::DETACHED_DIR_NAME) / p.dir_name;
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(current_info.database);
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(current_info.table);
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.partition_id : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.dir_name);
            if (columns_mask[src_index++])
            {
                chassert(has_bytes_on_disk_column);
                size_t bytes_on_disk = parts_sizes.at(p_id - begin).load();
                new_columns[res_index++]->insert(bytes_on_disk);
            }
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.disk->getName());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert((fs::path(current_info.data->getFullPathOnDisk(p.disk)) / detached_part_path).string());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.prefix : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.min_block : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.max_block : Field());
            if (columns_mask[src_index++])
                new_columns[res_index++]->insert(p.valid_name ? p.level : Field());
        }

        detached_parts.resize(begin);

        return {std::move(new_columns), rows};
    }
};

}

StorageSystemDetachedParts::StorageSystemDetachedParts(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{{
        {"database",         std::make_shared<DataTypeString>()},
        {"table",            std::make_shared<DataTypeString>()},
        {"partition_id",     std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"name",             std::make_shared<DataTypeString>()},
        {"bytes_on_disk",    std::make_shared<DataTypeUInt64>()},
        {"disk",             std::make_shared<DataTypeString>()},
        {"path",             std::make_shared<DataTypeString>()},
        {"reason",           std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"min_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"max_block_number", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())},
        {"level",            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())}
    }});
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemDetachedParts::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t num_streams)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    NameSet names_set(column_names.begin(), column_names.end());

    Block header;
    std::vector<UInt8> columns_mask(sample_block.columns());

    for (size_t i = 0; i < columns_mask.size(); ++i)
    {
        if (names_set.contains(sample_block.getByPosition(i).name))
        {
            columns_mask[i] = 1;
            header.insert(sample_block.getByPosition(i));
        }
    }

    bool has_bytes_on_disk_column = names_set.contains("bytes_on_disk");

    auto state = std::make_shared<SourceState>(StoragesInfoStream(query_info, context));

    Pipe pipe;

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<DetachedPartsSource>(header.cloneEmpty(), state, columns_mask, max_block_size, has_bytes_on_disk_column);
        pipe.addSource(std::move(source));
    }

    return pipe;
}

}
