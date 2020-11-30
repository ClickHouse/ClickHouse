#include <Common/Exception.h>

#include <DataStreams/IBlockInputStream.h>

#include <Storages/StorageMemory.h>
#include <Storages/StorageFactory.h>

#include <IO/WriteHelpers.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class MemorySource : public SourceWithProgress
{
public:
    /// We use range [first, last] which includes right border.
    /// Blocks are stored in std::list which may be appended in another thread.
    /// We don't use synchronisation here, because elements in range [first, last] won't be modified.
    MemorySource(
        Names column_names_,
        BlocksList::iterator first_,
        size_t num_blocks_,
        const StorageMemory & storage,
        const StorageMetadataPtr & metadata_snapshot)
        : SourceWithProgress(metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names(std::move(column_names_))
        , current_it(first_)
        , num_blocks(num_blocks_)
    {
    }

    /// If called, will initialize the number of blocks at first read.
    /// It allows to read data which was inserted into memory table AFTER Storage::read was called.
    /// This hack is needed for global subqueries.
    void delayInitialization(BlocksList * data_, std::mutex * mutex_)
    {
        data = data_;
        mutex = mutex_;
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (data)
        {
            std::lock_guard guard(*mutex);
            current_it = data->begin();
            num_blocks = data->size();
            is_finished = num_blocks == 0;

            data = nullptr;
            mutex = nullptr;
        }

        if (is_finished)
        {
            return {};
        }
        else
        {
            const Block & src = *current_it;
            Columns columns;
            columns.reserve(column_names.size());

            /// Add only required columns to `res`.
            for (const auto & name : column_names)
                columns.emplace_back(src.getByName(name).column);

            ++current_block_idx;

            if (current_block_idx == num_blocks)
                is_finished = true;
            else
                ++current_it;

            return Chunk(std::move(columns), src.rows());
        }
    }
private:
    Names column_names;
    BlocksList::iterator current_it;
    size_t current_block_idx = 0;
    size_t num_blocks;
    bool is_finished = false;

    BlocksList * data = nullptr;
    std::mutex * mutex = nullptr;
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
    explicit MemoryBlockOutputStream(
        StorageMemory & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {}

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        metadata_snapshot->check(block, true);
        std::lock_guard lock(storage.mutex);
        storage.data.push_back(block);
    }
private:
    StorageMemory & storage;
    StorageMetadataPtr metadata_snapshot;
};


StorageMemory::StorageMemory(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_description_));
    storage_metadata.setConstraints(std::move(constraints_));
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageMemory::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    std::lock_guard lock(mutex);

    if (delay_read_for_global_subqueries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        auto source = std::make_shared<MemorySource>(column_names, data.begin(), data.size(), *this, metadata_snapshot);
        source->delayInitialization(&data, &mutex);
        return Pipe(std::move(source));
    }

    size_t size = data.size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    BlocksList::iterator it = data.begin();

    size_t offset = 0;
    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        size_t next_offset = (stream + 1) * size / num_streams;
        size_t num_blocks = next_offset - offset;

        assert(num_blocks > 0);

        pipes.emplace_back(std::make_shared<MemorySource>(column_names, it, num_blocks, *this, metadata_snapshot));

        while (offset < next_offset)
        {
            ++it;
            ++offset;
        }
    }

    return Pipe::unitePipes(std::move(pipes));
}


BlockOutputStreamPtr StorageMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<MemoryBlockOutputStream>(*this, metadata_snapshot);
}


void StorageMemory::drop()
{
    std::lock_guard lock(mutex);
    data.clear();
}

void StorageMemory::truncate(
    const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &)
{
    std::lock_guard lock(mutex);
    data.clear();
}

std::optional<UInt64> StorageMemory::totalRows() const
{
    UInt64 rows = 0;
    std::lock_guard lock(mutex);
    for (const auto & buffer : data)
        rows += buffer.rows();
    return rows;
}

std::optional<UInt64> StorageMemory::totalBytes() const
{
    UInt64 bytes = 0;
    std::lock_guard lock(mutex);
    for (const auto & buffer : data)
        bytes += buffer.allocatedBytes();
    return bytes;
}


void registerStorageMemory(StorageFactory & factory)
{
    factory.registerStorage("Memory", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageMemory::create(args.table_id, args.columns, args.constraints);
    });
}

}
