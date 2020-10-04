#include <Common/Exception.h>

#include <DataStreams/IBlockInputStream.h>

#include <Interpreters/MutationsInterpreter.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMemory.h>

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
        BlocksList::const_iterator first_,
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
    void delayInitialization(std::shared_ptr<const BlocksList> data_) { data = data_; }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (data)
        {
            current_it = data->begin();
            num_blocks = data->size();
            is_finished = num_blocks == 0;

            data = nullptr;
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
    BlocksList::const_iterator current_it;
    size_t current_block_idx = 0;
    size_t num_blocks;
    bool is_finished = false;

    std::shared_ptr<const BlocksList> data = nullptr;
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
        auto new_data = std::make_unique<BlocksList>(*(storage.data.get()));
        new_data->push_back(block);
        storage.data.set(std::move(new_data));
    }
private:
    StorageMemory & storage;
    StorageMetadataPtr metadata_snapshot;
};


StorageMemory::StorageMemory(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_)
    : IStorage(table_id_), data(std::make_unique<const BlocksList>())
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

    auto current_data = data.get();

    if (delay_read_for_global_subqueries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        auto source = std::make_shared<MemorySource>(column_names, current_data->begin(), current_data->size(), *this, metadata_snapshot);
        source->delayInitialization(current_data);
        return Pipe(std::move(source));
    }

    size_t size = current_data->size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    auto it = current_data->begin();

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
    data.set(std::make_unique<BlocksList>());
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

void StorageMemory::mutate(const MutationCommands & commands, const Context & context)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, context);
    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, context, true);
    auto in = interpreter->execute();

    in->readPrefix();
    BlocksList out;
    Block block;
    while ((block = in->read()))
    {
        out.push_back(block);
    }
    in->readSuffix();

    // all column affected
    if (interpreter->isAffectingAllColumns())
    {
        data.set(std::make_unique<BlocksList>(out));
    }
    else
    {
        auto new_data = std::make_unique<BlocksList>(*(data.get()));
        auto data_it = new_data->begin();
        auto out_it = out.begin();
        while (data_it != new_data->end() && out_it != out.end())
        {
            updateBlockData(*data_it, *out_it);
            ++data_it;
            ++out_it;
        }
        data.set(std::move(new_data));
    }
}

void StorageMemory::truncate(
    const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &)
{
    data.set(std::make_unique<BlocksList>());
}

std::optional<UInt64> StorageMemory::totalRows() const
{
    UInt64 rows = 0;
    auto current_data = data.get();
    for (const auto & buffer : *current_data)
        rows += buffer.rows();
    return rows;
}

std::optional<UInt64> StorageMemory::totalBytes() const
{
    UInt64 bytes = 0;
    auto current_data = data.get();
    for (const auto & buffer : *current_data)
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
