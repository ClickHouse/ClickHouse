#include <cassert>
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
    using InitializerFunc = std::function<void(BlocksList::const_iterator &, size_t &, std::shared_ptr<const BlocksList> &)>;
public:
    /// Blocks are stored in std::list which may be appended in another thread.
    /// We use pointer to the beginning of the list and its current size.
    /// We don't need synchronisation in this reader, because while we hold SharedLock on storage,
    /// only new elements can be added to the back of the list, so our iterators remain valid

    MemorySource(
        Names column_names_,
        BlocksList::const_iterator first_,
        size_t num_blocks_,
        const StorageMemory & storage,
        const StorageMetadataPtr & metadata_snapshot,
        std::shared_ptr<const BlocksList> data_,
        InitializerFunc initializer_func_ = [](BlocksList::const_iterator &, size_t &, std::shared_ptr<const BlocksList> &) {})
        : SourceWithProgress(metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names(std::move(column_names_))
        , current_it(first_)
        , num_blocks(num_blocks_)
        , data(data_)
        , initializer_func(std::move(initializer_func_))
    {
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (!postponed_init_done)
        {
            initializer_func(current_it, num_blocks, data);
            postponed_init_done = true;
        }

        if (current_block_idx == num_blocks)
            return {};

        const Block & src = *current_it;
        Columns columns;
        columns.reserve(column_names.size());

        /// Add only required columns to `res`.
        for (const auto & name : column_names)
            columns.push_back(src.getByName(name).column);

        if (++current_block_idx < num_blocks)
            ++current_it;

        return Chunk(std::move(columns), src.rows());
    }

private:
    const Names column_names;
    BlocksList::const_iterator current_it;
    size_t num_blocks;
    size_t current_block_idx = 0;

    std::shared_ptr<const BlocksList> data;
    bool postponed_init_done = false;
    InitializerFunc initializer_func;
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
        const auto size_bytes_diff = block.allocatedBytes();
        const auto size_rows_diff = block.rows();

        metadata_snapshot->check(block, true);
        {
            std::lock_guard lock(storage.mutex);
            auto new_data = std::make_unique<BlocksList>(*(storage.data.get()));
            new_data->push_back(block);
            storage.data.set(std::move(new_data));

            storage.total_size_bytes.fetch_add(size_bytes_diff, std::memory_order_relaxed);
            storage.total_size_rows.fetch_add(size_rows_diff, std::memory_order_relaxed);
        }

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
    SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

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
            data.get()->end(),
            0,
            *this,
            metadata_snapshot,
            data.get(),
            [this](BlocksList::const_iterator & current_it, size_t & num_blocks, std::shared_ptr<const BlocksList> & current_data)
            {
                current_data = data.get();
                current_it = current_data->begin();
                num_blocks = current_data->size();
            }));
    }

    auto current_data = data.get();

    size_t size = current_data->size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    BlocksList::const_iterator it = current_data->begin();

    size_t offset = 0;
    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        size_t next_offset = (stream + 1) * size / num_streams;
        size_t num_blocks = next_offset - offset;

        assert(num_blocks > 0);

        pipes.emplace_back(std::make_shared<MemorySource>(column_names, it, num_blocks, *this, metadata_snapshot, current_data));

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

void StorageMemory::mutate(const MutationCommands & commands, const Context & context)
{
    std::lock_guard lock(mutex);
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

    std::unique_ptr<BlocksList> new_data;

    // all column affected
    if (interpreter->isAffectingAllColumns())
    {
        new_data = std::make_unique<BlocksList>(out);
    }
    else
    {
        /// just some of the column affected, we need update it with new column
        new_data = std::make_unique<BlocksList>(*(data.get()));
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
    total_size_bytes.store(rows, std::memory_order_relaxed);
    total_size_rows.store(bytes, std::memory_order_relaxed);
    data.set(std::move(new_data));
}


void StorageMemory::truncate(
    const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &)
{
    data.set(std::make_unique<BlocksList>());
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);
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
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageMemory::create(args.table_id, args.columns, args.constraints);
    });
}

}
