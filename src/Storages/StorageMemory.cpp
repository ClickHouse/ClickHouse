#include <cassert>
#include <Common/Exception.h>

#include <DataStreams/IBlockInputStream.h>

#include <Interpreters/MutationsInterpreter.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMemory.h>
#include <Storages/MemorySettings.h>

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
    using InitializerFunc = std::function<void(std::shared_ptr<const LazyBlocks> &)>;
public:
    /// Blocks are stored in std::list which may be appended in another thread.
    /// We use pointer to the beginning of the list and its current size.
    /// We don't need synchronisation in this reader, because while we hold SharedLock on storage,
    /// only new elements can be added to the back of the list, so our iterators remain valid

    MemorySource(
        Names column_names_,
        const StorageMemory & storage,
        const StorageMetadataPtr & metadata_snapshot,
        std::shared_ptr<const LazyBlocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {})
        : SourceWithProgress(metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names_and_types(metadata_snapshot->getColumns().getAllWithSubcolumns().addTypes(std::move(column_names_)))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
    {
        for (const auto & elem : column_names_and_types)
            column_positions.push_back(metadata_snapshot->getSampleBlock().getPositionByName(elem.getNameInStorage()));
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

        if (current_index >= data->size())
        {
            return {};
        }

        const LazyBlock & src = (*data)[current_index];
        Columns columns;
        columns.reserve(columns.size());

        /// Add only required columns to `res`.
        size_t i = 0;
        for (const auto & elem : column_names_and_types)
        {
            auto current_column = src[column_positions[i]]();
            if (elem.isSubcolumn())
                columns.emplace_back(elem.getTypeInStorage()->getSubcolumn(elem.getSubcolumnName(), *current_column));
            else
                columns.emplace_back(std::move(current_column));

            ++i;
        }

        size_t rows = columns.at(0)->size();
        return Chunk(std::move(columns), rows);
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
    std::shared_ptr<const LazyBlocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
    std::vector<size_t> column_positions;
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
    MemoryBlockOutputStream(
        StorageMemory & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {
    }

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        metadata_snapshot->check(block, true);

        inserted_bytes += block.allocatedBytes();
        inserted_rows += block.rows();

        Block sample = metadata_snapshot->getSampleBlock();

        LazyColumns lazy_columns;
        lazy_columns.reserve(sample.columns());

        for (const auto & elem : sample)
        {
            const ColumnPtr & column = block.getByName(elem.name).column;

            if (storage.compress)
                lazy_columns.emplace_back(column->compress());
            else
                lazy_columns.emplace_back([=]{ return column; });
        }

        new_blocks.emplace_back(std::move(lazy_columns));
    }

    void writeSuffix() override
    {
        std::lock_guard lock(storage.mutex);
        auto new_data = std::make_unique<LazyBlocks>(*(storage.data.get()));
        new_data->insert(new_data->end(), new_blocks.begin(), new_blocks.end());

        storage.data.set(std::move(new_data));
        storage.total_size_bytes.fetch_add(inserted_bytes, std::memory_order_relaxed);
        storage.total_size_rows.fetch_add(inserted_rows, std::memory_order_relaxed);
    }

private:
    LazyBlocks new_blocks;
    size_t inserted_bytes = 0;
    size_t inserted_rows = 0;

    StorageMemory & storage;
    StorageMetadataPtr metadata_snapshot;
};


StorageMemory::StorageMemory(
    const StorageID & table_id_,
    ColumnsDescription columns_description_,
    ConstraintsDescription constraints_,
    bool compress_)
    : IStorage(table_id_), data(std::make_unique<const LazyBlocks>()), compress(compress_)
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
            *this,
            metadata_snapshot,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [this](std::shared_ptr<const LazyBlocks> & data_to_initialize)
            {
                data_to_initialize = data.get();
            }));
    }

    auto current_data = data.get();
    size_t size = current_data->size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        pipes.emplace_back(std::make_shared<MemorySource>(column_names, *this, metadata_snapshot, current_data, parallel_execution_index));
    }

    return Pipe::unitePipes(std::move(pipes));
}


BlockOutputStreamPtr StorageMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<MemoryBlockOutputStream>(*this, metadata_snapshot);
}


void StorageMemory::drop()
{
    data.set(std::make_unique<LazyBlocks>());
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);
}

static inline void updateBlockData(LazyBlock & old_block, const LazyBlock & new_block, const Block & old_header, const Block & new_header)
{
    size_t i = 0;
    for (const auto & it : new_header)
    {
        old_block[old_header.getPositionByName(it.name)] = new_block[i];
        ++i;
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
    Block old_header = metadata_snapshot->getSampleBlock();
    Block mutation_header = in->getHeader();

    in->readPrefix();
    LazyBlocks out;
    while (Block block = in->read())
    {
        LazyColumns lazy_columns;

        for (const auto & elem : block)
        {
            if (compress)
                lazy_columns.emplace_back(elem.column->compress());
            else
                lazy_columns.emplace_back([=]{ return elem.column; });
        }

        out.emplace_back(std::move(lazy_columns));
    }
    in->readSuffix();

    std::unique_ptr<LazyBlocks> new_data;

    /// All columns affected.
    if (interpreter->isAffectingAllColumns())
    {
        new_data = std::make_unique<LazyBlocks>(out);
    }
    else
    {
        /// Just some of the columns affected, we need update it with new column.
        new_data = std::make_unique<LazyBlocks>(*(data.get()));
        auto data_it = new_data->begin();
        auto out_it = out.begin();

        while (data_it != new_data->end())
        {
            /// Mutation does not change the number of blocks.
            assert(out_it != out.end());

            updateBlockData(*data_it, *out_it, old_header, mutation_header);
            ++data_it;
            ++out_it;
        }

        assert(out_it == out.end());
    }

/*    size_t rows = 0;
    size_t bytes = 0;
    for (const auto & buffer : *new_data)
    {
        rows += buffer.rows();
        bytes += buffer.bytes();
    }
    total_size_bytes.store(rows, std::memory_order_relaxed);
    total_size_rows.store(bytes, std::memory_order_relaxed);*/

    data.set(std::move(new_data));
}


void StorageMemory::truncate(
    const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &)
{
    data.set(std::make_unique<LazyBlocks>());
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
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        bool has_settings = args.storage_def->settings;
        MemorySettings settings;
        if (has_settings)
            settings.loadFromQuery(*args.storage_def);

        return StorageMemory::create(args.table_id, args.columns, args.constraints, settings.compress);
    },
    {
        .supports_settings = true,
        .supports_parallel_insert = true,
    });
}

}
