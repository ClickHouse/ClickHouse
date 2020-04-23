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
    MemorySource(Names column_names_, BlocksList::iterator begin_, BlocksList::iterator end_, const StorageMemory & storage)
        : SourceWithProgress(storage.getSampleBlockForColumns(column_names_))
        , column_names(std::move(column_names_)), begin(begin_), end(end_), it(begin) {}

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (it == end)
        {
            return {};
        }
        else
        {
            Block src = *it;
            Columns columns;
            columns.reserve(column_names.size());

            /// Add only required columns to `res`.
            for (const auto & name : column_names)
                columns.emplace_back(src.getByName(name).column);

            ++it;
            return Chunk(std::move(columns), src.rows());
        }
    }
private:
    Names column_names;
    BlocksList::iterator begin;
    BlocksList::iterator end;
    BlocksList::iterator it;
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
    explicit MemoryBlockOutputStream(StorageMemory & storage_) : storage(storage_) {}

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        storage.check(block, true);
        std::lock_guard lock(storage.mutex);
        storage.data.push_back(block);
    }
private:
    StorageMemory & storage;
};


StorageMemory::StorageMemory(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_)
    : IStorage(table_id_)
{
    setColumns(std::move(columns_description_));
    setConstraints(std::move(constraints_));
}


Pipes StorageMemory::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    check(column_names);

    std::lock_guard lock(mutex);

    size_t size = data.size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        BlocksList::iterator begin = data.begin();
        BlocksList::iterator end = data.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        pipes.emplace_back(std::make_shared<MemorySource>(column_names, begin, end, *this));
    }

    return pipes;
}


BlockOutputStreamPtr StorageMemory::write(
    const ASTPtr & /*query*/, const Context & /*context*/)
{
    return std::make_shared<MemoryBlockOutputStream>(*this);
}


void StorageMemory::drop()
{
    std::lock_guard lock(mutex);
    data.clear();
}

void StorageMemory::truncate(const ASTPtr &, const Context &, TableStructureWriteLockHolder &)
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
        bytes += buffer.bytes();
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
