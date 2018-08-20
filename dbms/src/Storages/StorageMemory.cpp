#include <Common/Exception.h>

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Storages/StorageMemory.h>
#include <Storages/StorageFactory.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class MemoryBlockInputStream : public IProfilingBlockInputStream
{
public:
    MemoryBlockInputStream(const Names & column_names_, BlocksList::iterator begin_, BlocksList::iterator end_, const StorageMemory & storage_)
        : column_names(column_names_), begin(begin_), end(end_), it(begin), storage(storage_) {}

    String getName() const override { return "Memory"; }

    Block getHeader() const override { return storage.getSampleBlockForColumns(column_names); }

protected:
    Block readImpl() override
    {
        if (it == end)
        {
            return Block();
        }
        else
        {
            Block src = *it;
            Block res;

            /// Add only required columns to `res`.
            for (size_t i = 0, size = column_names.size(); i < size; ++i)
                res.insert(src.getByName(column_names[i]));

            ++it;
            return res;
        }
    }
private:
    Names column_names;
    BlocksList::iterator begin;
    BlocksList::iterator end;
    BlocksList::iterator it;
    const StorageMemory & storage;
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
    explicit MemoryBlockOutputStream(StorageMemory & storage_) : storage(storage_) {}

    Block getHeader() const override { return storage.getSampleBlock(); }

    void write(const Block & block) override
    {
        storage.check(block, true);
        std::lock_guard<std::mutex> lock(storage.mutex);
        storage.data.push_back(block);
    }
private:
    StorageMemory & storage;
};


StorageMemory::StorageMemory(String table_name_, ColumnsDescription columns_description_)
    : IStorage{std::move(columns_description_)}, table_name(std::move(table_name_))
{
}


BlockInputStreams StorageMemory::read(
    const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    checkQueryProcessingStage(processed_stage, context);
    check(column_names);

    std::lock_guard<std::mutex> lock(mutex);

    size_t size = data.size();

    if (num_streams > size)
        num_streams = size;

    BlockInputStreams res;

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        BlocksList::iterator begin = data.begin();
        BlocksList::iterator end = data.begin();

        std::advance(begin, stream * size / num_streams);
        std::advance(end, (stream + 1) * size / num_streams);

        res.push_back(std::make_shared<MemoryBlockInputStream>(column_names, begin, end, *this));
    }

    return res;
}


BlockOutputStreamPtr StorageMemory::write(
    const ASTPtr & /*query*/, const Settings & /*settings*/)
{
    return std::make_shared<MemoryBlockOutputStream>(*this);
}


void StorageMemory::drop()
{
    std::lock_guard<std::mutex> lock(mutex);
    data.clear();
}

void StorageMemory::truncate(const ASTPtr &)
{
    std::lock_guard<std::mutex> lock(mutex);
    data.clear();
}


void registerStorageMemory(StorageFactory & factory)
{
    factory.registerStorage("Memory", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return StorageMemory::create(args.table_name, args.columns);
    });
}

}
