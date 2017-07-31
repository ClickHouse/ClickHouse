#include <map>

#include <Common/Exception.h>

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Storages/StorageMemory.h>


namespace DB
{

class MemoryBlockInputStream : public IProfilingBlockInputStream
{
public:
    MemoryBlockInputStream(const Names & column_names_, BlocksList::iterator begin_, BlocksList::iterator end_)
        : column_names(column_names_), begin(begin_), end(end_), it(begin) {}

    String getName() const override { return "Memory"; }

    String getID() const override
    {
        std::stringstream res;
        res << "Memory(" << &*begin << ", " << &*end;

        for (const auto & name : column_names)
            res << ", " << name;

        res << ")";
        return res.str();
    }

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
};


class MemoryBlockOutputStream : public IBlockOutputStream
{
public:
    MemoryBlockOutputStream(StorageMemory & storage_) : storage(storage_) {}

    void write(const Block & block) override
    {
        storage.check(block, true);
        std::lock_guard<std::mutex> lock(storage.mutex);
        storage.data.push_back(block);
    }
private:
    StorageMemory & storage;
};


StorageMemory::StorageMemory(
    const std::string & name_,
    NamesAndTypesListPtr columns_)
    : name(name_), columns(columns_)
{
}


StorageMemory::StorageMemory(
    const std::string & name_,
    NamesAndTypesListPtr columns_,
    const NamesAndTypesList & materialized_columns_,
    const NamesAndTypesList & alias_columns_,
    const ColumnDefaults & column_defaults_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_},
    name(name_), columns(columns_)
{
}


BlockInputStreams StorageMemory::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

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

        res.push_back(std::make_shared<MemoryBlockInputStream>(column_names, begin, end));
    }

    return res;
}


BlockOutputStreamPtr StorageMemory::write(
    const ASTPtr & query, const Settings & settings)
{
    return std::make_shared<MemoryBlockOutputStream>(*this);
}


void StorageMemory::drop()
{
    std::lock_guard<std::mutex> lock(mutex);
    data.clear();
}

}
