#include <Storages/System/StorageSystemZeros.h>
#include <Storages/SelectQueryInfo.h>

#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>


namespace DB
{

namespace
{

struct ZerosState
{
    explicit ZerosState(UInt64 limit) : add_total_rows(limit) { }
    std::atomic<UInt64> num_generated_rows = 0;
    std::atomic<UInt64> add_total_rows = 0;
};

using ZerosStatePtr = std::shared_ptr<ZerosState>;


/// Source which generates zeros.
/// Uses state to share the number of generated rows between threads.
/// If state is nullptr, then limit is ignored.
class ZerosSource : public ISource
{
public:
    ZerosSource(UInt64 block_size, UInt64 limit_, ZerosStatePtr state_)
            : ISource(createHeader()), limit(limit_), state(std::move(state_))
    {
        column = createColumn(block_size);
    }

    String getName() const override { return "Zeros"; }

protected:
    Chunk generate() override
    {
        auto column_ptr = column;
        size_t column_size = column_ptr->size();

        UInt64 total_rows = state->add_total_rows.fetch_and(0);
        if (total_rows)
            addTotalRowsApprox(total_rows);

        if (limit)
        {
            auto generated_rows = state->num_generated_rows.fetch_add(column_size, std::memory_order_acquire);
            if (generated_rows >= limit)
                return {};

            if (generated_rows + column_size > limit)
            {
                column_size = limit - generated_rows;
                column_ptr = createColumn(column_size);
            }
        }

        progress(column->size(), column->byteSize());

        return { Columns {std::move(column_ptr)}, column_size };
    }

private:
    UInt64 limit;
    ZerosStatePtr state;
    ColumnPtr column;

    static Block createHeader()
    {
        return { ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "zero") };
    }

    static ColumnPtr createColumn(size_t size)
    {
        auto column_ptr = ColumnUInt8::create();
        /// It is probably the fastest method to create zero column, cause resize_fill uses memset internally.
        column_ptr->getData().resize_fill(size);

        return column_ptr;
    }
};

}

StorageSystemZeros::StorageSystemZeros(const StorageID & table_id_, bool multithreaded_, std::optional<UInt64> limit_)
    : IStorage(table_id_), multithreaded(multithreaded_), limit(limit_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription({{"zero", std::make_shared<DataTypeUInt8>(), "dummy"}}));
    setInMemoryMetadata(storage_metadata);

}

Pipe StorageSystemZeros::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    UInt64 query_limit = limit ? *limit : 0;
    if (query_info.trivial_limit)
        query_limit = query_limit ? std::min(query_limit, query_info.trivial_limit) : query_info.trivial_limit;

    if (query_limit && query_limit < max_block_size)
        max_block_size = query_limit;

    if (!multithreaded)
        num_streams = 1;
    else if (query_limit && num_streams * max_block_size > query_limit)
        /// We want to avoid spawning more streams than necessary
        num_streams = std::min(num_streams, static_cast<size_t>(((query_limit + max_block_size - 1) / max_block_size)));

    ZerosStatePtr state = std::make_shared<ZerosState>(query_limit);

    Pipe res;
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<ZerosSource>(max_block_size, query_limit, state);
        res.addSource(std::move(source));
    }

    return res;
}

}
