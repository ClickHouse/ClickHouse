#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemSymbols.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Common/SymbolIndex.h>


namespace DB
{


StorageSystemSymbols::StorageSystemSymbols(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"symbol", std::make_shared<DataTypeString>(), "Symbol name in the binary. It is mangled. You can apply demangle(symbol) to obtain a readable name."},
        {"address_begin", std::make_shared<DataTypeUInt64>(), "Start address of the symbol in the binary."},
        {"address_end", std::make_shared<DataTypeUInt64>(), "End address of the symbol in the binary."},
    }));
    setInMemoryMetadata(storage_metadata);
}


namespace
{

class SymbolsBlockSource : public ISource
{
private:
    using Iterator = std::vector<SymbolIndex::Symbol>::const_iterator;
    Iterator it;
    const Iterator end;
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;

public:
    SymbolsBlockSource(
        Iterator begin_,
        Iterator end_,
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_)
        : ISource(std::move(header))
        , it(begin_), end(end_), columns_mask(std::move(columns_mask_)), max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "Symbols"; }

protected:
    Chunk generate() override
    {
        if (it == end)
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

        size_t rows_count = 0;
        while (rows_count < max_block_size && it != end)
        {
            size_t src_index = 0;
            size_t res_index = 0;

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(it->name);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(reinterpret_cast<uintptr_t>(it->address_begin));
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(reinterpret_cast<uintptr_t>(it->address_end));

            ++rows_count;
            ++it;
        }

        return Chunk(std::move(res_columns), rows_count);
    }
};

}


Pipe StorageSystemSymbols::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /* query_info */,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    context->getAccess()->checkAccess(AccessType::INTROSPECTION);

    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    auto [columns_mask, res_block] = getQueriedColumnsMaskAndHeader(sample_block, column_names);

    const auto & symbols = SymbolIndex::instance().symbols();

    return Pipe(std::make_shared<SymbolsBlockSource>(
        symbols.cbegin(), symbols.cend(), std::move(columns_mask), std::move(res_block), max_block_size));
}

}

#endif
