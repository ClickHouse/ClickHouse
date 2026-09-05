#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)

#include <base/demangle.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemSymbols.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/InstrumentationManager.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Common/SymbolIndex.h>


namespace DB
{


StorageSystemSymbols::StorageSystemSymbols(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"symbol", std::make_shared<DataTypeString>(), "Symbol name in the binary. It is mangled. You can apply demangle(symbol) to obtain a readable name."},
#if USE_XRAY
        {"symbol_demangled", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Demangled symbol used for XRay instrumentation."},
        {"function_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "Function ID in the XRay instrumentation map."},
#endif
        {"address_begin", std::make_shared<DataTypeUInt64>(), "Start address of the symbol in the binary."},
        {"address_end", std::make_shared<DataTypeUInt64>(), "End address of the symbol in the binary."},
    }));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemSymbols::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


namespace
{

class SymbolsBlockSource final : public ISource
{
private:
    SymbolIndex::SymbolIterator iterator;
    bool iteration_finished = false;
    std::vector<UInt8> columns_mask;
    UInt64 max_block_size;

public:
    SymbolsBlockSource(
        const SymbolIndex & symbol_index,
        std::vector<UInt8> columns_mask_,
        Block header,
        UInt64 max_block_size_)
        : ISource(std::make_shared<const Block>(std::move(header)))
        , iterator(symbol_index.iterateSymbols()), columns_mask(std::move(columns_mask_)), max_block_size(max_block_size_)
    {
    }

    String getName() const override { return "Symbols"; }

protected:
    Chunk generate() override
    {
        if (iteration_finished)
            return {};

        MutableColumns res_columns = getPort().getHeader().cloneEmptyColumns();

#if USE_XRAY
        const auto & instrumentation_functions = InstrumentationManager::instance().getFunctions();
#endif

        size_t rows_count = 0;
        while (rows_count < max_block_size)
        {
            const SymbolIndex::Symbol * symbol = nullptr;
            std::string_view symbol_name;
            if (!iterator.next(symbol, symbol_name))
            {
                iteration_finished = true;
                break;
            }

            size_t src_index = 0;
            size_t res_index = 0;

            if (columns_mask[src_index++])
                res_columns[res_index++]->insertData(symbol_name.empty() ? "" : symbol_name.data(), symbol_name.size());
#if USE_XRAY
            const char * symbol_name_c_string = SymbolIndex::instance().getSymbolNameCString(*symbol);
            const auto function_name = *symbol_name_c_string ? demangle(symbol_name_c_string) : String{};
            const auto instrumentation_function = instrumentation_functions.get<InstrumentationManager::FunctionName>().find(function_name);

            /// Not every function is instrumented, so we need to look for those which are.
            if (instrumentation_function != instrumentation_functions.get<InstrumentationManager::FunctionName>().end())
            {
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(instrumentation_function->function_name);
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(instrumentation_function->function_id);
            }
            else
            {
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(Field());
                if (columns_mask[src_index++])
                    res_columns[res_index++]->insert(Field());
            }
#endif
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(reinterpret_cast<uintptr_t>(symbol->offset_begin));
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(reinterpret_cast<uintptr_t>(symbol->offset_end));

            ++rows_count;
        }

        if (!rows_count)
            return {};
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

    const auto & symbol_index = SymbolIndex::instance();

    return Pipe(std::make_shared<SymbolsBlockSource>(
        symbol_index, std::move(columns_mask), std::move(res_block), max_block_size));
}

}


/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemSymbols) }

#endif
