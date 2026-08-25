#if (defined(__ELF__) && !defined(OS_FREEBSD)) || defined(OS_DARWIN)

#include <base/demangle.h>
#include <Common/SystemTableDocumentation.h>
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
        : ISource(std::make_shared<const Block>(std::move(header)))
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

#if USE_XRAY
        const auto & instrumentation_functions = InstrumentationManager::instance().getFunctions();
#endif

        size_t rows_count = 0;
        while (rows_count < max_block_size && it != end)
        {
            size_t src_index = 0;
            size_t res_index = 0;

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(it->name);
#if USE_XRAY
            const auto function_name = demangle(it->name);
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
                res_columns[res_index++]->insert(reinterpret_cast<uintptr_t>(it->offset_begin));
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(reinterpret_cast<uintptr_t>(it->offset_end));

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


/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemSymbols) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "symbols",
    .description = R"DOCS_MD(
Contains information for introspection of `clickhouse` binary. It requires the introspection privilege to access.
This table is only useful for C++ experts and ClickHouse engineers.
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql
SELECT
    symbol,
    demangle(symbol) AS symbol_demangled,
    address_begin,
    address_end
FROM system.symbols
LIMIT 5
SETTINGS allow_introspection_functions = 1;
```

```text
Row 1:
──────
symbol:           _Z15isClickHouseAppNSt3__117basic_string_viewIcNS_11char_traitsIcEEEERNS_6vectorIPcNS_9allocatorIS5_EEEE
symbol_demangled: isClickHouseApp(std::__1::basic_string_view<char, std::__1::char_traits<char>>, std::__1::vector<char*, std::__1::allocator<char*>>&)
address_begin:    219229312 -- 219.23 million
address_end:      219231408 -- 219.23 million

Row 2:
──────
symbol:           main
symbol_demangled: main
address_begin:    219231872 -- 219.23 million
address_end:      219233485 -- 219.23 million

Row 3:
──────
symbol:           _ZN12_GLOBAL__N_19printHelpEiPPc
symbol_demangled: (anonymous namespace)::printHelp(int, char**)
address_begin:    219233536 -- 219.23 million
address_end:      219233902 -- 219.23 million

Row 4:
──────
symbol:           _ZNSt3__110filesystem4pathC2B8se210105IPcvEERKT_NS1_6formatE
symbol_demangled: std::__1::filesystem::path::path[abi:se210105]<char*, void>(char* const&, std::__1::filesystem::path::format)
address_begin:    219234496 -- 219.23 million
address_end:      219234620 -- 219.23 million

Row 5:
──────
symbol:           _ZNSt3__113unordered_setINS_17basic_string_viewIcNS_11char_traitsIcEEEENS_4hashIS4_EENS_8equal_toIS4_EENS_9allocatorIS4_EEEC2ESt16initializer_listIS4_E
symbol_demangled: std::__1::unordered_set<std::__1::basic_string_view<char, std::__1::char_traits<char>>, std::__1::hash<std::__1::basic_string_view<char, std::__1::char_traits<char>>>, std::__1::equal_to<std::__1::basic_string_view<char, std::__1::char_traits<char>>>, std::__1::allocator<std::__1::basic_string_view<char, std::__1::char_traits<char>>>>::unordered_set(std::initializer_list<std::__1::basic_string_view<char, std::__1::char_traits<char>>>)
address_begin:    219235584 -- 219.24 million
address_end:      219235708 -- 219.24 million
```
)DOCS_MD")

}

#endif
