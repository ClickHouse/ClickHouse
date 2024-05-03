#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemJemalloc.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Core/NamesAndTypes.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <fmt/core.h>

#include "config.h"

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif


namespace DB
{

#if USE_JEMALLOC

UInt64 getJeMallocValue(const char * name)
{
    UInt64 value{};
    size_t size = sizeof(value);
    mallctl(name, &value, &size, nullptr, 0);
    return value;
}

void fillJemallocBins(MutableColumns & res_columns)
{
    /// Bins for small allocations
    auto small_bins_count = getJeMallocValue("arenas.nbins");
    UInt16 bin_index = 0;
    for (UInt64 bin = 0; bin < small_bins_count; ++bin, ++bin_index)
    {
        auto size = getJeMallocValue(fmt::format("arenas.bin.{}.size", bin).c_str());
        auto ndalloc = getJeMallocValue(fmt::format("stats.arenas.{}.bins.{}.ndalloc", MALLCTL_ARENAS_ALL, bin).c_str());
        auto nmalloc = getJeMallocValue(fmt::format("stats.arenas.{}.bins.{}.nmalloc", MALLCTL_ARENAS_ALL, bin).c_str());

        size_t col_num = 0;
        res_columns.at(col_num++)->insert(bin_index);
        res_columns.at(col_num++)->insert(0);
        res_columns.at(col_num++)->insert(size);
        res_columns.at(col_num++)->insert(nmalloc);
        res_columns.at(col_num++)->insert(ndalloc);
    }

    /// Bins for large allocations
    auto large_bins_count = getJeMallocValue("arenas.nlextents");
    for (UInt64 bin = 0; bin < large_bins_count; ++bin, ++bin_index)
    {
        auto size = getJeMallocValue(fmt::format("arenas.lextent.{}.size", bin).c_str());
        auto ndalloc = getJeMallocValue(fmt::format("stats.arenas.{}.lextents.{}.ndalloc", MALLCTL_ARENAS_ALL, bin).c_str());
        auto nmalloc = getJeMallocValue(fmt::format("stats.arenas.{}.lextents.{}.nmalloc", MALLCTL_ARENAS_ALL, bin).c_str());

        size_t col_num = 0;
        res_columns.at(col_num++)->insert(bin_index);
        res_columns.at(col_num++)->insert(1);
        res_columns.at(col_num++)->insert(size);
        res_columns.at(col_num++)->insert(nmalloc);
        res_columns.at(col_num++)->insert(ndalloc);
    }
}

#else

void fillJemallocBins(MutableColumns &)
{
    LOG_INFO(&Poco::Logger::get("StorageSystemJemallocBins"), "jemalloc is not enabled");
}

#endif // USE_JEMALLOC


StorageSystemJemallocBins::StorageSystemJemallocBins(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    ColumnsDescription desc;
    auto columns = getNamesAndTypes();
    for (const auto & col : columns)
    {
        ColumnDescription col_desc(col.name, col.type);
        desc.add(col_desc);
    }
    storage_metadata.setColumns(desc);
    setInMemoryMetadata(storage_metadata);
}

NamesAndTypesList StorageSystemJemallocBins::getNamesAndTypes()
{
    return {
        { "index",          std::make_shared<DataTypeUInt16>() },
        { "large",          std::make_shared<DataTypeUInt8>() },
        { "size",           std::make_shared<DataTypeUInt64>() },
        { "allocations",    std::make_shared<DataTypeInt64>() },
        { "deallocations",  std::make_shared<DataTypeInt64>() },
    };
}

Pipe StorageSystemJemallocBins::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtuals());
    MutableColumns res_columns = header.cloneEmptyColumns();

    fillJemallocBins(res_columns);

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}

}
