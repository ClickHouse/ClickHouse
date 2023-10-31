#include <memory>
#include <Storages/System/StorageSystemRpmalloc.h>
#include <Poco/Logger.h>

#include "Columns/IColumn.h"
#include "Core/NamesAndTypes.h"
#include "Core/QueryProcessingStage.h"
#include "DataTypes/DataTypesNumber.h"
#include "Interpreters/Context_fwd.h"
#include "Processors/Sources/SourceFromSingleChunk.h"
#include "Storages/ColumnsDescription.h"
#include "Storages/SelectQueryInfo.h"
#include "Storages/StorageInMemoryMetadata.h"
#include "Storages/StorageSnapshot.h"
#include "base/types.h"
#include "config.h"

#if USE_RPMALLOC
#    include <rpmalloc/rpmalloc.h>
#endif

namespace DB
{

#if USE_RPMALLOC

void fillRpmallocStats(MutableColumns & res_columns)
{
    rpmalloc_global_statistics_t stats{};
    rpmalloc_global_statistics(&stats);

    size_t col_num = 0;
    res_columns.at(col_num++)->insert(UInt64(stats.mapped));
    res_columns.at(col_num++)->insert(UInt64(stats.mapped_peak));
    res_columns.at(col_num++)->insert(UInt64(stats.mapped_total));
    res_columns.at(col_num++)->insert(UInt64(stats.unmapped_total));
    res_columns.at(col_num++)->insert(UInt64(stats.huge_alloc));
    res_columns.at(col_num++)->insert(UInt64(stats.huge_alloc_peak));
    res_columns.at(col_num++)->insert(UInt64(stats.cached));
}

#else

void fillRpmallocStats(MutableColumns &)
{
    LOG_INFO(&Poco::Logger::get("StorageSystemRpmallocStats"), "rpmalloc is not enabled");
}

#endif


StorageSystemRpmallocStats::StorageSystemRpmallocStats(const StorageID & table_id_) : IStorage(table_id_)
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

NamesAndTypesList StorageSystemRpmallocStats::getNamesAndTypes()
{
    return {
        {"mapped", std::make_shared<DataTypeUInt64>()},
        {"mapped_peak", std::make_shared<DataTypeUInt64>()},
        {"mapped_total", std::make_shared<DataTypeUInt64>()},
        {"unmapped_total", std::make_shared<DataTypeUInt64>()},
        {"huge_alloc", std::make_shared<DataTypeUInt64>()},
        {"huge_alloc_peak", std::make_shared<DataTypeUInt64>()},
        {"cached", std::make_shared<DataTypeUInt64>()},
    };
}

Pipe StorageSystemRpmallocStats::read(
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

    fillRpmallocStats(res_columns);

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}

}
