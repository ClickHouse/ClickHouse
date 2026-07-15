#include "config.h"

#if USE_LANCE

#include <Common/Exception.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>

#include <arrow/table.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
}
}

namespace DB::Lance
{

ReadSource::ReadSource(
    const Block & header,
    ObjectInfoPtr object_info_,
    DatasetOptions options_,
    ScanDescription scan_)
    : ISource(std::make_shared<const Block>(header), false)
    , object_info(std::move(object_info_))
    , options(std::move(options_))
    , scan(std::move(scan_))
{
}

Chunk ReadSource::generate()
{
    if (is_finished)
        return {};

    if (!dataset)
        dataset.emplace(Dataset::open(options));

    if (scan.need_only_count && scan.projection.empty())
    {
        if (const auto rows = dataset->totalRows(scan.snapshot))
        {
            is_finished = true;
            return Chunk(Columns{}, *rows);
        }
    }

    if (!scan_handle)
        scan_handle.emplace(dataset->planScan(scan));

    auto record_batch = scan_handle->nextBatch();
    if (!record_batch)
    {
        is_finished = true;
        return {};
    }

    auto table = arrow::Table::FromRecordBatches({record_batch});
    if (!table.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to create Lance Arrow table: {}", table.status().ToString());

    if (!converter)
    {
        converter = std::make_unique<ArrowColumnToCHColumn>(
            getPort().getHeader(),
            "Lance",
            format_settings,
            /* parquet_columns_to_clickhouse */ std::nullopt,
            /* clickhouse_columns_to_parquet */ std::nullopt,
            /* allow_missing_columns */ false,
            format_settings.null_as_default,
            format_settings.date_time_overflow_behavior,
            /* allow_geoparquet_parser */ false,
            /* case_insensitive_matching */ false,
            /* is_stream */ true,
            /* enable_json_parsing */ false);
    }

    return converter->arrowTableToCHChunk(*table, (*table)->num_rows(), /* metadata */ nullptr, /* block_missing_values */ nullptr);
}

}

#endif
