#include "config.h"

#if USE_LANCE

#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceScanDescription.h>
#include <Common/Exception.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <ch_lance.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_EXCEPTION;
}
}

namespace DB::Lance
{

namespace
{

String takeError(ch_lance_error & error)
{
    String message = error.message ? error.message : "Unknown Lance error";
    ch_lance_free_error(&error);
    return message;
}

[[noreturn]] void throwLanceError(ch_lance_error & error)
{
    throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "{}", takeError(error));
}

}

Scan::Scan(Scan && other) noexcept
    : scan(std::exchange(other.scan, nullptr))
{
}

Scan & Scan::operator=(Scan && other) noexcept
{
    if (this != &other)
    {
        if (scan)
            ch_lance_free_scan(scan);
        scan = std::exchange(other.scan, nullptr);
    }
    return *this;
}

Scan::~Scan()
{
    if (scan)
        ch_lance_free_scan(scan);
}

std::shared_ptr<arrow::RecordBatch> Scan::nextBatch() const
{
    ArrowArray array{};
    ArrowSchema schema{};
    bool has_batch = false;
    ch_lance_error error{};
    if (!ch_lance_next_batch(scan, &array, &schema, &has_batch, &error))
        throwLanceError(error);
    if (!has_batch)
        return nullptr;

    auto record_batch = arrow::ImportRecordBatch(&array, &schema);
    if (!record_batch.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to import Lance Arrow record batch: {}", record_batch.status().ToString());

    return *record_batch;
}

Dataset::Dataset(Dataset && other) noexcept
    : dataset(std::exchange(other.dataset, nullptr))
{
}

Dataset & Dataset::operator=(Dataset && other) noexcept
{
    if (this != &other)
    {
        if (dataset)
            ch_lance_free_dataset(dataset);
        dataset = std::exchange(other.dataset, nullptr);
    }
    return *this;
}

Dataset::~Dataset()
{
    if (dataset)
        ch_lance_free_dataset(dataset);
}

Dataset Dataset::open(const DatasetOptions & options)
{
    ch_lance_dataset_options native_options
    {
        .uri = options.uri.c_str(),
        .use_s3 = options.use_s3,
        .s3_region = options.s3_region.c_str(),
        .s3_endpoint = options.s3_endpoint.c_str(),
        .s3_access_key_id = options.s3_access_key_id.c_str(),
        .s3_secret_access_key = options.s3_secret_access_key.c_str(),
        .s3_session_token = options.s3_session_token.c_str(),
        .s3_role_arn = options.s3_role_arn.c_str(),
        .s3_role_session_name = options.s3_role_session_name.c_str(),
        .s3_use_environment_credentials = options.s3_use_environment_credentials,
        .s3_no_sign_request = options.s3_no_sign_request,
        .s3_allow_http = options.s3_allow_http,
        .s3_virtual_hosted_style_request = options.s3_virtual_hosted_style_request,
    };

    ch_lance_error error{};
    auto * dataset = ch_lance_open_dataset(&native_options, &error);
    if (!dataset)
        throwLanceError(error);
    return Dataset(dataset);
}

SnapshotInfo Dataset::currentSnapshot() const
{
    ch_lance_snapshot_info snapshot{};
    ch_lance_error error{};
    if (!ch_lance_current_snapshot(dataset, &snapshot, &error))
        throwLanceError(error);
    return SnapshotInfo{snapshot.snapshot_id, snapshot.schema_id};
}

NamesAndTypesList Dataset::tableSchema(const TableStateSnapshot & snapshot, ContextPtr) const
{
    ArrowSchema schema{};
    ch_lance_error error{};
    if (!ch_lance_export_schema(dataset, snapshot.snapshot_id, &schema, &error))
        throwLanceError(error);

    auto arrow_schema = arrow::ImportSchema(&schema);
    if (!arrow_schema.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to import Lance Arrow schema: {}", arrow_schema.status().ToString());

    const auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(**arrow_schema, nullptr, "Lance", FormatSettings{});
    return header.getNamesAndTypesList();
}

std::optional<size_t> Dataset::totalRows(const TableStateSnapshot & snapshot) const
{
    uint64_t rows = 0;
    bool has_value = false;
    ch_lance_error error{};
    if (!ch_lance_total_rows(dataset, snapshot.snapshot_id, &rows, &has_value, &error))
        throwLanceError(error);
    if (!has_value)
        return std::nullopt;
    return rows;
}

std::optional<size_t> Dataset::totalBytes() const
{
    uint64_t bytes = 0;
    bool has_value = false;
    ch_lance_error error{};
    if (!ch_lance_total_bytes(dataset, &bytes, &has_value, &error))
        throwLanceError(error);
    if (!has_value)
        return std::nullopt;
    return bytes;
}

Scan Dataset::planScan(const ScanDescription & scan_description) const
{
    std::vector<const char *> projection;
    projection.reserve(scan_description.projection.size());
    for (const auto & column : scan_description.projection)
        projection.push_back(column.c_str());

    ch_lance_string_list projection_list
    {
        .values = projection.empty() ? nullptr : projection.data(),
        .size = projection.size(),
    };
    ch_lance_scan_options options
    {
        .snapshot_id = scan_description.snapshot.snapshot_id,
        .projection = projection_list,
        .predicate = scan_description.predicate ? scan_description.predicate->c_str() : nullptr,
        .need_only_count = scan_description.need_only_count,
        .max_block_size = scan_description.max_block_size,
    };

    ch_lance_error error{};
    auto * scan = ch_lance_plan_scan(dataset, &options, &error);
    if (!scan)
        throwLanceError(error);
    return Scan(scan);
}

}

#endif
