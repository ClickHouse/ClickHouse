#include "config.h"

#if USE_LANCE

#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceScanDescription.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/Stopwatch.h>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <ch_lance.h>

#include <fmt/format.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace ProfileEvents
{
extern const Event LanceDatasetOpen;
extern const Event LanceDatasetOpenMicroseconds;
extern const Event LancePlanScan;
extern const Event LancePlanScanMicroseconds;
extern const Event LanceNextBatch;
extern const Event LanceNextBatchMicroseconds;
extern const Event LanceRuntimeInit;
extern const Event LanceCountRows;
extern const Event LanceCountRowsMicroseconds;
extern const Event LanceSnapshotIdentityMismatch;
}

namespace DB
{
namespace ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int AUTHENTICATION_FAILED;
extern const int BAD_ARGUMENTS;
extern const int CANNOT_OPEN_FILE;
extern const int FILE_DOESNT_EXIST;
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
extern const int QUERY_WAS_CANCELLED;
extern const int S3_ERROR;
extern const int UNKNOWN_EXCEPTION;
}
}

namespace DB::Lance
{

namespace ErrorMapping
{

int toClickHouseErrorCode(UInt32 kind, UInt32 origin)
{
    switch (kind)
    {
        case CH_LANCE_ERROR_NONE:
            return ErrorCodes::LOGICAL_ERROR;
        case CH_LANCE_ERROR_INVALID_ARGUMENT:
            return ErrorCodes::BAD_ARGUMENTS;
        case CH_LANCE_ERROR_NOT_FOUND:
            return ErrorCodes::FILE_DOESNT_EXIST;
        case CH_LANCE_ERROR_PERMISSION_DENIED:
            return ErrorCodes::ACCESS_DENIED;
        case CH_LANCE_ERROR_UNAUTHENTICATED:
            return ErrorCodes::AUTHENTICATION_FAILED;
        case CH_LANCE_ERROR_CORRUPT_DATA:
            return ErrorCodes::INCORRECT_DATA;
        case CH_LANCE_ERROR_UNSUPPORTED:
            return ErrorCodes::BAD_ARGUMENTS;
        case CH_LANCE_ERROR_VERSION_NOT_FOUND:
            return ErrorCodes::FILE_DOESNT_EXIST;
        case CH_LANCE_ERROR_STORAGE:
            if (origin == CH_LANCE_ERROR_ORIGIN_S3)
                return ErrorCodes::S3_ERROR;
            if (origin == CH_LANCE_ERROR_ORIGIN_LOCAL)
                return ErrorCodes::CANNOT_OPEN_FILE;
            return ErrorCodes::UNKNOWN_EXCEPTION;
        case CH_LANCE_ERROR_INTERNAL:
            return ErrorCodes::UNKNOWN_EXCEPTION;
        case CH_LANCE_ERROR_CANCELLED:
            return ErrorCodes::QUERY_WAS_CANCELLED;
        case CH_LANCE_ERROR_SNAPSHOT_MISMATCH:
            return ErrorCodes::INCORRECT_DATA;
    }
    return ErrorCodes::UNKNOWN_EXCEPTION;
}

}

namespace
{

struct LanceError
{
    UInt32 kind;
    UInt32 origin;
    String message;
};

LanceError takeError(ch_lance_error & error)
{
    LanceError result
    {
        .kind = error.kind,
        .origin = error.origin,
        .message = error.message ? error.message : "Lance FFI error has no message",
    };
    ch_lance_free_error(&error);
    return result;
}

[[noreturn]] void throwLanceError(ch_lance_error & error, const String & operation = {})
{
    const auto lance_error = takeError(error);
    const auto message = operation.empty() ? lance_error.message : fmt::format("{}: {}", operation, lance_error.message);
    if (lance_error.kind == CH_LANCE_ERROR_NONE)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance FFI call failed without an error kind: {}", message);

    const auto code = ErrorMapping::toClickHouseErrorCode(lance_error.kind, lance_error.origin);
    if (lance_error.kind == CH_LANCE_ERROR_SNAPSHOT_MISMATCH)
        ProfileEvents::increment(ProfileEvents::LanceSnapshotIdentityMismatch);
    if (lance_error.kind > CH_LANCE_ERROR_SNAPSHOT_MISMATCH)
        throw Exception(code, "Unknown Lance FFI error kind {}: {}", lance_error.kind, message);
    throw Exception(code, "{}", message);
}

ch_lance_dataset_options toNativeOptions(const DatasetOptions & options, ch_lance_cancel_handle * cancel = nullptr)
{
    return ch_lance_dataset_options
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
        .s3_request_timeout_ms = options.s3_request_timeout_ms,
        .s3_connect_timeout_ms = options.s3_connect_timeout_ms,
        .cancel = cancel,
    };
}

ch_lance_snapshot_info toNativeSnapshot(const TableStateSnapshot & snapshot)
{
    snapshot.validate(ErrorCodes::LOGICAL_ERROR);
    ch_lance_snapshot_info native{
        .version = snapshot.version,
        .manifest_id = {},
        .manifest_size = snapshot.manifest_size,
        .manifest_sha256 = {},
        .has_etag = snapshot.has_etag,
        .etag_sha256 = {},
    };
    std::copy(snapshot.manifest_id.begin(), snapshot.manifest_id.end(), native.manifest_id);
    std::copy(snapshot.manifest_sha256.begin(), snapshot.manifest_sha256.end(), native.manifest_sha256);
    std::copy(snapshot.etag_sha256.begin(), snapshot.etag_sha256.end(), native.etag_sha256);
    return native;
}

}

String DatasetOptions::identityKey() const
{
    SipHash hash;
    hash.update(uri);
    hash.update(static_cast<UInt8>(use_s3));
    hash.update(s3_region);
    hash.update(s3_endpoint);
    hash.update(s3_access_key_id);
    hash.update(s3_secret_access_key);
    hash.update(s3_session_token);
    hash.update(s3_role_arn);
    hash.update(s3_role_session_name);
    hash.update(static_cast<UInt8>(s3_use_environment_credentials));
    hash.update(static_cast<UInt8>(s3_no_sign_request));
    hash.update(static_cast<UInt8>(s3_allow_http));
    hash.update(static_cast<UInt8>(s3_virtual_hosted_style_request));
    return getSipHash128AsHexString(hash);
}

void ensureRuntime(UInt32 worker_threads)
{
    ch_lance_runtime_config config{.worker_threads = worker_threads};
    ch_lance_error error{};
    if (!ch_lance_runtime_ensure(&config, &error))
        throwLanceError(error, "Cannot initialize Lance runtime");
}

RuntimeStats runtimeStats()
{
    ch_lance_runtime_stats stats{};
    ch_lance_get_runtime_stats(&stats);
    return RuntimeStats
    {
        .open_dataset_calls = stats.open_dataset_calls,
        .plan_scan_calls = stats.plan_scan_calls,
        .next_batch_calls = stats.next_batch_calls,
        .runtime_initialized = stats.runtime_initialized,
    };
}

DatasetHandle::Impl::Impl(ch_lance_dataset * dataset_, DatasetOptions options_)
    : dataset(dataset_)
    , options(std::move(options_))
{
}

DatasetHandle::Impl::~Impl()
{
    if (dataset)
        ch_lance_free_dataset(dataset);
}

CancelHandle::CancelHandle()
    : handle(ch_lance_cancel_handle_create())
{
    if (!handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create Lance cancel handle");
}

CancelHandle::~CancelHandle()
{
    if (handle)
        ch_lance_cancel_handle_free(handle);
}

CancelHandle::CancelHandle(CancelHandle && other) noexcept
    : handle(std::exchange(other.handle, nullptr))
{
}

CancelHandle & CancelHandle::operator=(CancelHandle && other) noexcept
{
    if (this != &other)
    {
        if (handle)
            ch_lance_cancel_handle_free(handle);
        handle = std::exchange(other.handle, nullptr);
    }
    return *this;
}

void CancelHandle::requestCancel() noexcept
{
    if (handle)
        ch_lance_cancel_handle_cancel(handle);
}

DatasetHandle DatasetHandle::open(const DatasetOptions & options, const CancelHandlePtr & cancel)
{
    return openEphemeral(options, cancel);
}

DatasetHandle DatasetHandle::openEphemeral(const DatasetOptions & options, const CancelHandlePtr & cancel)
{
    ensureRuntime();

    const auto before_init = runtimeStats().runtime_initialized;

    Stopwatch open_watch;
    ch_lance_error error{};
    auto native_options = toNativeOptions(options, cancel ? cancel->raw() : nullptr);
    auto * dataset = ch_lance_open_dataset(&native_options, &error);
    if (!dataset)
        throwLanceError(error);

    ProfileEvents::increment(ProfileEvents::LanceDatasetOpen);
    ProfileEvents::increment(ProfileEvents::LanceDatasetOpenMicroseconds, open_watch.elapsedMicroseconds());

    const auto after_init = runtimeStats().runtime_initialized;
    if (after_init > before_init)
        ProfileEvents::increment(ProfileEvents::LanceRuntimeInit, after_init - before_init);

    return DatasetHandle(std::make_shared<Impl>(dataset, options));
}

const DatasetOptions & DatasetHandle::options() const
{
    if (!impl)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance DatasetHandle is empty");
    return impl->options;
}

String DatasetHandle::identityKey() const
{
    return options().identityKey();
}

ch_lance_dataset * DatasetHandle::raw() const
{
    if (!impl)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance DatasetHandle is empty");
    return impl->dataset;
}

TableStateSnapshot DatasetHandle::currentSnapshot() const
{
    ch_lance_snapshot_info snapshot{};
    ch_lance_error error{};
    if (!ch_lance_current_snapshot(raw(), &snapshot, &error))
        throwLanceError(error);
    TableStateSnapshot result{
        .version = snapshot.version,
        .manifest_id = {},
        .manifest_size = snapshot.manifest_size,
        .manifest_sha256 = {},
        .has_etag = snapshot.has_etag,
        .etag_sha256 = {},
    };
    std::copy(std::begin(snapshot.manifest_id), std::end(snapshot.manifest_id), result.manifest_id.begin());
    std::copy(std::begin(snapshot.manifest_sha256), std::end(snapshot.manifest_sha256), result.manifest_sha256.begin());
    std::copy(std::begin(snapshot.etag_sha256), std::end(snapshot.etag_sha256), result.etag_sha256.begin());
    result.validate(ErrorCodes::INCORRECT_DATA);
    return result;
}

NamesAndTypesList DatasetHandle::tableSchema(
    const TableStateSnapshot & snapshot,
    ContextPtr context,
    const CancelHandlePtr & cancel,
    std::unordered_set<String> * utf8_columns) const
{
    ArrowSchema schema{};
    ch_lance_error error{};
    const auto native_snapshot = toNativeSnapshot(snapshot);
    if (!ch_lance_export_schema(raw(), &native_snapshot, &schema, cancel ? cancel->raw() : nullptr, &error))
        throwLanceError(error, fmt::format("Cannot export schema for `Lance` dataset version {}", snapshot.version));

    auto arrow_schema = arrow::ImportSchema(&schema);
    if (!arrow_schema.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to import Lance Arrow schema: {}", arrow_schema.status().ToString());

    if (utf8_columns)
    {
        utf8_columns->clear();
        for (const auto & field : (*arrow_schema)->fields())
        {
            if (field->type()->id() == arrow::Type::STRING || field->type()->id() == arrow::Type::LARGE_STRING)
                utf8_columns->insert(field->name());
        }
    }

    const auto format_settings = getFormatSettings(context);
    const auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        **arrow_schema,
        nullptr,
        "Lance",
        format_settings,
        /* skip_columns_with_unsupported_types */ false,
        /* allow_inferring_nullable_columns */ true,
        /* case_insensitive_matching */ false,
        /* allow_geoparquet_parser */ false,
        /* enable_json_parsing */ false);
    return header.getNamesAndTypesList();
}

std::optional<size_t> DatasetHandle::totalRows(const TableStateSnapshot & snapshot, const CancelHandlePtr & cancel) const
{
    uint64_t rows = 0;
    bool has_value = false;
    Stopwatch watch;
    ch_lance_error error{};
    const auto native_snapshot = toNativeSnapshot(snapshot);
    if (!ch_lance_total_rows(raw(), &native_snapshot, &rows, &has_value, cancel ? cancel->raw() : nullptr, &error))
        throwLanceError(error, fmt::format("Cannot count rows in `Lance` dataset version {}", snapshot.version));
    ProfileEvents::increment(ProfileEvents::LanceCountRows);
    ProfileEvents::increment(ProfileEvents::LanceCountRowsMicroseconds, watch.elapsedMicroseconds());
    if (!has_value)
        return std::nullopt;
    return rows;
}

std::optional<size_t> DatasetHandle::countRows(
    const TableStateSnapshot & snapshot,
    const std::optional<String> & predicate,
    const std::vector<UInt64> & fragment_ids,
    const CancelHandlePtr & cancel) const
{
    uint64_t rows = 0;
    bool has_value = false;
    Stopwatch watch;
    ch_lance_error error{};
    const auto native_snapshot = toNativeSnapshot(snapshot);
    if (!ch_lance_count_rows(
            raw(),
            &native_snapshot,
            predicate ? predicate->c_str() : nullptr,
            fragment_ids.empty() ? nullptr : fragment_ids.data(),
            fragment_ids.size(),
            &rows,
            &has_value,
            cancel ? cancel->raw() : nullptr,
            &error))
        throwLanceError(error, fmt::format("Cannot count rows in `Lance` dataset version {}", snapshot.version));
    ProfileEvents::increment(ProfileEvents::LanceCountRows);
    ProfileEvents::increment(ProfileEvents::LanceCountRowsMicroseconds, watch.elapsedMicroseconds());
    if (!has_value)
        return std::nullopt;
    return rows;
}

std::optional<size_t> DatasetHandle::totalBytes() const
{
    uint64_t bytes = 0;
    bool has_value = false;
    ch_lance_error error{};
    if (!ch_lance_total_bytes(raw(), &bytes, &has_value, &error))
        throwLanceError(error);
    if (!has_value)
        return std::nullopt;
    return bytes;
}

std::vector<FragmentInfo> DatasetHandle::listFragments(const TableStateSnapshot & snapshot, const CancelHandlePtr & cancel) const
{
    ch_lance_fragment_info * list = nullptr;
    size_t size = 0;
    ch_lance_error error{};
    const auto native_snapshot = toNativeSnapshot(snapshot);
    if (!ch_lance_list_fragments(raw(), &native_snapshot, &list, &size, cancel ? cancel->raw() : nullptr, &error))
        throwLanceError(error, fmt::format("Cannot list fragments for `Lance` dataset version {}", snapshot.version));

    std::vector<FragmentInfo> result;
    result.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        FragmentInfo info;
        info.id = list[i].id;
        if (list[i].num_rows != std::numeric_limits<UInt64>::max())
            info.num_rows = list[i].num_rows;
        info.size_bytes = list[i].size_bytes;
        result.push_back(std::move(info));
    }
    ch_lance_free_fragment_list(list, size);
    return result;
}

Scan DatasetHandle::planScan(const ScanDescription & scan_description, const CancelHandlePtr & cancel) const
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
    /// Keep fragment id storage alive for the duration of ch_lance_plan_scan.
    const auto & fragment_ids = scan_description.fragment_ids;
    const auto native_snapshot = toNativeSnapshot(scan_description.snapshot);
    ch_lance_scan_options options{
        .snapshot = native_snapshot,
        .projection = projection_list,
        .predicate = scan_description.predicate ? scan_description.predicate->c_str() : nullptr,
        .need_only_count = scan_description.need_only_count,
        .max_block_size = scan_description.max_block_size,
        .limit = scan_description.limit.value_or(0),
        .cancel = cancel ? cancel->raw() : nullptr,
        /// FFI uses scan_unordered so zero-init stays ordered (compatible default).
        .scan_unordered = !scan_description.scan_in_order,
        .fragment_readahead = scan_description.fragment_readahead,
        .batch_readahead = scan_description.batch_readahead,
        .io_buffer_size = scan_description.io_buffer_size,
        .fragment_ids = fragment_ids.empty() ? nullptr : fragment_ids.data(),
        .fragment_ids_size = fragment_ids.size(),
    };

    Stopwatch plan_watch;
    ch_lance_error error{};
    auto * scan = ch_lance_plan_scan(raw(), &options, &error);
    if (!scan)
        throwLanceError(error, fmt::format("Cannot plan scan for `Lance` dataset version {}", scan_description.snapshot.version));

    ProfileEvents::increment(ProfileEvents::LancePlanScan);
    ProfileEvents::increment(ProfileEvents::LancePlanScanMicroseconds, plan_watch.elapsedMicroseconds());
    return Scan(*this, scan);
}

Scan::Scan(DatasetHandle dataset_, ch_lance_scan * scan_)
    : dataset(std::move(dataset_))
    , scan(scan_)
{
}

Scan::Scan(Scan && other) noexcept
    : dataset(std::move(other.dataset))
    , scan(std::exchange(other.scan, nullptr))
{
}

Scan & Scan::operator=(Scan && other) noexcept
{
    if (this != &other)
    {
        if (scan)
            ch_lance_free_scan(scan);
        dataset = std::move(other.dataset);
        scan = std::exchange(other.scan, nullptr);
    }
    return *this;
}

Scan::~Scan()
{
    if (scan)
        ch_lance_free_scan(scan);
}

void Scan::requestCancel() noexcept
{
    if (scan)
        ch_lance_cancel_scan(scan);
}

std::shared_ptr<arrow::RecordBatch> Scan::nextBatch() const
{
    ArrowArray array{};
    ArrowSchema schema{};
    bool has_batch = false;
    Stopwatch watch;
    ch_lance_error error{};
    if (!ch_lance_next_batch(scan, &array, &schema, &has_batch, &error))
        throwLanceError(error);

    ProfileEvents::increment(ProfileEvents::LanceNextBatch);
    ProfileEvents::increment(ProfileEvents::LanceNextBatchMicroseconds, watch.elapsedMicroseconds());

    if (!has_batch)
        return nullptr;

    auto record_batch = arrow::ImportRecordBatch(&array, &schema);
    if (!record_batch.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to import Lance Arrow record batch: {}", record_batch.status().ToString());

    return *record_batch;
}

}

#endif
