#include "config.h"

#if USE_LANCE

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <arrow/array/array_nested.h>
#include <arrow/record_batch.h>

#include <limits>
#include <utility>
#include <vector>

namespace ProfileEvents
{
extern const Event LanceBatchesRead;
extern const Event LanceRowsRead;
extern const Event LanceReadBytes;
extern const Event LanceLocalReadBytes;
extern const Event LanceS3ReadBytes;
extern const Event LanceArrowConvertMicroseconds;
extern const Event LanceBatchSourcesActive;
extern const Event LanceArrowFieldMappingsBuilt;
extern const Event LanceProducerTasks;
extern const Event LanceScanSchemaExports;
extern const Event LanceQueuePushBatches;
extern const Event LanceQueuePopBatches;
extern const Event LanceQueuePushWaitMicroseconds;
extern const Event LanceQueuePopWaitMicroseconds;
extern const Event LanceQueuePeakBatches;
extern const Event LanceQueuePeakBytes;
extern const Event LanceProducerEOF;
extern const Event LanceProducerErrors;
extern const Event LanceProducerCancels;
extern const Event LanceGlobalLimitTruncatedRows;
extern const Event LanceScansCancelledByLimit;
}

namespace CurrentMetrics
{
extern const Metric LanceQueuedBatches;
extern const Metric LanceQueuedBytes;
extern const Metric LanceInFlightBatches;
extern const Metric LanceInFlightBytes;
}

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
extern const int QUERY_WAS_CANCELLED;
}
}

namespace DB::Lance
{

ReadCancellation::ReadCancellation(const ContextPtr & context)
    : cancel_handle(std::make_shared<CancelHandle>())
{
    if (!context)
        return;

    const auto query_status = context->getProcessListElementSafe();
    if (!query_status)
        return;

    query_cancel_callback
        = std::make_unique<StopCallback>(query_status->getCancellationToken(), [handle = cancel_handle] { handle->requestCancel(); });
}

void ReadCancellation::requestCancel() noexcept
{
    cancel_handle->requestCancel();
}

namespace
{

class ScanBatchProvider final : public BatchProvider
{
public:
    explicit ScanBatchProvider(Scan scan_)
        : scan(std::move(scan_))
    {
    }

    std::optional<Scan::Batch> nextBatch() override { return scan.nextBatch(); }
    void releaseBatch(UInt64 bytes) noexcept override { scan.releaseBatch(bytes); }
    void requestCancel() noexcept override { scan.requestCancel(); }
    const std::shared_ptr<arrow::Schema> & schema() const override { return scan.schema(); }
    Scan::Stats stats() const noexcept override { return scan.stats(); }

private:
    Scan scan;
};

}

ScanCoordinator::Batch::Batch(
    std::shared_ptr<ScanCoordinator> coordinator_, std::shared_ptr<arrow::RecordBatch> record_batch_, UInt64 rows_, UInt64 bytes_)
    : coordinator(std::move(coordinator_))
    , record_batch(std::move(record_batch_))
    , batch_rows(rows_)
    , batch_bytes(bytes_)
{
}

ScanCoordinator::Batch::Batch(Batch && other) noexcept
    : coordinator(std::move(other.coordinator))
    , record_batch(std::move(other.record_batch))
    , batch_rows(std::exchange(other.batch_rows, 0))
    , batch_bytes(std::exchange(other.batch_bytes, 0))
{
}

ScanCoordinator::Batch & ScanCoordinator::Batch::operator=(Batch && other) noexcept
{
    if (this != &other)
    {
        release();
        coordinator = std::move(other.coordinator);
        record_batch = std::move(other.record_batch);
        batch_rows = std::exchange(other.batch_rows, 0);
        batch_bytes = std::exchange(other.batch_bytes, 0);
    }
    return *this;
}

ScanCoordinator::Batch::~Batch()
{
    release();
}

void ScanCoordinator::Batch::release() noexcept
{
    record_batch.reset();
    if (coordinator)
    {
        coordinator->releaseBatch(batch_bytes);
        coordinator.reset();
    }
    batch_rows = 0;
    batch_bytes = 0;
}

std::shared_ptr<ScanCoordinator>
ScanCoordinator::create(DatasetHandle dataset, ScanDescription scan_description, ReadCancellationPtr cancellation)
{
    if (!cancellation)
        cancellation = std::make_shared<ReadCancellation>(nullptr);
    const bool use_s3 = dataset.options().use_s3;
    auto provider = std::make_unique<ScanBatchProvider>(dataset.planScan(scan_description, cancellation->handle()));
    return std::shared_ptr<ScanCoordinator>(
        new ScanCoordinator(std::move(provider), use_s3, scan_description.limit, std::move(cancellation)));
}

std::shared_ptr<ScanCoordinator> ScanCoordinator::createWithProvider(
    std::unique_ptr<BatchProvider> provider, bool use_s3, std::optional<UInt64> row_limit, ReadCancellationPtr cancellation)
{
    if (!provider)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance scan coordinator requires a batch provider");
    if (!cancellation)
        cancellation = std::make_shared<ReadCancellation>(nullptr);
    return std::shared_ptr<ScanCoordinator>(new ScanCoordinator(std::move(provider), use_s3, row_limit, std::move(cancellation)));
}

ScanCoordinator::ScanCoordinator(
    std::unique_ptr<BatchProvider> provider_, bool use_s3_, std::optional<UInt64> row_limit, ReadCancellationPtr cancellation_)
    : cancellation(std::move(cancellation_))
    , use_s3(use_s3_)
    , provider(std::move(provider_))
    , has_row_limit(row_limit.has_value())
    , remaining_rows(row_limit.value_or(std::numeric_limits<UInt64>::max()))
{
    refreshCurrentMetrics();
}

ScanCoordinator::~ScanCoordinator()
{
    cancel();
    accountStatsOnce();
    clearCurrentMetrics();
    std::lock_guard lock(state_mutex);
    current_state = State::Closed;
}

std::optional<ScanCoordinator::Batch> ScanCoordinator::nextBatch()
{
    {
        std::lock_guard lock(state_mutex);
        if (current_state != State::Running)
            return std::nullopt;
    }

    std::optional<Scan::Batch> next;
    try
    {
        next = provider->nextBatch();
    }
    catch (...)
    {
        refreshCurrentMetrics();
        bool propagate = false;
        {
            std::lock_guard lock(state_mutex);
            if (current_state == State::Running)
            {
                current_state = State::Failed;
                first_exception = std::current_exception();
                propagate = true;
            }
        }
        provider->requestCancel();
        if (propagate)
            std::rethrow_exception(first_exception);
        return std::nullopt;
    }
    refreshCurrentMetrics();

    if (!next)
    {
        /// The Rust producer records an error before publishing its single error message.
        /// A different consumer can observe channel close immediately after that message
        /// is claimed; it must not win the race and replace `Failed` with `Ended`.
        if (provider->stats().producer_error != 0)
            return std::nullopt;
        {
            std::lock_guard lock(state_mutex);
            if (current_state == State::Running)
                current_state = State::Ended;
        }
        accountStatsOnce();
        return std::nullopt;
    }

    const UInt64 claimed_rows = claimRows(next->rows);
    if (claimed_rows == 0)
    {
        provider->releaseBatch(next->bytes);
        finishAtLimit();
        return std::nullopt;
    }

    auto record_batch = std::move(next->record_batch);
    if (claimed_rows < next->rows)
    {
        ProfileEvents::increment(ProfileEvents::LanceGlobalLimitTruncatedRows, next->rows - claimed_rows);
        record_batch = record_batch->Slice(0, static_cast<int64_t>(claimed_rows));
    }

    if (has_row_limit && remaining_rows.load(std::memory_order_acquire) == 0)
        finishAtLimit();

    return Batch(shared_from_this(), std::move(record_batch), claimed_rows, next->bytes);
}

UInt64 ScanCoordinator::claimRows(UInt64 rows)
{
    if (!has_row_limit)
        return rows;

    UInt64 remaining = remaining_rows.load(std::memory_order_acquire);
    while (remaining != 0)
    {
        const UInt64 claimed = std::min(rows, remaining);
        if (remaining_rows.compare_exchange_weak(remaining, remaining - claimed, std::memory_order_acq_rel, std::memory_order_acquire))
            return claimed;
    }
    return 0;
}

void ScanCoordinator::finishAtLimit() noexcept
{
    {
        std::lock_guard lock(state_mutex);
        if (current_state != State::Running)
            return;
        current_state = State::Ended;
    }
    provider->requestCancel();
    ProfileEvents::increment(ProfileEvents::LanceScansCancelledByLimit);
    refreshCurrentMetrics();
    accountStatsOnce();
}

void ScanCoordinator::cancel() noexcept
{
    {
        std::lock_guard lock(state_mutex);
        if (current_state != State::Running)
            return;
        current_state = State::Cancelled;
    }
    cancellation->requestCancel();
    provider->requestCancel();
    refreshCurrentMetrics();
}

ScanCoordinator::State ScanCoordinator::state() const
{
    std::lock_guard lock(state_mutex);
    return current_state;
}

void ScanCoordinator::releaseBatch(UInt64 bytes) noexcept
{
    provider->releaseBatch(bytes);
    refreshCurrentMetrics();
}

void ScanCoordinator::accountStatsOnce() noexcept
{
    if (stats_accounted.exchange(true, std::memory_order_acq_rel))
        return;
    const auto stats = provider->stats();
    ProfileEvents::increment(ProfileEvents::LanceProducerTasks, stats.producer_tasks);
    ProfileEvents::increment(ProfileEvents::LanceScanSchemaExports, stats.schema_exports);
    ProfileEvents::increment(ProfileEvents::LanceQueuePushBatches, stats.queue_push_batches);
    ProfileEvents::increment(ProfileEvents::LanceQueuePopBatches, stats.queue_pop_batches);
    ProfileEvents::increment(ProfileEvents::LanceQueuePushWaitMicroseconds, stats.queue_push_wait_microseconds);
    ProfileEvents::increment(ProfileEvents::LanceQueuePopWaitMicroseconds, stats.consumer_pop_wait_microseconds);
    ProfileEvents::increment(ProfileEvents::LanceQueuePeakBatches, stats.queue_peak_batches);
    ProfileEvents::increment(ProfileEvents::LanceQueuePeakBytes, stats.queue_peak_bytes);
    ProfileEvents::increment(ProfileEvents::LanceProducerEOF, stats.producer_eof);
    ProfileEvents::increment(ProfileEvents::LanceProducerErrors, stats.producer_error);
    ProfileEvents::increment(ProfileEvents::LanceProducerCancels, stats.producer_cancel);
}

void ScanCoordinator::refreshCurrentMetrics() noexcept
{
    const auto stats = provider->stats();
    std::lock_guard lock(current_metrics_mutex);
    const auto update = [](CurrentMetrics::Metric metric, UInt64 current, UInt64 & reported)
    {
        CurrentMetrics::add(metric, static_cast<CurrentMetrics::Value>(current) - static_cast<CurrentMetrics::Value>(reported));
        reported = current;
    };
    update(CurrentMetrics::LanceQueuedBatches, stats.queued_batches, reported_current_metrics.queued_batches);
    update(CurrentMetrics::LanceQueuedBytes, stats.queued_bytes, reported_current_metrics.queued_bytes);
    update(CurrentMetrics::LanceInFlightBatches, stats.in_flight_batches, reported_current_metrics.in_flight_batches);
    update(CurrentMetrics::LanceInFlightBytes, stats.in_flight_bytes, reported_current_metrics.in_flight_bytes);
}

void ScanCoordinator::clearCurrentMetrics() noexcept
{
    std::lock_guard lock(current_metrics_mutex);
    CurrentMetrics::sub(CurrentMetrics::LanceQueuedBatches, reported_current_metrics.queued_batches);
    CurrentMetrics::sub(CurrentMetrics::LanceQueuedBytes, reported_current_metrics.queued_bytes);
    CurrentMetrics::sub(CurrentMetrics::LanceInFlightBatches, reported_current_metrics.in_flight_batches);
    CurrentMetrics::sub(CurrentMetrics::LanceInFlightBytes, reported_current_metrics.in_flight_bytes);
    reported_current_metrics.queued_batches = 0;
    reported_current_metrics.queued_bytes = 0;
    reported_current_metrics.in_flight_batches = 0;
    reported_current_metrics.in_flight_bytes = 0;
}

namespace
{

[[noreturn]] void throwUnsupportedNull(const String & path, const IDataType & type)
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Lance column '{}' contains NULL values that cannot be represented by ClickHouse type '{}'",
        path,
        type.getName());
}

void validateArrayNullability(
    const std::shared_ptr<arrow::Array> & array,
    const DataTypePtr & type,
    const String & path,
    const std::vector<UInt8> * active_rows = nullptr);

bool hasActiveNulls(const arrow::Array & array, const std::vector<UInt8> * active_rows)
{
    if (array.null_count() == 0)
        return false;
    if (!active_rows)
        return true;

    for (int64_t index = 0; index < array.length(); ++index)
    {
        if ((*active_rows)[index] && array.IsNull(index))
            return true;
    }
    return false;
}

template <typename ArrowListArray>
void validateListNullability(
    const std::shared_ptr<arrow::Array> & array, const DataTypePtr & type, const String & path, const std::vector<UInt8> * active_rows)
{
    if (hasActiveNulls(*array, active_rows))
        throwUnsupportedNull(path, *type);

    const auto * array_type = dynamic_cast<const DataTypeArray *>(removeNullable(type).get());
    if (!array_type)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Lance column '{}' has Arrow list type but ClickHouse type '{}'", path, type->getName());
    }

    const auto & list_array = static_cast<const ArrowListArray &>(*array);
    auto flattened = list_array.Flatten();
    if (!flattened.ok())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to flatten Lance list column '{}': {}", path, flattened.status().ToString());
    }

    validateArrayNullability(flattened.ValueOrDie(), array_type->getNestedType(), path + "[]");
}

void validateStructNullability(
    const std::shared_ptr<arrow::Array> & array, const DataTypePtr & type, const String & path, const std::vector<UInt8> * active_rows)
{
    if (hasActiveNulls(*array, active_rows) && !type->isNullable())
        throwUnsupportedNull(path, *type);

    const auto * tuple_type = dynamic_cast<const DataTypeTuple *>(removeNullable(type).get());
    if (!tuple_type)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Lance column '{}' has Arrow struct type but ClickHouse type '{}'", path, type->getName());
    }

    const auto & struct_array = static_cast<const arrow::StructArray &>(*array);
    const auto & struct_type = static_cast<const arrow::StructType &>(*array->type());
    std::vector<UInt8> child_active_rows(static_cast<size_t>(array->length()));
    for (int64_t index = 0; index < array->length(); ++index)
    {
        child_active_rows[index] = (!active_rows || (*active_rows)[index]) && !array->IsNull(index);
    }

    for (int index = 0; index < struct_type.num_fields(); ++index)
    {
        const auto & field = struct_type.field(index);
        const auto position = tuple_type->tryGetPositionByName(field->name());
        if (!position)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Lance struct field '{}.{}' is missing from ClickHouse type '{}'",
                path,
                field->name(),
                type->getName());
        }

        validateArrayNullability(
            struct_array.field(index), tuple_type->getElement(*position), path + "." + field->name(), &child_active_rows);
    }
}

void validateMapNullability(
    const std::shared_ptr<arrow::Array> & array, const DataTypePtr & type, const String & path, const std::vector<UInt8> * active_rows)
{
    if (hasActiveNulls(*array, active_rows))
        throwUnsupportedNull(path, *type);

    const auto * map_type = dynamic_cast<const DataTypeMap *>(removeNullable(type).get());
    if (!map_type)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Lance column '{}' has Arrow map type but ClickHouse type '{}'", path, type->getName());
    }

    const auto & map_array = static_cast<const arrow::MapArray &>(*array);
    auto flattened = map_array.Flatten();
    if (!flattened.ok())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to flatten Lance map column '{}': {}", path, flattened.status().ToString());
    }

    const auto & entries = static_cast<const arrow::StructArray &>(*flattened.ValueOrDie());
    if (entries.field(0)->null_count() != 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Lance map column '{}' contains a NULL key", path);
    }

    validateArrayNullability(entries.field(0), map_type->getKeyType(), path + ".key");
    validateArrayNullability(entries.field(1), map_type->getValueType(), path + ".value");
}

void validateArrayNullability(
    const std::shared_ptr<arrow::Array> & array, const DataTypePtr & type, const String & path, const std::vector<UInt8> * active_rows)
{
    switch (array->type_id())
    {
        case arrow::Type::LIST: validateListNullability<arrow::ListArray>(array, type, path, active_rows); break;
        case arrow::Type::LARGE_LIST: validateListNullability<arrow::LargeListArray>(array, type, path, active_rows); break;
        case arrow::Type::FIXED_SIZE_LIST: validateListNullability<arrow::FixedSizeListArray>(array, type, path, active_rows); break;
        case arrow::Type::STRUCT: validateStructNullability(array, type, path, active_rows); break;
        case arrow::Type::MAP: validateMapNullability(array, type, path, active_rows); break;
        default: break;
    }
}

void validateRecordBatchNullability(const arrow::RecordBatch & batch, const Block & header)
{
    for (int index = 0; index < batch.num_columns(); ++index)
    {
        const auto & field = batch.schema()->field(index);
        if (!header.has(field->name()))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Lance column '{}' is missing from the ClickHouse header", field->name());
        }

        validateArrayNullability(batch.column(index), header.getByName(field->name()).type, field->name());
    }
}

/// Arrow buffer footprint of the batch (not S3 wire size).
void accountLanceBatchMetrics(size_t batch_rows, UInt64 batch_bytes, bool use_s3)
{
    ProfileEvents::increment(ProfileEvents::LanceBatchesRead);
    ProfileEvents::increment(ProfileEvents::LanceRowsRead, batch_rows);
    ProfileEvents::increment(ProfileEvents::LanceReadBytes, batch_bytes);
    if (use_s3)
        ProfileEvents::increment(ProfileEvents::LanceS3ReadBytes, batch_bytes);
    else
        ProfileEvents::increment(ProfileEvents::LanceLocalReadBytes, batch_bytes);
}

void addVirtualColumns(
    Chunk & chunk,
    const NamesAndTypesList & requested_virtual_columns,
    const ReadVirtualValues & values,
    ContextPtr context,
    const std::optional<FormatSettings> & format_settings)
{
    if (requested_virtual_columns.empty())
        return;

    VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
        chunk,
        requested_virtual_columns,
        {
            .path = values.path,
            .storage_id = values.storage_id,
            .size = std::nullopt,
            .filename = nullptr,
            .last_modified = std::nullopt,
            .etag = nullptr,
            .tags = nullptr,
            .data_lake_snapshot_version = values.snapshot_version,
            .iceberg_metadata_file_path = nullptr,
        },
        context,
        format_settings);
}

Block makePhysicalHeader(const Block & output_header, const NamesAndTypesList & requested_virtual_columns)
{
    ColumnsWithTypeAndName physical_columns;
    physical_columns.reserve(output_header.columns());
    for (const auto & column : output_header)
    {
        if (!requested_virtual_columns.contains(column.name))
            physical_columns.emplace_back(column.type->createColumn(), column.type, column.name);
    }
    return Block(std::move(physical_columns));
}

class DatasetCountProvider final : public CountSource::Provider
{
public:
    DatasetCountProvider(DatasetHandle dataset_, ScanDescription scan_, ReadCancellationPtr cancellation_)
        : dataset(std::move(dataset_))
        , scan(std::move(scan_))
        , cancellation(std::move(cancellation_))
    {
    }

    std::optional<size_t> countRows() override
    {
        if (scan.predicate || !scan.fragment_ids.empty())
            return dataset.countRows(scan.snapshot, scan.predicate, scan.fragment_ids, cancellation->handle());
        return dataset.totalRows(scan.snapshot, cancellation->handle());
    }

    void requestCancel() noexcept override
    {
        cancellation->requestCancel();
    }

private:
    DatasetHandle dataset;
    ScanDescription scan;
    ReadCancellationPtr cancellation;
};
}

BatchSource::BatchSource(
    const Block & output_header,
    Block physical_header_,
    std::shared_ptr<ScanCoordinator> coordinator_,
    NamesAndTypesList requested_virtual_columns_,
    ReadVirtualValues virtual_values_,
    ContextPtr context_,
    FormatSettings format_settings_)
    : ISource(std::make_shared<const Block>(output_header), /*enable_auto_progress=*/true)
    , physical_header(std::move(physical_header_))
    , coordinator(std::move(coordinator_))
    , requested_virtual_columns(std::move(requested_virtual_columns_))
    , virtual_values(std::move(virtual_values_))
    , context(std::move(context_))
    , format_settings(std::move(format_settings_))
{
    if (!coordinator)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance BatchSource requires a scan coordinator");
}

void BatchSource::onCancel() noexcept
{
    coordinator->cancel();
}

Chunk BatchSource::generate()
{
    if (is_finished || isCancelled())
        return {};

    std::optional<ScanCoordinator::Batch> batch;
    try
    {
        batch = coordinator->nextBatch();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED || isCancelled())
        {
            is_finished = true;
            return {};
        }
        throw;
    }

    if (!batch)
    {
        is_finished = true;
        return {};
    }

    if (isCancelled())
        return {};

    const auto & record_batch = batch->recordBatch();
    const size_t batch_rows = static_cast<size_t>(batch->rows());
    if (!reported_active)
    {
        ProfileEvents::increment(ProfileEvents::LanceBatchSourcesActive);
        reported_active = true;
    }
    accountLanceBatchMetrics(batch_rows, batch->bytes(), coordinator->usesS3());

    try
    {
        Stopwatch convert_watch;
        if (!converter)
        {
            converter = std::make_unique<ArrowColumnToCHColumn>(
                physical_header,
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

        const bool build_field_mapping = !converter->hasRecordBatchFieldMapping();
        auto chunk = converter->arrowRecordBatchToCHChunk(
            *record_batch, /* metadata */ nullptr, /* block_missing_values */ nullptr);
        if (build_field_mapping)
            ProfileEvents::increment(ProfileEvents::LanceArrowFieldMappingsBuilt);
        ProfileEvents::increment(ProfileEvents::LanceArrowConvertMicroseconds, convert_watch.elapsedMicroseconds());
        validateRecordBatchNullability(*record_batch, physical_header);
        addVirtualColumns(chunk, requested_virtual_columns, virtual_values, context, format_settings);
        return chunk;
    }
    catch (...)
    {
        coordinator->cancel();
        throw;
    }
}

CountSource::CountSource(
    const Block & output_header_,
    DatasetHandle dataset_,
    ScanDescription scan_,
    ReadCancellationPtr cancellation_,
    NamesAndTypesList requested_virtual_columns_,
    ReadVirtualValues virtual_values_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_)
    : ISource(std::make_shared<const Block>(output_header_), /*enable_auto_progress=*/true)
    , provider(std::make_unique<DatasetCountProvider>(std::move(dataset_), scan_, std::move(cancellation_)))
    , max_block_size(scan_.max_block_size)
    , physical_header(makePhysicalHeader(output_header_, requested_virtual_columns_))
    , requested_virtual_columns(std::move(requested_virtual_columns_))
    , virtual_values(std::move(virtual_values_))
    , context(std::move(context_))
    , format_settings(std::move(format_settings_))
{
    if (max_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`Lance` row-count Source requires max_block_size greater than zero");
}

CountSource::CountSource(
    const Block & output_header_,
    std::unique_ptr<Provider> provider_,
    size_t max_block_size_,
    NamesAndTypesList requested_virtual_columns_,
    ReadVirtualValues virtual_values_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_)
    : ISource(std::make_shared<const Block>(output_header_), /*enable_auto_progress=*/true)
    , provider(std::move(provider_))
    , max_block_size(max_block_size_)
    , physical_header(makePhysicalHeader(output_header_, requested_virtual_columns_))
    , requested_virtual_columns(std::move(requested_virtual_columns_))
    , virtual_values(std::move(virtual_values_))
    , context(std::move(context_))
    , format_settings(std::move(format_settings_))
{
    if (!provider)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "`Lance` row-count Source requires a count provider");
    if (max_block_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`Lance` row-count Source requires max_block_size greater than zero");
}

void CountSource::onCancel() noexcept
{
    provider->requestCancel();
}

Chunk CountSource::generate()
{
    if (is_finished || isCancelled())
        return {};

    try
    {
        if (!rows_remaining)
        {
            const auto rows = provider->countRows();
            if (!rows)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "`Lance` row-count Source did not receive a row count");

            ProfileEvents::increment(ProfileEvents::LanceRowsRead, *rows);
            addTotalRowsApprox(*rows);
            rows_remaining = rows;
        }

        if (*rows_remaining == 0)
        {
            is_finished = true;
            return {};
        }

        const size_t chunk_rows = std::min(*rows_remaining, max_block_size);
        *rows_remaining -= chunk_rows;
        if (*rows_remaining == 0)
            is_finished = true;

        auto chunk = cloneConstWithDefault(Chunk{physical_header.getColumns(), 0}, chunk_rows);
        addVirtualColumns(chunk, requested_virtual_columns, virtual_values, context, format_settings);
        return chunk;
    }
    catch (const Exception &)
    {
        if (isCancelled())
            return {};
        provider->requestCancel();
        throw;
    }
    catch (...)
    {
        provider->requestCancel();
        throw;
    }
}
}

#endif
