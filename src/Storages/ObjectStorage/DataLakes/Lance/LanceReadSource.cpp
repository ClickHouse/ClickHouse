#include "config.h"

#if USE_LANCE

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <arrow/array/array_nested.h>
#include <arrow/table.h>

#include <vector>

namespace ProfileEvents
{
extern const Event LanceBatchesRead;
extern const Event LanceRowsRead;
extern const Event LanceReadBytes;
extern const Event LanceLocalReadBytes;
extern const Event LanceS3ReadBytes;
extern const Event LanceArrowConvertMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
extern const int QUERY_WAS_CANCELLED;
extern const int UNKNOWN_EXCEPTION;
}
}

namespace DB::Lance
{
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
size_t approximateArrayDataBytes(const arrow::ArrayData & data)
{
    size_t bytes = 0;
    for (const auto & buffer : data.buffers)
    {
        if (buffer)
            bytes += static_cast<size_t>(buffer->size());
    }
    for (const auto & child : data.child_data)
    {
        if (child)
            bytes += approximateArrayDataBytes(*child);
    }
    if (data.dictionary)
        bytes += approximateArrayDataBytes(*data.dictionary);
    return bytes;
}

size_t approximateRecordBatchBytes(const arrow::RecordBatch & batch)
{
    size_t bytes = 0;
    for (int index = 0; index < batch.num_columns(); ++index)
    {
        const auto & column = batch.column(index);
        if (column && column->data())
            bytes += approximateArrayDataBytes(*column->data());
    }
    return bytes;
}

void accountLanceBatchMetrics(const arrow::RecordBatch & batch, size_t batch_rows, bool use_s3)
{
    const size_t batch_bytes = approximateRecordBatchBytes(batch);
    ProfileEvents::increment(ProfileEvents::LanceBatchesRead);
    ProfileEvents::increment(ProfileEvents::LanceRowsRead, batch_rows);
    ProfileEvents::increment(ProfileEvents::LanceReadBytes, batch_bytes);
    if (use_s3)
        ProfileEvents::increment(ProfileEvents::LanceS3ReadBytes, batch_bytes);
    else
        ProfileEvents::increment(ProfileEvents::LanceLocalReadBytes, batch_bytes);
}

}

ReadSource::ReadSource(
    const Block & header,
    ObjectInfoPtr object_info_,
    DatasetHandle dataset_,
    ScanDescription scan_,
    CancelHandlePtr cancel_handle_,
    FormatSettings format_settings_)
    : ISource(std::make_shared<const Block>(header), /*enable_auto_progress=*/true)
    , object_info(std::move(object_info_))
    , dataset(std::move(dataset_))
    , scan(std::move(scan_))
    , format_settings(std::move(format_settings_))
    , cancel_handle(std::move(cancel_handle_))
{
    if (!dataset)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance ReadSource requires a non-empty DatasetHandle");
    if (!cancel_handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lance ReadSource requires a non-empty CancelHandle");
}

void ReadSource::onCancel() noexcept
{
    /// Thread-safe: signal the query cancel handle (interrupts open/plan/count/next)
    /// and the scan handle if already planned. Does not free the scan.
    if (cancel_handle)
        cancel_handle->requestCancel();
    std::lock_guard lock(scan_mutex);
    if (scan_handle)
        scan_handle->requestCancel();
}

Chunk ReadSource::generate()
{
    if (is_finished || isCancelled())
        return {};

    if (scan.need_only_count && scan.projection.empty())
    {
        if (isCancelled())
            return {};

        try
        {
            const auto rows = scan.predicate || !scan.fragment_ids.empty()
                ? dataset.countRows(scan.snapshot, scan.predicate, scan.fragment_ids, cancel_handle)
                : dataset.totalRows(scan.snapshot, cancel_handle);
            if (rows)
            {
                is_finished = true;
                ProfileEvents::increment(ProfileEvents::LanceRowsRead, *rows);
                addTotalRowsApprox(*rows);
                return Chunk(Columns{}, *rows);
            }
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
    }

    if (!scan_handle)
    {
        if (isCancelled())
            return {};

        /// Plan outside the mutex so a concurrent cancel is not blocked on metadata I/O.
        /// cancel_handle is shared so onCancel can interrupt planScan mid-wait.
        try
        {
            auto planned_scan = dataset.planScan(scan, cancel_handle);
            {
                std::lock_guard lock(scan_mutex);
                if (!scan_handle)
                    scan_handle.emplace(std::move(planned_scan));
                if (isCancelled())
                {
                    if (cancel_handle)
                        cancel_handle->requestCancel();
                    if (scan_handle)
                        scan_handle->requestCancel();
                    return {};
                }
            }
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
    }
    else if (isCancelled())
    {
        if (cancel_handle)
            cancel_handle->requestCancel();
        std::lock_guard lock(scan_mutex);
        if (scan_handle)
            scan_handle->requestCancel();
        return {};
    }

    std::shared_ptr<arrow::RecordBatch> record_batch;
    try
    {
        /// nextBatch without scan_mutex: requestCancel is thread-safe on the Scan/FFI side.
        /// scan_handle stays engaged until the source is destroyed (after generate returns).
        record_batch = scan_handle->nextBatch();
    }
    catch (const Exception & e)
    {
        /// Cooperative cancel: finish the source without propagating an error so the
        /// process-list cancel status remains the query outcome (same idea as object storage sources).
        if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED || isCancelled())
        {
            is_finished = true;
            return {};
        }
        throw;
    }

    if (!record_batch)
    {
        is_finished = true;
        return {};
    }

    if (isCancelled())
        return {};

    size_t batch_rows = static_cast<size_t>(record_batch->num_rows());
    if (scan.limit)
    {
        if (rows_emitted >= *scan.limit)
        {
            is_finished = true;
            return {};
        }
        const size_t remaining = *scan.limit - rows_emitted;
        if (batch_rows > remaining)
        {
            record_batch = record_batch->Slice(0, static_cast<int64_t>(remaining));
            batch_rows = remaining;
        }
    }

    accountLanceBatchMetrics(*record_batch, batch_rows, dataset.options().use_s3);

    ArrowColumnToCHColumn::checkRecordBatchValidityBitmaps(*record_batch);

    if (scan.discard_output_columns)
    {
        rows_emitted += batch_rows;
        if (scan.limit && rows_emitted >= *scan.limit)
            is_finished = true;
        return Chunk(Columns{}, batch_rows);
    }

    Stopwatch convert_watch;
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

    auto chunk = converter->arrowTableToCHChunk(
        *table, (*table)->num_rows(), /* metadata */ nullptr, /* block_missing_values */ nullptr);
    ProfileEvents::increment(ProfileEvents::LanceArrowConvertMicroseconds, convert_watch.elapsedMicroseconds());
    /// Run this after conversion because `ArrowColumnToCHColumn` validates nested offsets before
    /// the nullability check uses Arrow `Flatten` to inspect the projected child values.
    validateRecordBatchNullability(*record_batch, getPort().getHeader());
    rows_emitted += chunk.getNumRows();
    if (scan.limit && rows_emitted >= *scan.limit)
        is_finished = true;
    return chunk;
}

}

#endif
