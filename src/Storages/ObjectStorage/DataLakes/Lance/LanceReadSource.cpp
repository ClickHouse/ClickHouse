#include "config.h"

#if USE_LANCE

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>
#include <Common/Exception.h>

#include <arrow/array/array_nested.h>
#include <arrow/table.h>

#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
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

}

ReadSource::ReadSource(
    const Block & header, ObjectInfoPtr object_info_, DatasetOptions options_, ScanDescription scan_, FormatSettings format_settings_)
    : ISource(std::make_shared<const Block>(header), false)
    , object_info(std::move(object_info_))
    , options(std::move(options_))
    , scan(std::move(scan_))
    , format_settings(std::move(format_settings_))
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
        const auto rows = scan.predicate ? dataset->countRows(scan.snapshot, scan.predicate) : dataset->totalRows(scan.snapshot);
        if (rows)
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

    ArrowColumnToCHColumn::checkRecordBatchValidityBitmaps(*record_batch);

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
    /// Run this after conversion because `ArrowColumnToCHColumn` validates nested offsets before
    /// the nullability check uses Arrow `Flatten` to inspect the projected child values.
    validateRecordBatchNullability(*record_batch, getPort().getHeader());
    return chunk;
}

}

#endif
