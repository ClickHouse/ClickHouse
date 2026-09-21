#include <Storages/StorageMaxComputeArrow.h>

#if USE_ODPS_TUNNEL && USE_ODPS_ARROW

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <limits>
#include <set>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int UNSUPPORTED_METHOD;
    extern const int CANNOT_CONVERT_TYPE;
}

namespace
{

    /// Whether a header column with this ODPS top-level name takes part in the
    /// MAP-as-Nested-pair mapping (its header name is `name.suffix`).
    bool headerHasNestedPairFor(const Block & sample_block, const std::string & name)
    {
        const std::string prefix = name + ".";
        for (const auto & column : sample_block)
            if (column.name.starts_with(prefix))
                return true;
        return false;
    }

    bool hasTemporalType(const DataTypePtr & type)
    {
        const auto nested = removeNullable(type);
        if (nested->getTypeId() == TypeIndex::Date || nested->getTypeId() == TypeIndex::DateTime)
            return true;
        if (const auto * array = typeid_cast<const DataTypeArray *>(nested.get()))
            return hasTemporalType(array->getNestedType());
        if (const auto * tuple = typeid_cast<const DataTypeTuple *>(nested.get()))
            return std::any_of(tuple->getElements().begin(), tuple->getElements().end(), hasTemporalType);
        return false;
    }

    /// Validate before the shared converter narrows temporal values. Its final cast is context-less.
    /// Null parents and unused dictionary entries do not contribute values to the result.
    void validateTemporalValues(const arrow::Array & array, const DataTypePtr & type, const String & name)
    {
        if (!hasTemporalType(type))
            return;
        const auto nested = removeNullable(type);
        if (array.type_id() == arrow::Type::DICTIONARY)
        {
            const auto & dictionary = assert_cast<const arrow::DictionaryArray &>(array);
            for (int64_t i = 0; i < array.length(); ++i)
                if (!array.IsNull(i))
                    validateTemporalValues(*dictionary.dictionary()->Slice(dictionary.GetValueIndex(i), 1), type, name);
            return;
        }

        if (const auto * array_type = typeid_cast<const DataTypeArray *>(nested.get()))
        {
            const auto validate_list = [&](const auto & list)
            {
                for (int64_t i = 0; i < list.length(); ++i)
                    if (!list.IsNull(i))
                        validateTemporalValues(
                            *list.values()->Slice(list.value_offset(i), list.value_length(i)),
                            array_type->getNestedType(), name);
            };
            switch (array.type_id())
            {
                case arrow::Type::LIST:
                    validate_list(assert_cast<const arrow::ListArray &>(array));
                    return;
                case arrow::Type::LARGE_LIST:
                    validate_list(assert_cast<const arrow::LargeListArray &>(array));
                    return;
                case arrow::Type::FIXED_SIZE_LIST:
                    validate_list(assert_cast<const arrow::FixedSizeListArray &>(array));
                    return;
                default:
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "MaxCompute temporal array '{}' requires an Arrow list", name);
            }
        }

        if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(nested.get()))
        {
            if (array.type_id() != arrow::Type::STRUCT)
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "MaxCompute temporal tuple '{}' requires an Arrow struct", name);
            const auto & structure = assert_cast<const arrow::StructArray &>(array);
            const auto & struct_type = assert_cast<const arrow::StructType &>(*array.type());
            for (size_t field = 0; field < tuple_type->getElements().size(); ++field)
            {
                const auto & field_type = tuple_type->getElement(field);
                if (!hasTemporalType(field_type))
                    continue;
                const int index = tuple_type->hasExplicitNames()
                    ? struct_type.GetFieldIndex(tuple_type->getElementNames()[field])
                    : static_cast<int>(field);
                if (index < 0 || index >= struct_type.num_fields())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "MaxCompute temporal tuple '{}' is missing field {}", name, field);
                const auto child = structure.field(index);
                for (int64_t i = 0; i < array.length(); ++i)
                    if (!array.IsNull(i))
                        validateTemporalValues(*child->Slice(i, 1), field_type, name);
            }
            return;
        }

        if (nested->getTypeId() == TypeIndex::Date)
        {
            if (array.type_id() != arrow::Type::DATE32)
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "MaxCompute Date column '{}' requires Arrow date32", name);
            const auto & dates = assert_cast<const arrow::Date32Array &>(array);
            for (int64_t i = 0; i < dates.length(); ++i)
                if (!dates.IsNull(i) && (dates.Value(i) < 0 || dates.Value(i) > std::numeric_limits<UInt16>::max()))
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "MaxCompute Date value is out of range at column '{}'", name);
            return;
        }

        if (array.type_id() != arrow::Type::TIMESTAMP)
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "MaxCompute DateTime column '{}' requires an Arrow timestamp", name);
        const auto & timestamps = assert_cast<const arrow::TimestampArray &>(array);
        const auto & timestamp_type = assert_cast<const arrow::TimestampType &>(*array.type());
        Int64 units_per_second = 0;
        switch (timestamp_type.unit())
        {
            case arrow::TimeUnit::SECOND: units_per_second = 1; break;
            case arrow::TimeUnit::MILLI: units_per_second = 1000; break;
            case arrow::TimeUnit::MICRO: units_per_second = 1000000; break;
            case arrow::TimeUnit::NANO: units_per_second = 1000000000; break;
        }
        if (units_per_second == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "MaxCompute column '{}' has an unknown timestamp unit", name);

        for (int64_t i = 0; i < timestamps.length(); ++i)
        {
            if (timestamps.IsNull(i))
                continue;
            const Int64 value = timestamps.Value(i);
            if (value < 0 || value / units_per_second > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "MaxCompute DateTime value is out of range at column '{}'", name);
        }
    }

}

void OdpsArrowColumnMatcher::validate(
    const apsara::odps::sdk::IODPSTableSchema * odps_schema,
    const std::vector<std::string> & odps_read_cols,
    const Block & sample_block)
{
    if (!odps_schema)
        throw Exception(ErrorCodes::INCORRECT_DATA, "MaxCompute download session has no table schema");

    for (const auto & odps_col_name : odps_read_cols)
    {
        const apsara::odps::sdk::IODPSTableColumn * odps_column = nullptr;
        for (uint32_t i = 0, n = odps_schema->GetColumnCount(); i < n; ++i)
        {
            if (odps_schema->GetTableColumn(i).GetName() == odps_col_name)
            {
                odps_column = &odps_schema->GetTableColumn(i);
                break;
            }
        }

        if (!odps_column)
            throw Exception(ErrorCodes::INCORRECT_DATA, "MaxCompute column '{}' is missing from the download schema", odps_col_name);

        const auto odps_type = odps_column->GetType();
        switch (odps_type)
        {
            case apsara::odps::sdk::ODPSColumnType::ODPS_INTERVAL_YEAR_MONTH:
            case apsara::odps::sdk::ODPSColumnType::ODPS_INTERVAL_DAY_TIME:
            case apsara::odps::sdk::ODPSColumnType::ODPS_TIMESTAMP:
            case apsara::odps::sdk::ODPSColumnType::ODPS_TIMESTAMP_NTZ:
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "MaxCompute column '{}' cannot be read with the Arrow format: unsupported ODPS type {}",
                    odps_col_name, odps_column->ToString());
            case apsara::odps::sdk::ODPSColumnType::ODPS_MAP:
                if (!headerHasNestedPairFor(sample_block, odps_col_name))
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_METHOD,
                        "MaxCompute column '{}' cannot be read with the Arrow format: MAP requires Nested key/value columns",
                        odps_col_name);
                break;
            case apsara::odps::sdk::ODPSColumnType::ODPS_ARRAY:
            case apsara::odps::sdk::ODPSColumnType::ODPS_STRUCT:
            {
                const auto * header_column = sample_block.findByName(odps_col_name);
                const bool is_array = odps_type == apsara::odps::sdk::ODPSColumnType::ODPS_ARRAY;
                if (!header_column || (is_array ? !isArray(header_column->type) : !isTuple(header_column->type)))
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_METHOD,
                        "MaxCompute column '{}' cannot be read with the Arrow format: {} requires a {} declaration",
                        odps_col_name, is_array ? "ARRAY" : "STRUCT", is_array ? "Array" : "Tuple");
                break;
            }
            default:
                break;
        }
    }
}

OdpsArrowBatchPreprocessor::OdpsArrowBatchPreprocessor(const Block & sample_block_)
{
    /// Mirror the MAP folding of `StorageMaxCompute::read`: any header column
    /// named `prefix.suffix` is served by the ODPS top-level column `prefix`,
    /// which arrives from the Arrow stream as a single map column to split.
    std::set<std::string> seen;
    for (const auto & column : sample_block_)
    {
        const auto pos = column.name.find('.');
        if (pos == std::string::npos)
            continue;

        const std::string prefix = column.name.substr(0, pos);
        if (seen.insert(prefix).second)
            map_columns_to_split.push_back(prefix);
    }
}

std::shared_ptr<arrow::RecordBatch> OdpsArrowBatchPreprocessor::process(
    const std::shared_ptr<arrow::RecordBatch> & batch) const
{
    if (map_columns_to_split.empty())
        return batch;

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    fields.reserve(static_cast<size_t>(batch->num_columns()) + map_columns_to_split.size());
    columns.reserve(static_cast<size_t>(batch->num_columns()) + map_columns_to_split.size());

    for (int i = 0, num_columns = batch->num_columns(); i < num_columns; ++i)
    {
        const auto & field = batch->schema()->field(i);
        const auto & column = batch->column(i);

        if (std::find(map_columns_to_split.begin(), map_columns_to_split.end(), field->name())
            == map_columns_to_split.end())
        {
            fields.push_back(field);
            columns.push_back(column);
            continue;
        }

        if (field->type()->id() != arrow::Type::MAP)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "ODPS Arrow column '{}' was expected to be a MAP but has Arrow type {}",
                field->name(),
                field->type()->name());

        const auto * map_type = static_cast<const arrow::MapType *>(field->type().get());
        const auto & map_array = assert_cast<const arrow::MapArray &>(*column);

        /// Zero-copy split: both list arrays share the map's offsets buffer
        /// and null bitmap, they only differ in the flat child array and the
        /// element type. The list lengths stay equal to the map length, so
        /// per-row offsets semantics are preserved exactly.
        fields.push_back(arrow::field(field->name() + ".key", arrow::list(map_type->key_type()), field->nullable()));
        columns.push_back(std::make_shared<arrow::ListArray>(
            arrow::list(map_type->key_type()),
            map_array.length(),
            map_array.value_offsets(),
            map_array.keys(),
            map_array.null_bitmap(),
            map_array.null_count(),
            map_array.offset()));

        fields.push_back(arrow::field(field->name() + ".value", arrow::list(map_type->item_type()), field->nullable()));
        columns.push_back(std::make_shared<arrow::ListArray>(
            arrow::list(map_type->item_type()),
            map_array.length(),
            map_array.value_offsets(),
            map_array.items(),
            map_array.null_bitmap(),
            map_array.null_count(),
            map_array.offset()));
    }

    return arrow::RecordBatch::Make(
        std::make_shared<arrow::Schema>(std::move(fields)), batch->num_rows(), std::move(columns));
}

MaxComputeArrowSource::MaxComputeArrowSource(
    MaxComputeReadSessionPtr session_,
    const String & logger_name_,
    UInt64 max_block_size_,
    UInt64 count_,
    UInt64 start_,
    const Block & sample_block_,
    const std::vector<std::string> & odps_read_cols_,
    bool compress_,
    UInt64 max_batch_bytes_,
    UInt64 max_retries_,
    UInt64 retry_initial_backoff_ms_,
    UInt64 retry_max_backoff_ms_,
    UInt64 retry_max_elapsed_ms_,
    const FormatSettings & format_settings_)
    : ISource(std::make_shared<const Block>(sample_block_.cloneEmpty()))
    , log(&Poco::Logger::get(logger_name_))
    , session(std::move(session_))
    , count(count_)
{
    sample_block = sample_block_;
    if (count != 0)
    {
        /// Compression mapping. The row-based path passes a bool that the SDK
        /// expands to `mConf.GetCompressOption()` (the Configuration default,
        /// ZLIB); that cannot be mirrored here because ZLIB is rejected by
        /// `OpenArrowReader` outright ("not compatible with arrow"), and the
        /// failure would surface on every batch. ZSTD is the Arrow-compatible
        /// stand-in; `false` keeps the same NO_COMPRESS meaning as the
        /// row-based path.
        apsara::odps::sdk::CompressOption compress_option = compress_
            ? apsara::odps::sdk::CompressOption::ZSTD_COMPRESS
            : apsara::odps::sdk::CompressOption::NO_COMPRESS;

        /// One batch per chunk: the server-side batch row limit is
        /// `max_block_size` so a batch feeds one chunk directly.
        reader = std::make_unique<AutoReconnectArrowReader>(
            [read_session = this->session]
            {
                return read_session->createReaderDownload();
            },
            start_,
            count,
            max_block_size_,
            max_batch_bytes_,
            odps_read_cols_,
            compress_option,
            max_retries_,
            retry_initial_backoff_ms_,
            retry_max_backoff_ms_,
            retry_max_elapsed_ms_,
            [this]
            {
                return isCancelled();
            },
            log);
    }
    else
    {
        finishTask();
        return;
    }

    preprocessor = std::make_unique<OdpsArrowBatchPreprocessor>(sample_block);

    /// DATETIME lands as timestamp(MILLI) -> DateTime64(3) and is aligned to
    /// the header type by the converter's built-in `castColumn`: a DateTime
    /// header truncates to seconds, exactly like the row-based path's
    /// `ms / 1000` integer division.
    /// `is_stream=true` clears the converter's dictionary cache per batch:
    /// each ODPS batch is decoded from its own IPC stream, so dictionaries
    /// are never shared across batches and a stale cache would corrupt
    /// dictionary-encoded columns.
    /// External-table reads must not silently replace `NULL`s or saturate temporal values,
    /// regardless of the user's input-format settings.
    auto strict_format_settings = format_settings_;
    strict_format_settings.null_as_default = false;
    strict_format_settings.date_time_overflow_behavior = FormatSettings::DateTimeOverflowBehavior::Throw;
    converter = std::make_unique<ArrowColumnToCHColumn>(
        sample_block,
        "ODPSArrow",
        strict_format_settings,
        std::nullopt,
        std::nullopt,
        /*allow_missing_columns=*/ false,
        /*null_as_default=*/ false,
        FormatSettings::DateTimeOverflowBehavior::Throw,
        /*allow_geoparquet_parser=*/ false,
        /*case_insensitive_matching=*/ false,
        /*is_stream=*/ true);
}

MaxComputeArrowSource::~MaxComputeArrowSource()
{
    try
    {
        if (reader)
            reader->close();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to close ODPS Arrow reader");
    }
}

Chunk MaxComputeArrowSource::generate()
{
    if (count == 0 || eof)
        return {};

    std::shared_ptr<arrow::RecordBatch> batch;
    if (!reader->read(batch))
    {
        eof = true;
        reader->close();
        finishTask();
        UInt64 total_time_ns = watch.elapsed();
        LOG_TRACE(log, "MaxCompute Arrow reader finished. Total cost: {}ms, read rows: {}",
                  total_time_ns / 1000000, reader->totalReadRows());
        return {};
    }

    /// Malformed validity bitmaps must be rejected before the batch is turned
    /// into a table (Arrow would read out of bounds while recomputing null
    /// counts over a truncated bitmap).
    ArrowColumnToCHColumn::checkRecordBatchValidityBitmaps(*batch);

    /// MAP splitting happens before conversion: the converter matches by
    /// column name and the header exposes `col.key` / `col.value`, while the
    /// ODPS Arrow stream carries the single map column `col`.
    batch = preprocessor->process(batch);

    for (const auto & column : sample_block)
    {
        if (!hasTemporalType(column.type))
            continue;
        const auto values = batch->GetColumnByName(column.name);
        if (!values)
            throw Exception(ErrorCodes::INCORRECT_DATA, "MaxCompute batch is missing column '{}'", column.name);
        const auto status = values->ValidateFull();
        if (!status.ok())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid MaxCompute Arrow column '{}': {}", column.name, status.ToString());
        validateTemporalValues(*values, column.type, column.name);
    }

    auto table_result = arrow::Table::FromRecordBatches({batch});
    if (!table_result.ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot construct Arrow table from MaxCompute batch: {}", table_result.status().ToString());
    auto table = std::move(table_result).ValueUnsafe();
    return converter->arrowTableToCHChunk(table, static_cast<size_t>(batch->num_rows()), nullptr);
}

void MaxComputeArrowSource::finishTask()
{
    if (task_finished)
        return;
    task_finished = true;
    session->finishTask(count, reader ? reader->totalReadRows() : 0);
}

}

#endif
