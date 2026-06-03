#include <Formats/FormatSettings.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableChanges.h>

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/EnginePredicate.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Formats/FormatFactory.h>
#include <Common/logger_useful.h>
#include <arrow/table.h>
#include <arrow/c/bridge.h>


namespace DB::ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int BAD_ARGUMENTS;
}

namespace DeltaLake
{

TableChanges::TableChanges(
    const TableChangesVersionRange & version_range_,
    KernelHelperPtr helper_,
    const DB::Block & header_,
    const std::optional<DB::FormatSettings> & format_settings_,
    const std::string & format_name_,
    DB::ContextPtr context_)
    : DB::WithContext(context_)
    , version_range(version_range_)
    , helper(helper_)
    , header(header_)
    , format_settings(format_settings_.value_or(DB::getFormatSettings(context_)))
    , format_name(format_name_)
    , log(getLogger("TableChanges"))
{
}

TableChangesVersionRange TableChanges::getVersionRange(int64_t start_version, int64_t end_version)
{
    if (start_version < 0)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Snapshot start version cannot be a negative value ({})", start_version);

    if (end_version != TableSnapshot::LATEST_SNAPSHOT_VERSION)
    {
        if (end_version < 0)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Snapshot end version cannot be a negative value ({})", end_version);

        if (end_version < start_version)
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Snapshot end version cannot be less than snapshot start version ({} < {})", end_version, start_version);
    }

    return end_version == TableSnapshot::LATEST_SNAPSHOT_VERSION
        ? DeltaLake::TableChangesVersionRange{start_version, std::nullopt}
        : DeltaLake::TableChangesVersionRange{start_version, end_version};
}

DB::NamesAndTypesList TableChanges::getSchema() const
{
    std::lock_guard lock(mutex);
    return getSchemaUnlocked();
}

DB::NamesAndTypesList TableChanges::getSchemaUnlocked() const
{
    if (schema.has_value())
        return schema.value();

    using KernelSharedSchema = KernelPointerWrapper<ffi::SharedSchema, ffi::free_schema>;
    KernelSharedSchema kernel_schema(ffi::table_changes_schema(getTableChanges().get()));
    schema = convertToClickHouseSchema(kernel_schema.get());

    LOG_TRACE(log, "Table schema: {}, source header: {}", schema->toString(), header.dumpNames());
    return schema.value();
}

TableChanges::KernelTableChanges & TableChanges::getTableChanges() const
{
    if (table_changes.get())
        return table_changes;

    auto * engine_builder = helper->createBuilder();
    engine = KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");
    table_changes = version_range.second.has_value()
        ? KernelUtils::unwrapResult(
            ffi::table_changes_between_versions(
                KernelUtils::toDeltaString(helper->getTableLocation()),
                engine.get(),
                version_range.first,
                version_range.second.value()),
            "table_changes_between_versions")
        : KernelUtils::unwrapResult(
            ffi::table_changes_from_version(
                KernelUtils::toDeltaString(helper->getTableLocation()),
                engine.get(),
                version_range.first),
            "table_changes_from_version");

    return table_changes;
}

TableChanges::KernelTableChangesScanIterator & TableChanges::getTableChangesScanIterator() const
{
    if (table_changes_scan_iterator.get())
        return table_changes_scan_iterator;

    std::exception_ptr engine_predicate_exception;
    if (filter.has_value())
    {
        auto predicate = getEnginePredicate(filter.value(), engine_predicate_exception, getContext());
        table_changes_scan = KernelUtils::unwrapResult(
            ffi::table_changes_scan(
                getTableChanges().release(),
                engine.get(),
                predicate.get()),
            "table_changes_scan (with predicate)");
    }
    else
    {
        table_changes_scan = KernelUtils::unwrapResult(
            ffi::table_changes_scan(
                getTableChanges().release(),
                engine.get(),
                /* predicate */nullptr),
            "table_changes_scan");
    }

    if (engine_predicate_exception)
        std::rethrow_exception(engine_predicate_exception);

    table_changes_scan_iterator = KernelUtils::unwrapResult(
        ffi::table_changes_scan_execute(table_changes_scan.get(), engine.get()),
        "table_changes_scan_execute");

    LOG_TEST(log, "Initialized table changes scan iterator");
    chassert(table_changes_scan_iterator.get());
    return table_changes_scan_iterator;
}

DB::Chunk TableChanges::next()
{
    std::lock_guard lock(mutex);

    ffi::ArrowFFIData ffi_arrow_data = KernelUtils::unwrapResult(
        ffi::scan_table_changes_next(getTableChangesScanIterator().get()),
        "scan_table_changes_next");

    if (ffi_arrow_data.array.length == 0)
        return {};

    auto record_batch = arrow::ImportRecordBatch(
        reinterpret_cast<ArrowArray *>(&ffi_arrow_data.array),
        reinterpret_cast<ArrowSchema *>(&ffi_arrow_data.schema));

    if (!record_batch.ok())
    {
        throw DB::Exception(
            DB::ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to create chunks batch: {}",
            record_batch.status().ToString());
    }

    auto table_result = arrow::Table::FromRecordBatches(std::vector<std::shared_ptr<arrow::RecordBatch>>{*record_batch});
    if (!table_result.ok())
        throw DB::Exception(DB::ErrorCodes::UNKNOWN_EXCEPTION,
            "Error while reading batch of Arrow data: {}", table_result.status().ToString());

    const std::shared_ptr<arrow::Table> & table = *table_result;
    const size_t num_rows = (*table_result)->num_rows();
    if (num_rows == 0)
        return {};

    LOG_TEST(log, "Received {} rows", num_rows);

    /// Currently delta-kernel CDF is supported only with disabled columnMapping mode.
    /// So parquet file column names will exactly match table column names.
    /// However, once non-default columnMapping modes are supported,
    /// some adjustments will be needed to make it work here.

    DB::ArrowColumnToCHColumn arrow_conv(
        header, format_name, format_settings,
        /* parquet_columns_to_clickhouse */std::nullopt,
        /* clickhouse_columns_to_parquet */std::nullopt,
        format_settings.parquet.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.allow_geoparquet_parser,
        format_settings.parquet.case_insensitive_column_matching,
        /* is_stream_ */false,
        format_settings.parquet.enable_json_parsing);

    return arrow_conv.arrowTableToCHChunk(table, num_rows, /* metadata */nullptr, /* block_missing_values */nullptr);
}

}

#endif
