#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableChanges.h>

#if USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <arrow/table.h>
#include <arrow/c/bridge.h>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

namespace DeltaLake
{

TableChanges::TableChanges(const std::pair<size_t, size_t> & version_range_, KernelHelperPtr helper_)
    : version_range(version_range_)
    , helper(helper_)
{
}

TableChanges::TableChanges(size_t from_version_, KernelHelperPtr helper_)
    : version_range({from_version_, std::nullopt})
    , helper(helper_)
{
}

void TableChanges::initialize()
{
    std::lock_guard lock(mutex);
    if (initialized)
      return;

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

    table_changes_scan = KernelUtils::unwrapResult(
        ffi::table_changes_scan(
            table_changes.release(),
            engine.get(),
            /* predicate */nullptr),
        "table_changes_scan");

    table_changes_scan_iterator = KernelUtils::unwrapResult(
        ffi::table_changes_scan_execute(table_changes_scan.get(), engine.get()),
        "table_changes_scan_execute");

    initialized = true;
}

DB::Chunk TableChanges::next()
{
    ffi::ArrowFFIData ffi_arrow_data = KernelUtils::unwrapResult(
        ffi::scan_table_changes_next(table_changes_scan_iterator.get()),
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

    auto table = arrow::Table::FromRecordBatches(std::vector<std::shared_ptr<arrow::RecordBatch>>{*record_batch});

    UNUSED(table);
    //NameToArrowColumn name_to_arrow_column;

    //for (auto column_name : table->ColumnNames())
    //{
    //    auto arrow_column = table->GetColumnByName(column_name);
    //    if (!arrow_column)
    //        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column '{}' is duplicated", column_name);

    //    auto arrow_field = table->schema()->GetFieldByName(column_name);

    //    if (parquet_columns_to_clickhouse)
    //    {
    //        auto column_name_it = parquet_columns_to_clickhouse->find(column_name);
    //        if (column_name_it == parquet_columns_to_clickhouse->end())
    //        {
    //            throw Exception(
    //                ErrorCodes::LOGICAL_ERROR,
    //                "Column '{}' is not present in input data. Column name mapping has {} columns",
    //                column_name,
    //                parquet_columns_to_clickhouse->size());
    //        }
    //        column_name = column_name_it->second;
    //    }

    //    if (case_insensitive_matching)
    //        boost::to_lower(column_name);

    //    name_to_arrow_column[std::move(column_name)] = {std::move(arrow_column), std::move(arrow_field)};
    //}
    //return arrowColumnsToCHChunk(name_to_arrow_column, num_rows, metadata, block_missing_values);
    //return conv.arrowTableToCHChunk(table, (*record_batch)->num_rows(), nullptr);
    return {};
}

}

#endif
