#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableChanges.h>

#if USE_DELTA_KERNEL_RS

#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/DataObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/EnginePredicate.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/ExpressionVisitor.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/getSchemaFromSnapshot.h>
#include <Common/logger_useful.h>
#include <delta_kernel_ffi.hpp>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace DB::Setting
{
    extern const SettingsBool delta_lake_enable_expression_visitor_logging;
}

namespace ProfileEvents
{
    extern const Event DeltaLakeScannedFiles;
}

namespace DeltaLake
{

namespace
{

using KernelExternEngine = KernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
using KernelTableChanges = KernelPointerWrapper<ffi::ExclusiveTableChanges, ffi::free_table_changes>;
using KernelTableChangesScan = KernelPointerWrapper<ffi::SharedTableChangesScan, ffi::free_table_changes_scan>;
using KernelTableChangesScanFilesIterator
    = KernelPointerWrapper<ffi::SharedTableChangesScanFilesIterator, ffi::free_table_changes_scan_files_iter>;
using KernelSharedSchema = KernelPointerWrapper<ffi::SharedSchema, ffi::free_schema>;
using KernelExpression = KernelPointerWrapper<ffi::SharedExpression, ffi::free_kernel_expression>;

KernelExternEngine createEngine(const IKernelHelper & helper)
{
    return KernelExternEngine(KernelUtils::unwrapResult(ffi::builder_build(helper.createBuilder()), "builder_build"));
}

KernelTableChanges createTableChanges(
    const TableChangesVersionRange & version_range, const IKernelHelper & helper, ffi::SharedExternEngine * engine)
{
    const auto table_location = KernelUtils::toDeltaString(helper.getTableLocation());
    if (version_range.second.has_value())
        return KernelTableChanges(KernelUtils::unwrapResult(
            ffi::table_changes_between_versions(table_location, engine, version_range.first, version_range.second.value()),
            "table_changes_between_versions"));

    return KernelTableChanges(KernelUtils::unwrapResult(
        ffi::table_changes_from_version(table_location, engine, version_range.first),
        "table_changes_from_version"));
}

/// Iterates over the data files of the change data feed, which delta-kernel-rs lists without reading them.
class TableChangesIterator final : public DB::IObjectIterator
{
public:
    TableChangesIterator(
        KernelExternEngine engine_,
        KernelTableChangesScan scan_,
        DB::NamesAndTypesList read_schema_,
        DB::NamesAndTypesList expression_schema_,
        KernelHelperPtr helper_,
        DB::ContextPtr context_,
        DB::IDataLakeMetadata::FileProgressCallback callback_,
        bool enable_expression_visitor_logging_)
        : engine(std::move(engine_))
        , scan(std::move(scan_))
        , read_schema(std::move(read_schema_))
        , cdc_read_schema(read_schema)
        , expression_schema(std::move(expression_schema_))
        , helper(std::move(helper_))
        , context(std::move(context_))
        , callback(std::move(callback_))
        , enable_expression_visitor_logging(enable_expression_visitor_logging_)
        , log(getLogger("DeltaLakeTableChangesIterator"))
    {
        /// CDC files contain the `_change_type` column, which other data files do not contain.
        cdc_read_schema.emplace_back(TableChanges::CHANGE_TYPE_COLUMN, std::make_shared<DB::DataTypeString>());

        files = KernelTableChangesScanFilesIterator(KernelUtils::unwrapResult(
            ffi::table_changes_scan_files_iter_init(scan.get(), engine.get()), "table_changes_scan_files_iter_init"));
    }

    size_t estimatedKeysCount() override
    {
        /// The number of files is not known before the log is read, as for the data files of a snapshot.
        return std::numeric_limits<size_t>::max();
    }

    DB::ObjectInfoPtr next(size_t) override
    {
        std::lock_guard lock(mutex);
        if (finished)
            return nullptr;

        NextFile next_file{.iterator = this, .object = {}, .exception = {}};
        const bool has_file = KernelUtils::unwrapResult(
            ffi::table_changes_scan_files_next(files.get(), &next_file, &onFile), "table_changes_scan_files_next");

        /// The exception cannot be thrown from the callback, because it is called by Rust code.
        if (next_file.exception)
            std::rethrow_exception(next_file.exception);

        if (!has_file)
        {
            finished = true;
            /// Free the kernel objects early, they are not needed anymore.
            files = KernelTableChangesScanFilesIterator();
            return nullptr;
        }

        chassert(next_file.object);
        if (emit_profile_events)
            ProfileEvents::increment(ProfileEvents::DeltaLakeScannedFiles);
        if (callback)
            callback(DB::FileProgress(0, next_file.object->getFileSizeHint().value_or(0)));
        return next_file.object;
    }

private:
    struct NextFile
    {
        TableChangesIterator * iterator;
        DB::ObjectInfoPtr object;
        std::exception_ptr exception;
    };

    static void onFile(
        ffi::NullableCvoid engine_context,
        ffi::KernelStringSlice path,
        int64_t size,
        bool is_cdc,
        ffi::OptionalValue<ffi::SharedExpression *> transform,
        const uint64_t * row_indexes,
        uintptr_t row_indexes_len,
        bool row_indexes_are_selection)
    {
        /// Take ownership of the handle first, so that it is freed on any exit path.
        std::optional<KernelExpression> transform_handle;
        if (transform.tag == ffi::OptionalValue<ffi::SharedExpression *>::Tag::Some)
            transform_handle.emplace(transform.some._0);

        auto * next_file = static_cast<NextFile *>(engine_context);
        try
        {
            next_file->object = next_file->iterator->makeObject(
                KernelUtils::fromDeltaString(path),
                size,
                is_cdc,
                transform_handle ? transform_handle->get() : nullptr,
                row_indexes,
                row_indexes_len,
                row_indexes_are_selection);
        }
        catch (...)
        {
            next_file->exception = std::current_exception();
        }
    }

    DB::ObjectInfoPtr makeObject(
        const std::string & path,
        int64_t size,
        bool is_cdc,
        ffi::SharedExpression * transform,
        const uint64_t * row_indexes,
        size_t row_indexes_len,
        bool row_indexes_are_selection) const
    {
        /// The transform adds the columns of the change data feed, so it is always present.
        if (!transform)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "No transform expression for the change data feed file {}", path);

        /// The path can be an absolute URI, e.g. in a shallow clone, which references the files of its source table.
        auto object = std::make_shared<DataObjectInfo>(helper->resolveDataFilePath(path, context), size);
        auto & metadata = object->data_lake_metadata.emplace();

        metadata.schema_transform = visitScanCallbackExpression(
            transform, is_cdc ? cdc_read_schema : read_schema, expression_schema, enable_expression_visitor_logging);

        if (is_cdc)
            metadata.additional_file_columns = std::make_shared<const DB::NamesAndTypesList>(
                DB::NamesAndTypesList{{TableChanges::CHANGE_TYPE_COLUMN, std::make_shared<DB::DataTypeString>()}});

        if (row_indexes_len > 0)
        {
            auto rows = std::make_shared<DB::DataLakeObjectMetadata::ExcludedRows>();
            for (size_t i = 0; i < row_indexes_len; ++i)
                rows->add(row_indexes[i]);

            if (row_indexes_are_selection)
                metadata.selected_rows = std::move(rows);
            else
                metadata.excluded_rows = std::move(rows);
        }

        LOG_TEST(
            log, "Change data feed file: {}, size: {}, CDC file: {}, row indexes: {} ({}), transform: {}",
            object->getPath(), size, is_cdc, row_indexes_len, row_indexes_are_selection ? "selected" : "excluded",
            metadata.schema_transform->dumpNames());

        return object;
    }

    /// The order of the members is important: the kernel objects are freed in the reverse order.
    KernelExternEngine engine;
    KernelTableChangesScan scan;
    KernelTableChangesScanFilesIterator files;

    const DB::NamesAndTypesList read_schema;
    DB::NamesAndTypesList cdc_read_schema;
    const DB::NamesAndTypesList expression_schema;
    const KernelHelperPtr helper;
    const DB::ContextPtr context;
    const DB::IDataLakeMetadata::FileProgressCallback callback;
    const bool enable_expression_visitor_logging;
    const LoggerPtr log;

    std::mutex mutex;
    bool finished = false;
};

}

TableChanges::TableChanges(const TableChangesVersionRange & version_range_, KernelHelperPtr helper_)
    : version_range(version_range_)
    , helper(helper_)
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

bool TableChanges::isChangeDataFeedColumn(const std::string & name)
{
    return name == CHANGE_TYPE_COLUMN || name == "_commit_version" || name == "_commit_timestamp";
}

DB::NamesAndTypesList TableChanges::getSchema() const
{
    std::lock_guard lock(mutex);
    if (schema.has_value())
        return schema.value();

    auto engine = createEngine(*helper);
    auto table_changes = createTableChanges(version_range, *helper, engine.get());
    KernelSharedSchema kernel_schema(ffi::table_changes_schema(table_changes.get()));
    schema = convertToClickHouseSchema(kernel_schema.get(), engine.get());

    LOG_TRACE(log, "Change data feed schema: {}", schema->toString());
    return schema.value();
}

DB::ObjectIterator TableChanges::iterate(
    const DB::ActionsDAG * filter_dag,
    const DB::NameToNameMap & physical_names_map,
    DB::IDataLakeMetadata::FileProgressCallback callback,
    DB::ContextPtr context) const
{
    auto engine = createEngine(*helper);
    auto table_changes = createTableChanges(version_range, *helper, engine.get());

    /// The logical schema of the change data feed, with physical names, is the result of the transform of every file.
    /// The fields of structs have physical names too, as in the columns read from the data files: a requested subcolumn
    /// (e.g. `c-t.c-t-x` for `t.x`) is found by its physical name in the type of the column that the transform outputs.
    KernelSharedSchema logical_schema(ffi::table_changes_schema(table_changes.get()));
    auto expression_schema = convertToClickHouseSchema(logical_schema.get(), engine.get());
    if (!physical_names_map.empty())
    {
        for (auto & [name, type] : expression_schema)
        {
            if (isChangeDataFeedColumn(name))
                continue;
            const auto logical_path = appendToLogicalPath({}, name);
            name = getPhysicalName(logical_path, physical_names_map);
            type = replaceTypeNamesToPhysicalRecursively(type, logical_path, name, physical_names_map);
        }
    }

    /// `table_changes_scan` consumes the table changes.
    std::exception_ptr predicate_exception;
    std::shared_ptr<EnginePredicate> predicate;
    if (filter_dag)
        predicate = getEnginePredicate(*filter_dag, predicate_exception, context);
    KernelTableChangesScan scan(KernelUtils::unwrapResult(
        ffi::table_changes_scan(table_changes.release(), engine.get(), predicate.get()), "table_changes_scan"));
    if (predicate_exception)
        std::rethrow_exception(predicate_exception);

    /// The columns of the data files, with physical names.
    KernelSharedSchema physical_schema(ffi::table_changes_scan_physical_schema(scan.get()));
    auto read_schema = convertToClickHouseSchema(physical_schema.get(), engine.get());

    LOG_TRACE(
        log, "Reading the change data feed of versions [{}, {}], read schema: {}, expression schema: {}",
        version_range.first, version_range.second ? DB::toString(*version_range.second) : "latest",
        read_schema.toString(), expression_schema.toString());

    return std::make_shared<TableChangesIterator>(
        std::move(engine),
        std::move(scan),
        std::move(read_schema),
        std::move(expression_schema),
        helper,
        context,
        std::move(callback),
        context->getSettingsRef()[DB::Setting::delta_lake_enable_expression_visitor_logging]);
}

}

#endif
