#include <Storages/ObjectStorage/DataLakes/Iceberg/AlterDropPartitionExecutor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SnapshotSummary.h>

#if USE_AVRO
/// NOLINTBEGIN(clang-analyzer-core.uninitialized.UndefReturn)
/// avro uses nasty '*std::any_cast' which triggers clang-tidy, the warning is false positive since
/// a type and value are consistent in avro::GenericDatum, and even more - all avro manifests in iceberg
/// consist only of AVRO_RECORDS

#include <Core/Block.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/logger_useful.h>

#include <base/defines.h>
#include <base/scope_guard.h>

#include <Poco/JSON/Array.h>

#include <limits>
#include <memory>
#include <set>
#include <string>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INVALID_PARTITION_VALUE;
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int ICEBERG_SPECIFICATION_VIOLATION;
extern const int CONCURRENT_ACCESS_NOT_SUPPORTED;
}

namespace DataLakeStorageSetting
{
extern const DataLakeStorageSettingsBool iceberg_use_version_hint;
}

namespace FailPoints
{
extern const char iceberg_writes_cleanup[];
extern const char iceberg_drop_partition_pause_after_discovery[];
}

namespace Iceberg
{

/// One global retry cap shared with INSERT/UPDATE/DELETE.
static constexpr auto MAX_TRANSACTION_RETRIES = 100;

namespace
{

bool partitionEquals(const Row & lhs, const Row & rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t i = 0; i < lhs.size(); ++i)
        if (!accurateEquals(lhs[i], rhs[i]))
            return false;
    return true;
}

void validateDropPartitionAST(const ASTPartition & ast, const PartitionCommand & command)
{
    if (ast.all)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} ALL is not supported for Iceberg", command.typeToString());
    if (ast.id)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} ID is not supported for Iceberg", command.typeToString());
    if (!ast.value)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't have partition value", command.typeToString());
}

/// Evaluate the user-supplied DROP PARTITION expression against the Iceberg
/// partition spec, following the same convention as `MergeTree`
/// (`MergeTreeData::getPartitionIDFromQuery`).
///
/// `ParserPartition` only accepts an `ASTLiteral` or an `ASTFunction` named
/// `tuple` here, so we only need to handle three shapes:
///   - `ASTLiteral` with a scalar `Field`           — e.g. `DROP PARTITION 7`
///   - `ASTLiteral` with `Field::Types::Tuple`      — e.g. `DROP PARTITION (3, '4')`
///   - `ASTFunction{name=="tuple"}`                 — e.g. `DROP PARTITION (icebergBucket(4, 'abc'))`
///
/// For the function form, each argument is constant-folded with
/// `evaluateConstantExpression` (so transforms like `icebergBucket(4, 'abc')`
/// or `toYearNumSinceEpoch(toDate('2025-01-01'))` evaluate to their
/// partition-key value). Each resulting `Field` is then coerced to the
/// corresponding partition-result type via `convertFieldToTypeOrThrow`.
Row parsePartitionTuple(const IAST & value_ast, const DataTypes & partition_types, ContextPtr context)
{
    const auto partitions_fields_count = partition_types.size();
    auto wrong_partition_fields_count = [&](size_t got)
    {
        return Exception(
            ErrorCodes::INVALID_PARTITION_VALUE,
            "Wrong number of fields in the partition expression: {}, must be: {}",
            got,
            partitions_fields_count);
    };

    Row out(partitions_fields_count);

    if (const auto * lit = value_ast.as<ASTLiteral>())
    {
        if (lit->value.getType() == Field::Types::Tuple)
        {
            const auto & tuple = lit->value.safeGet<Tuple>();
            if (tuple.size() != partitions_fields_count)
                throw wrong_partition_fields_count(tuple.size());
            for (size_t i = 0; i < partitions_fields_count; ++i)
                out[i] = convertFieldToTypeOrThrow(tuple[i], *partition_types[i]);
            return out;
        }

        if (partitions_fields_count != 1)
            throw wrong_partition_fields_count(1);
        out[0] = convertFieldToTypeOrThrow(lit->value, *partition_types[0]);
        return out;
    }

    const auto * fn = value_ast.as<ASTFunction>();
    if (!fn || fn->name != "tuple")
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Expected literal or tuple for partition key, got {}", value_ast.getID());

    const auto & args = fn->arguments ? fn->arguments->children : ASTs{};
    if (args.size() != partitions_fields_count)
        throw wrong_partition_fields_count(args.size());

    for (size_t i = 0; i < partitions_fields_count; ++i)
    {
        Field value = evaluateConstantExpression(args[i], context).first;
        out[i] = convertFieldToTypeOrThrow(value, *partition_types[i]);
    }
    return out;
}

DataTypes resolvePartitionTypes(
    const Poco::JSON::Object & partition_spec,
    const Poco::JSON::Object & current_schema,
    const IcebergSchemaProcessor & schema_processor,
    Int32 schema_id,
    ContextPtr context)
{
    auto partition_fields = partition_spec.getArray(f_fields);

    std::vector<Int32> source_ids;
    for (size_t i = 0; i < partition_fields->size(); ++i)
    {
        auto field_object = partition_fields->getObject(static_cast<UInt32>(i));
        auto field_source_id = field_object->getValue<Int32>(f_source_id);
        source_ids.emplace_back(field_source_id);
    }

    auto names_and_types = schema_processor.tryGetFieldsCharacteristics(schema_id, source_ids);
    if (names_and_types.size() != source_ids.size())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Could not resolve all partition source columns against schema {} (got {}/{} fields)",
            schema_id,
            names_and_types.size(),
            source_ids.size());

    Block block;
    for (const auto & [name, type] : names_and_types)
        block.insert(ColumnWithTypeAndName{nullptr, type, name});

    SharedHeader sample_block = std::make_shared<const Block>(std::move(block));
    auto schema_fields = current_schema.getArray(f_fields);

    if (!schema_fields || schema_fields->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not find key '{}' in schema {} or fields is empty", f_fields, schema_id);

    ChunkPartitioner partitioner(partition_fields, schema_fields, context, sample_block);
    return partitioner.getResultTypes();
}

}

AlterDropPartitionExecutor::AlterDropPartitionExecutor(
    const PartitionCommand & command_,
    const IcebergMetadata & metadata_,
    ContextPtr context_,
    ObjectStoragePtr object_storage_,
    const PersistentTableComponents & components_,
    const DataLakeStorageSettings & data_lake_settings_,
    String write_format_,
    LoggerPtr log_)
    : command(command_)
    , metadata(metadata_)
    , context(context_)
    , object_storage(std::move(object_storage_))
    , components(components_)
    , data_lake_settings(data_lake_settings_)
    , write_format(std::move(write_format_))
    , log(std::move(log_))
{
}

std::optional<AlterDropPartitionExecutor::SnapshotState> AlterDropPartitionExecutor::fetchSnapshotState()
{
    SnapshotState state;

    {
        auto [snapshot, table_state] = metadata.getRelevantState(
            context,
            /*force_fetch_latest_metadata=*/true,
            /*ignore_explicit_metadata_file_path=*/true);
        if (!snapshot)
            return std::nullopt;

        /// FIXME: in all other places schema_id is int32
        if (snapshot->schema_id_on_snapshot_commit > std::numeric_limits<Int32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg schema_id {} exceeds Int32 range", snapshot->schema_id_on_snapshot_commit);

        state.snapshot = std::move(snapshot);
        state.table_state = std::move(table_state);
    }

    auto compression_method = DB::Iceberg::getCompressionMethodFromMetadataFile(state.table_state.metadata_file_path);
    auto metadata_object = getMetadataJSONObject(
        state.table_state.metadata_file_path,
        object_storage,
        components.metadata_cache,
        context,
        log,
        compression_method,
        components.table_uuid);

    const auto format_version = metadata_object->getValue<Int32>(f_format_version);
    if (format_version != 2)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "DROP PARTITION is supported only for Iceberg format-version 2, but the table has format-version {}",
            format_version);

    auto specs = metadata_object->getArray(f_partition_specs);
    if (!specs || specs->size() == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "No 'partition-specs' or empty in metadata file {}", state.table_state.metadata_file_path);

    state.metadata_object = metadata_object;
    state.schema_id = static_cast<Int32>(state.snapshot->schema_id_on_snapshot_commit);
    state.partition_spec_id = metadata_object->getValue<Int64>(f_default_spec_id);

    /// TODO: support different specs
    if (specs->size() > 1)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "DROP PARTITION is not supported on Iceberg tables with evolved partition specs "
            "({} specs in metadata)",
            specs->size());

    auto partition_spec = specs->getObject(0);
    if (!partition_spec || partition_spec->getValue<Int64>(f_spec_id) == state.partition_spec_id)
        state.partition_spec = partition_spec;

    if (!state.partition_spec)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Default partition spec {} not found in metadata {}",
            state.partition_spec_id,
            state.table_state.metadata_file_path);

    if (!state.partition_spec->has(f_fields))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Default partition spec {} doesn't have '{}' key, metadata {}",
            state.partition_spec_id,
            f_fields,
            state.table_state.metadata_file_path);

    auto partition_fields = state.partition_spec->getArray(f_fields);
    for (size_t i = 0; i < partition_fields->size(); ++i)
    {
        auto field_object = partition_fields->getObject(static_cast<UInt32>(i));
        auto field_name = field_object->getValue<String>(f_name);
        state.partition_columns.emplace_back(std::move(field_name));
    }

    if (state.partition_columns.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DROP PARTITION is not supported on unpartitioned Iceberg tables");

    auto schemas = metadata_object->getArray(f_schemas);
    if (!schemas || schemas->size() == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Iceberg '{}' key not found in metadata {} or empty",
            f_schemas,
            state.table_state.metadata_file_path);

    Poco::JSON::Object::Ptr current_schema;
    for (size_t i = 0; schemas && i < schemas->size(); ++i)
    {
        auto schema = schemas->getObject(static_cast<UInt32>(i));
        if (!schema || schema->getValue<Int32>(f_schema_id) != state.schema_id)
            continue;

        current_schema = schema;
        break;
    }

    if (!current_schema)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Iceberg schema '{}' not found in metadata {} or empty",
            state.schema_id,
            state.table_state.metadata_file_path);

    state.partition_types
        = resolvePartitionTypes(*state.partition_spec, *current_schema, *components.schema_processor, state.schema_id, context);

    if (state.partition_types.size() != state.partition_columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partitions types count doesn't match with number of partition columns");

    return state;
}

AlterDropPartitionExecutor::TargetFilePaths
AlterDropPartitionExecutor::discoverTargetFilePaths(const SnapshotState & state, const Row & target_partition) const
{
    TargetFilePaths targets;
    auto collect = [&](const std::vector<ProcessedManifestFileEntryPtr> & entries)
    {
        for (const auto & entry : entries)
        {
            const auto & parsed = entry->parsed_entry;

            if (!parsed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Manifest file entry is not parsed");

            if (partitionEquals(parsed->partition_key_value, target_partition))
                targets.emplace(components.path_resolver.resolve(parsed->file_path_key));
        }
    };

    for (const auto & manifest_key : state.snapshot->manifest_list_entries)
    {
        auto handle = getManifestFileEntriesHandle(object_storage, components, context, log, manifest_key, state.schema_id);

        collect(handle.getFilesWithoutDeleted(FileContentType::DATA));
        collect(handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE));

        for (const auto & entry : handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE))
        {
            if (!entry->parsed_entry)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Manifest file entry is not parsed");
            if (partitionEquals(entry->parsed_entry->partition_key_value, target_partition))
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED, "DROP PARTITION is not supported when the selected partition has equality deletes");
        }
    }

    return targets;
}

AlterDropPartitionExecutor::DropPlan
AlterDropPartitionExecutor::buildDropPlan(const SnapshotState & state, const TargetFilePaths & targets) const
{
    DropPlan result;
    std::set<Row> changed_partitions;
    auto unprocessed_target_file_paths = targets;

    UInt64 removed_data_files = 0;
    UInt64 removed_records = 0;
    UInt64 removed_files_size = 0;
    UInt64 removed_position_deletes = 0;
    UInt64 removed_position_delete_files = 0;

    auto process_entries
        = [&](const std::vector<ProcessedManifestFileEntryPtr> & entries, size_t & entries_to_keep, size_t & entries_to_remove)
    {
        for (const auto & entry : entries)
        {
            if (!entry->parsed_entry)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Manifest file entry is not parsed");

            const auto & parsed_entry = *entry->parsed_entry;
            const String storage_path = components.path_resolver.resolve(parsed_entry.file_path_key);
            if (!targets.contains(storage_path))
            {
                ++entries_to_keep;
                continue;
            }

            ++entries_to_remove;
            unprocessed_target_file_paths.erase(storage_path);
            switch (parsed_entry.content_type)
            {
                case FileContentType::DATA:
                    ++removed_data_files;
                    removed_records += parsed_entry.record_count;
                    removed_files_size += parsed_entry.file_size_in_bytes;
                    break;
                case FileContentType::POSITION_DELETE:
                    ++removed_position_delete_files;
                    removed_position_deletes += parsed_entry.record_count;
                    removed_files_size += parsed_entry.file_size_in_bytes;
                    break;
                case FileContentType::EQUALITY_DELETE:
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "DROP PARTITION encountered an equality-delete entry, which is not supported");
            }
            changed_partitions.insert(parsed_entry.partition_key_value);
        }
    };

    for (const auto & manifest_key : state.snapshot->manifest_list_entries)
    {
        auto handle = getManifestFileEntriesHandle(object_storage, components, context, log, manifest_key, state.schema_id);

        size_t entries_to_keep = 0;
        size_t entries_to_remove = 0;

        process_entries(handle.getFilesWithoutDeleted(FileContentType::DATA), entries_to_keep, entries_to_remove);
        process_entries(handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE), entries_to_keep, entries_to_remove);

        if (entries_to_remove == 0)
            continue;

        /// Removing a manifest with equality deletes would silently resurrect rows.
        if (!handle.getFilesWithoutDeleted(FileContentType::EQUALITY_DELETE).empty())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "DROP PARTITION is not supported on Iceberg tables where an affected manifest '{}' contains equality deletes",
                manifest_key.manifest_file_path);

        TargetManifest target_manifest{.manifest_key = manifest_key};
        if (entries_to_keep == 0)
            result.target_manifests.fully_matched.emplace_back(std::move(target_manifest));
        else
            result.target_manifests.partially_matched.emplace_back(std::move(target_manifest));
    }

    if (!unprocessed_target_file_paths.empty())
        throw Exception(
            ErrorCodes::CONCURRENT_ACCESS_NOT_SUPPORTED,
            "DROP PARTITION lost a race with a concurrent operation that replaced files in the "
            "target partition; please retry");

    result.snapshot_summary_update = Iceberg::SnapshotSummaryUpdateDelete{
        .deleted_data_files = removed_data_files,
        .removed_records = removed_records,
        .removed_files_size = removed_files_size,
        .removed_position_delete_files = removed_position_delete_files,
        .removed_position_deletes = removed_position_deletes,
        .num_partitions = static_cast<UInt64>(changed_partitions.size())};
    return result;
}

AlterDropPartitionExecutor::ManifestListWriteResult AlterDropPartitionExecutor::writeManifestList(
    SnapshotState & state, const DropPlan & plan, FileNamesGenerator & filename_generator, std::vector<String> & files_for_cleanup)
{
    auto parent_snapshot_id = state.metadata_object->getValue<Int64>(f_current_snapshot_id);
    auto metadata_info = filename_generator.generateMetadataPathWithInfo();

    auto [new_snapshot, manifest_list_path] = MetadataGenerator{state.metadata_object}.generateNextMetadata(
        filename_generator,
        metadata_info.path,
        parent_snapshot_id,
        /*added_files=*/0,
        /*added_records=*/0,
        /*added_files_size=*/0,
        /*num_partitions=*/0,
        /*added_delete_files=*/0,
        /*num_deleted_rows=*/0);

    Poco::JSON::Object::Ptr parent_snapshot;
    auto snapshots = state.metadata_object->getArray(f_snapshots);
    for (size_t i = 0; snapshots && i < snapshots->size(); ++i)
    {
        auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        if (snapshot && snapshot->getValue<Int64>(f_metadata_snapshot_id) == parent_snapshot_id)
        {
            parent_snapshot = snapshot;
            break;
        }
    }

    if (!parent_snapshot || !parent_snapshot->has(f_summary))
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Parent snapshot {} has no summary", parent_snapshot_id);

    auto parent_summary = parent_snapshot->getObject(f_summary);
    auto parsed_summary = SnapshotSummary::fromJSON(*parent_summary, /*with_extra_fields=*/false, /*require_totals=*/true);
    if (!parsed_summary)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Cannot use summary of parent snapshot {} for DROP PARTITION: {}",
            parent_snapshot_id,
            parsed_summary.error());

    new_snapshot->set(f_summary, SnapshotSummary{plan.snapshot_summary_update, parsed_summary->getTotals()}.toJSON());

    const String storage_manifest_list_path = components.path_resolver.resolve(manifest_list_path);
    files_for_cleanup.push_back(storage_manifest_list_path);

    std::unordered_set<String> removed_manifest_paths;
    for (const auto & tm : plan.target_manifests.fully_matched)
        removed_manifest_paths.insert(tm.manifest_key.manifest_file_path.serialize());

    std::unordered_set<String> carry_forward_manifest_paths;
    for (const auto & manifest_key : state.snapshot->manifest_list_entries)
    {
        const auto path = manifest_key.manifest_file_path.serialize();
        if (!removed_manifest_paths.contains(path))
            carry_forward_manifest_paths.insert(path);
    }

    LOG_TRACE(log, "ALTER DROP PARTITION writing new manifest list {}", storage_manifest_list_path);

    auto buf = object_storage->writeObject(
        StoredObject(storage_manifest_list_path),
        WriteMode::Rewrite,
        /*attributes=*/std::nullopt,
        DBMS_DEFAULT_BUFFER_SIZE,
        context->getWriteSettings());

    generateManifestList(
        components.path_resolver,
        state.metadata_object,
        object_storage,
        context,
        /*manifest_entry_names=*/{},
        new_snapshot,
        /*manifest_entry_sizes=*/{},
        *buf,
        FileContentType::DATA,
        /*use_previous_snapshots=*/false,
        /*per_entry_content_types=*/{},
        /*entry_counts=*/{},
        carry_forward_manifest_paths);

    buf->finalize();

    return ManifestListWriteResult{.metadata_info = metadata_info};
}

bool AlterDropPartitionExecutor::commitMetadataJSON(
    SnapshotState & state, FileNamesGenerator & filename_generator, const GeneratedMetadataFileWithInfo & metadata_info)
{
    std::string json_representation = stringifyJSON(state.metadata_object, 4);

    fiu_do_on(FailPoints::iceberg_writes_cleanup, { throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for cleanup enabled"); });

    auto hint_path = filename_generator.generateVersionHint();

    return writeMetadataFileAndVersionHint(
        components.path_resolver,
        metadata_info,
        json_representation,
        hint_path,
        object_storage,
        context,
        data_lake_settings[DataLakeStorageSetting::iceberg_use_version_hint]);
}

void AlterDropPartitionExecutor::cleanupNotCommited(std::vector<std::string> files)
{
    for (const auto & path : files)
    {
        try
        {
            object_storage->removeObjectIfExists(StoredObject(path));
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to clean up partially-written manifest {}", path));
        }
    }
}

bool AlterDropPartitionExecutor::tryCommit(SnapshotState & state, const DropPlan & plan)
{
    /// Match the table's current metadata compression instead of the table-init default: a long-lived
    /// table can move from uncompressed to e.g. `vN.gz.metadata.json` (external writer or changed
    /// setting), and the replacement metadata must follow the same convention.
    const auto compression_method = DB::Iceberg::getCompressionMethodFromMetadataFile(state.table_state.metadata_file_path);
    FileNamesGenerator filename_generator(components.path_resolver.getTableLocation(), false, compression_method, write_format);
    filename_generator.setVersion(state.table_state.metadata_version + 1);
    filename_generator.setCompressionMethod(compression_method);

    std::vector<std::string> files_for_cleanup;
    bool committed = false;

    SCOPE_EXIT({
        if (!committed)
            cleanupNotCommited(std::move(files_for_cleanup));
    });

    auto [metadata_info] = writeManifestList(state, plan, filename_generator, files_for_cleanup);

    committed = commitMetadataJSON(state, filename_generator, metadata_info);
    if (!committed)
        return false;

    LOG_INFO(
        log,
        "DROP PARTITION committed: removed {} data files ({} rows), {} position-delete files",
        plan.snapshot_summary_update.deleted_data_files,
        plan.snapshot_summary_update.removed_records,
        plan.snapshot_summary_update.removed_position_delete_files);

    return true;
}

void AlterDropPartitionExecutor::run()
{
    const auto & partition_ast = command.partition->as<ASTPartition &>();
    validateDropPartitionAST(partition_ast, command);

    TargetFilePaths targets;

    for (int attempt = 0; attempt < MAX_TRANSACTION_RETRIES; ++attempt)
    {
        auto state_opt = fetchSnapshotState();
        if (!state_opt)
        {
            LOG_DEBUG(log, "Table has no snapshot, nothing to drop");
            return;
        }
        SnapshotState & state = *state_opt;

        const auto target_partition = parsePartitionTuple(*partition_ast.value, state.partition_types, context);

        if (attempt == 0)
        {
            targets = discoverTargetFilePaths(state, target_partition);
            if (targets.empty())
            {
                LOG_INFO(log, "No data files match the requested partition; DROP PARTITION is a no-op");
                return;
            }
            FailPointInjection::pauseFailPoint(FailPoints::iceberg_drop_partition_pause_after_discovery);
        }

        auto plan = buildDropPlan(state, targets);
        if (!plan.target_manifests.partially_matched.empty())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "DROP PARTITION would require rewriting a manifest that also contains files from other partitions");

        if (tryCommit(state, plan))
            return;
    }

    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Too many unsuccessful retries to drop partition in Iceberg table");
}

}
}

// NOLINTEND(clang-analyzer-core.uninitialized.UndefReturn)
#endif
