#include <string>
#include <Processors/Formats/Impl/AvroRowOutputFormat.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AlterDropPartitionExecutor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroSchema.h>
#include <base/scope_guard.h>
#include <DataFile.hh>
#include <GenericDatum.hh>
#include <Types.hh>
#include <Poco/JSON/Array.h>

#if USE_AVRO

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
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/MetadataGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/logger_useful.h>

#include <Poco/JSON/Stringifier.h>

#include <limits>
#include <set>
#include <sstream>
#include <memory>

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
Row parsePartitionTuple(const IAST & value_ast, const std::vector<DataTypePtr> & partition_types, ContextPtr context)
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
        throw Exception(
            ErrorCodes::INVALID_PARTITION_VALUE,
            "Expected literal or tuple for partition key, got {}",
            value_ast.getID());

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

std::vector<DataTypePtr> resolvePartitionTypes(
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
    auto schema_fields        = current_schema.getArray(f_fields);

    if (!schema_fields || schema_fields->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Could not find key '{}' in schema {} or fields is empty", f_fields, schema_id);

    ChunkPartitioner partitioner(partition_fields, schema_fields, context, sample_block);
    return partitioner.getResultTypes();
}

}

AlterDropPartitionExecutor::AlterDropPartitionExecutor(
    const PartitionCommand & command_,
    ContextPtr context_,
    ObjectStoragePtr object_storage_,
    const PersistentTableComponents & components_,
    const DataLakeStorageSettings & data_lake_settings_,
    String write_format_,
    LoggerPtr log_,
    std::function<std::pair<IcebergDataSnapshotPtr, TableStateSnapshot>()> fetch_latest_state_)
    : command(command_)
    , context(context_)
    , object_storage(std::move(object_storage_))
    , components(components_)
    , data_lake_settings(data_lake_settings_)
    , write_format(std::move(write_format_))
    , log(std::move(log_))
    , fetch_latest_state(std::move(fetch_latest_state_))
{
}

std::optional<AlterDropPartitionExecutor::SnapshotState> AlterDropPartitionExecutor::fetchSnapshotState()
{
    auto [snapshot, table_state] = fetch_latest_state();
    if (!snapshot)
        return std::nullopt;

    /// FIXME: in all other places schema_id is int32
    if (snapshot->schema_id_on_snapshot_commit > std::numeric_limits<Int32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg schema_id {} exceeds Int32 range", snapshot->schema_id_on_snapshot_commit);

    auto metadata_object = getMetadataJSONObject(
        table_state.metadata_file_path,
        object_storage,
        components.metadata_cache,
        context,
        log,
        components.metadata_compression_method,
        components.table_uuid);

    if (metadata_object->getValue<Int32>(f_format_version) < 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DROP PARTITION is supported only for Iceberg format-version 2");

    SnapshotState state;
    state.snapshot = std::move(snapshot);
    state.table_state = std::move(table_state);
    state.metadata_object = metadata_object;
    state.schema_id = static_cast<Int32>(state.snapshot->schema_id_on_snapshot_commit);
    state.partition_spec_id = metadata_object->getValue<Int64>(f_default_spec_id);

    auto specs = metadata_object->getArray(f_partition_specs);
    if (!specs || specs->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No 'partition-specs' or empty in metadata file {}", table_state.metadata_file_path);

    /// TODO: support different specs
    /// Conservative guard against Iceberg partition-spec evolution. When a
    /// table has carried more than one partition spec over its lifetime, the
    /// snapshot may reference manifests written under older specs whose
    /// partition tuples have a different arity / transform set than the
    /// current default. Properly handling that requires per-manifest spec
    /// resolution during both discovery (so partition predicates can be
    /// applied under the right spec) and writeback (so each replacement
    /// manifest is stamped with its original spec_id). Until that work
    /// lands, refuse rather than silently produce manifests that
    /// double-encode entries against the wrong schema.
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
            table_state.metadata_file_path);

    if (!state.partition_spec->has(f_fields))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Default partition spec {} doesn't have '{}' key, metadata {}",
            state.partition_spec_id,
            f_fields,
            table_state.metadata_file_path);

    auto partition_fields = state.partition_spec->getArray(f_fields);
    for (size_t i = 0; i < partition_fields->size(); ++i)
    {
        auto field_object = partition_fields->getObject(static_cast<UInt32>(i));
        auto field_name   = field_object->getValue<String>(f_name);
        state.partition_columns.emplace_back(std::move(field_name));
    }

    if (state.partition_columns.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DROP PARTITION is not supported on unpartitioned Iceberg tables");

    auto schemas = metadata_object->getArray(f_schemas);
    if (!schemas || schemas->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg '{}' key not found in metadata {} or empty", f_schemas, table_state.metadata_file_path);

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
            table_state.metadata_file_path);

    state.partition_types
        = resolvePartitionTypes(*state.partition_spec, *current_schema, *components.schema_processor, state.schema_id, context);

    if (state.partition_types.size() != state.partition_columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Partitions types count doesn't match with number of partition columns");

    return state;
}

AlterDropPartitionExecutor::TargetFilePaths
AlterDropPartitionExecutor::discoverTargetFilePaths(const SnapshotState & state, const Row & target_partition) const
{
    auto collect = [&](const std::vector<ProcessedManifestFileEntryPtr> & entries, std::unordered_set<String> & sink)
    {
        for (const auto & entry : entries)
        {
            const auto & parsed = entry->parsed_entry;

            if (!parsed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Manifest file entry is not parsed");

            if (partitionEquals(parsed->partition_key_value, target_partition))
                sink.emplace(components.path_resolver.resolve(parsed->file_path_key));
        }
    };

    TargetFilePaths targets;
    for (const auto & manifest_key : state.snapshot->manifest_list_entries)
    {
        auto handle = getManifestFileEntriesHandle(object_storage, components, context, log, manifest_key, state.schema_id);

        collect(handle.getFilesWithoutDeleted(FileContentType::DATA), targets.data);
        collect(handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE), targets.position_delete);
    }

    return targets;
}

AlterDropPartitionExecutor::TargetManifests
AlterDropPartitionExecutor::findTargetManifests(const SnapshotState & state, const TargetFilePaths & targets) const
{
    auto match_entries = [&](const auto & entries, const std::unordered_set<String> & target_paths, TargetManifest & out)
    {
        for (const auto & entry : entries)
        {
            const auto & parsed = entry->parsed_entry;

            if (!parsed)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Manifest file entry is not parsed");

            const String storage_path = components.path_resolver.resolve(parsed->file_path_key);
            if (target_paths.contains(storage_path))
                out.entries_to_remove.emplace_back(entry);
            else
                out.entries_to_keep.emplace_back(entry);
        }
    };

    TargetManifests result;

    for (const auto & manifest_key : state.snapshot->manifest_list_entries)
    {
        TargetManifest target_manifest;
        target_manifest.manifest_path = manifest_key.manifest_file_path;
        target_manifest.manifest_content_type = manifest_key.content_type;

        auto handle = getManifestFileEntriesHandle(object_storage, components, context, log, manifest_key, state.schema_id);
        match_entries(handle.getFilesWithoutDeleted(FileContentType::DATA), targets.data, target_manifest);
        match_entries(handle.getFilesWithoutDeleted(FileContentType::POSITION_DELETE), targets.position_delete, target_manifest);

        if (target_manifest.entries_to_remove.empty())
            continue;

        if (target_manifest.entries_to_keep.empty())
            result.fully_matched.emplace_back(std::move(target_manifest));
        else
            result.partially_matched.emplace_back(std::move(target_manifest));
    }

    return result;
}

AlterDropPartitionExecutor::DropPlan::DropPlan(TargetManifests && target_manifests_)
    : target_manifests(std::move(target_manifests_))
{
    std::set<Row> changed_partitions;

    Int64 removed_data_files = 0;
    Int64 removed_records = 0;
    Int64 removed_files_size = 0;
    Int64 removed_position_deletes = 0;
    Int64 removed_position_delete_files = 0;

    auto update_statistic = [&](const std::vector<ProcessedManifestFileEntryPtr> & entries)
    {
        for (const auto & entry : entries)
        {
            if (!entry->parsed_entry)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Manifest file entry is not parsed");
            const auto & parsed_entry = *entry->parsed_entry;
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
                    /// Discovery never matches equality-delete entries, so we
                    /// should never see one here. Treat as a hard error rather
                    /// than silently miscount.
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "DROP PARTITION encountered an equality-delete entry, which is not supported");
            }
            changed_partitions.insert(parsed_entry.partition_key_value);
        }
    };

    for (const auto & tm : target_manifests.fully_matched)
        update_statistic(tm.entries_to_remove);
    for (const auto & tm : target_manifests.partially_matched)
        update_statistic(tm.entries_to_remove);

    snapshot_summary_update = Iceberg::SnapshotSummaryUpdateDelete{
        .deleted_data_files = removed_data_files,
        .removed_records = removed_records,
        .removed_files_size = removed_files_size,
        .removed_position_delete_files = removed_position_delete_files,
        .removed_position_deletes = removed_position_deletes,
        .num_partitions = static_cast<Int64>(changed_partitions.size())};
}

std::vector<AlterDropPartitionExecutor::ReplacementManifestWrite> AlterDropPartitionExecutor::writeReplacementManifests(
    const SnapshotState & state, const DropPlan & plan, FileNamesGenerator & filename_generator, std::vector<String> & files_for_cleanup)
{
    std::vector<ReplacementManifestWrite> result;
    result.reserve(plan.target_manifests.partially_matched.size());

    for (const auto & target_manifest : plan.target_manifests.partially_matched)
    {
        FileContentType replacement_content_type = target_manifest.entries_to_keep.front()->parsed_entry->content_type;
        for (const auto & s : target_manifest.entries_to_keep)
        {
            if (s->parsed_entry->content_type != replacement_content_type)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Manifest {} mixes content types; rewriting it is not supported",
                    target_manifest.manifest_path.serialize());
        }

        auto new_manifest_path = filename_generator.generateManifestEntryName();
        const String new_storage_path = components.path_resolver.resolve(new_manifest_path);
        files_for_cleanup.push_back(new_storage_path);

        auto buf = object_storage->writeObject(
            StoredObject(new_storage_path),
            WriteMode::Rewrite,
            /*attributes=*/std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        generateExistingManifestFile(
            state.metadata_object,
            state.partition_spec,
            state.partition_spec_id,
            state.partition_columns,
            state.partition_types,
            target_manifest.entries_to_keep,
            *buf);
        buf->finalize();

        Int64 length = buf->count();
        if (length == 0)
            length = object_storage->getObjectMetadata(new_storage_path, /*with_tags=*/false).size_bytes;

        Int64 min_entry_seq = std::numeric_limits<Int64>::max();
        Int64 row_total = 0;
        for (const auto & s : target_manifest.entries_to_keep)
        {
            row_total += s->parsed_entry->record_count;
            Int64 seq = s->parsed_entry->parsed_sequence_number.value_or(s->sequence_number);
            min_entry_seq = std::min(min_entry_seq, seq);
        }
        if (min_entry_seq == std::numeric_limits<Int64>::max())
            min_entry_seq = 0;

        ReplacementManifestWrite write;
        write.path = std::move(new_manifest_path);
        write.length = length;
        write.min_sequence_number = min_entry_seq;
        write.existing_rows_count = static_cast<Int32>(row_total);
        write.existing_files_count = static_cast<Int32>(target_manifest.entries_to_keep.size());
        write.content_type = replacement_content_type;
        result.push_back(std::move(write));
    }
    return result;
}

namespace
{
    /// Hide some boilerplate of working with poco's json objects
    struct MetadataJsonView
    {
        Poco::JSON::Object::Ptr metadata;

        Poco::JSON::Object::Ptr findSnapshot(Int64 id)
        {
            auto snapshots = metadata->getArray(f_snapshots);
            if (!snapshots)
                return {};
            for (auto i = 0ull; i < snapshots->size(); ++i)
            {
                auto snapshot = snapshots->getObject(static_cast<uint32_t>(i));
                if (snapshot && snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id) == id)
                    return snapshot;
            }
            return {};
        }
    };

    struct SnapshotJsonView
    {
        Poco::JSON::Object::Ptr snapshot;
        const Iceberg::IcebergPathResolver & path_resolver;

        auto getManifestPathResolved() const
        {
            auto parent_manifest_list = Iceberg::IcebergPathFromMetadata::deserialize(snapshot->getValue<String>(Iceberg::f_manifest_list));
            auto resolved_path = path_resolver.resolve(parent_manifest_list);
            return resolved_path;
        }
    };

    template <typename T>
    const T & checkAndGetValue(const avro::GenericDatum & datum)
    {
        if constexpr (std::is_same_v<T, avro::GenericRecord>)
        {
            if (datum.type() != avro::AVRO_RECORD)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Unexpected avro's type '{}' (instead of '{}')", datum.type(), avro::AVRO_RECORD);

            return datum.value<T>();
        }
        else if constexpr (std::is_same_v<T, std::string>)
        {
            if (datum.type() != avro::AVRO_STRING)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Unexpected avro's type '{}' (instead of '{}')", datum.type(), avro::AVRO_STRING);

            return datum.value<T>();
        }
        else
        {
            static_assert(false, "Unimplemented");
        }
    };
}

AlterDropPartitionExecutor::ManifestListWriteResult AlterDropPartitionExecutor::writeManifestList(
    SnapshotState & state,
    const DropPlan & plan,
    const std::vector<ReplacementManifestWrite> & replacements,
    FileNamesGenerator & filename_generator,
    std::vector<String> & files_for_cleanup)
{
    auto parent_snapshot_id = state.metadata_object->getValue<Int64>(f_current_snapshot_id);
    auto metadata_info      = filename_generator.generateMetadataPathWithInfo();

    auto [new_snapshot, manifest_list_path] = MetadataGenerator{state.metadata_object}.generateNextMetadata(
        filename_generator,
        metadata_info.path,
        parent_snapshot_id,
        plan.snapshot_summary_update);

    const String storage_manifest_list_path = components.path_resolver.resolve(manifest_list_path);
    files_for_cleanup.push_back(storage_manifest_list_path);

    std::unordered_set<String> skip_manifest_paths;
    for (const auto & tm : plan.target_manifests.fully_matched)
        skip_manifest_paths.insert(tm.manifest_path.serialize());
    for (const auto & tm : plan.target_manifests.partially_matched)
        skip_manifest_paths.insert(tm.manifest_path.serialize());

    {
        auto buf = object_storage->writeObject(
            StoredObject(storage_manifest_list_path),
            WriteMode::Rewrite,
            /*attributes=*/ std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            context->getWriteSettings());

        auto schema = avro::compileJsonSchemaFromString(manifest_list_v2_schema); // NOLINT

        auto writer = [&]() mutable -> avro::DataFileWriter<avro::GenericDatum> {
            auto adapter = std::make_unique<OutputStreamWriteBufferAdapter>(buf);
            return avro::DataFileWriter<avro::GenericDatum>(std::move(adapter), schema);
        }();

        for (const auto & r : replacements)
        {
            avro::GenericDatum entry_datum(schema.root());
            avro::GenericRecord & entry = entry_datum.value<avro::GenericRecord>();

            entry.field(Iceberg::f_manifest_path) = r.path.serialize();
            entry.field(Iceberg::f_manifest_length) = r.length;
            entry.field(Iceberg::f_partition_spec_id) = state.partition_spec_id;
            entry.field(Iceberg::f_content) = static_cast<Int32>(r.content_type);
            entry.field(Iceberg::f_sequence_number) = new_snapshot->getValue<Int64>(Iceberg::f_metadata_sequence_number);
            entry.field(Iceberg::f_min_sequence_number) = r.min_sequence_number;
            entry.field(Iceberg::f_added_snapshot_id) = new_snapshot->getValue<Int64>(Iceberg::f_metadata_snapshot_id);
            entry.field(Iceberg::f_added_files_count) = 0;
            entry.field(Iceberg::f_existing_files_count) = r.existing_files_count;
            entry.field(Iceberg::f_deleted_files_count) = 0; // TODO: check if we should fill it in
            entry.field(Iceberg::f_added_rows_count) = 0;
            entry.field(Iceberg::f_existing_rows_count) = r.existing_rows_count;
            entry.field(Iceberg::f_deleted_rows_count) = 0;

            writer.write(entry_datum);
        }

        auto parent_snapshot = MetadataJsonView{.metadata = state.metadata_object}.findSnapshot(parent_snapshot_id);
        auto parent_manifest_list_path = SnapshotJsonView{parent_snapshot, components.path_resolver}.getManifestPathResolved();

        forEachAvroEntry(
            parent_manifest_list_path,
            object_storage,
            context,
            "IcebergWrites",
            [&](const avro::GenericDatum & datum)
            {
                const auto & old_entry         = checkAndGetValue<avro::GenericRecord>(datum);
                const auto & old_manifest_path = old_entry.field(Iceberg::f_manifest_path);
                const auto & manifest_path     = checkAndGetValue<std::string>(old_manifest_path);

                if (!skip_manifest_paths.contains(manifest_path))
                    writer.write(datum);
            });
    }

    return ManifestListWriteResult{new_snapshot, metadata_info};
}

bool AlterDropPartitionExecutor::commitMetadataJSON(
    SnapshotState & state, FileNamesGenerator & filename_generator, const GeneratedMetadataFileWithInfo & metadata_info)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(state.metadata_object, oss, 4);
    std::string json_representation = removeEscapedSlashes(oss.str());

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

bool AlterDropPartitionExecutor::tryCommit(SnapshotState & state, DropPlan plan)
{
    FileNamesGenerator filename_generator(
        components.path_resolver.getTableLocation(), false, components.metadata_compression_method, write_format);
    filename_generator.setVersion(state.table_state.metadata_version + 1);
    filename_generator.setCompressionMethod(components.metadata_compression_method);

    std::vector<String> files_for_cleanup;
    bool committed = false;

    SCOPE_EXIT({
        if (!committed)
        {
            for (const auto & path : files_for_cleanup)
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
    });

    auto replacements = writeReplacementManifests(state, plan, filename_generator, files_for_cleanup);
    auto list_result  = writeManifestList(state, plan, replacements, filename_generator, files_for_cleanup);

    /// TODO: can be optimized, instead of failing and repeat whole operation, recreate all files
    /// we can just update summary and keep untouched in manifest list
    /// e.g. if thre previous operation was an APPEND
    committed = commitMetadataJSON(state, filename_generator, list_result.metadata_info);
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

        DropPlan plan{findTargetManifests(state, targets)};

        if (tryCommit(state, std::move(plan)))
        {
            LOG_INFO(log, "No data files match the requested partition; DROP PARTITION is a no-op");
            return;
        }
    }
}

}
}

#endif
