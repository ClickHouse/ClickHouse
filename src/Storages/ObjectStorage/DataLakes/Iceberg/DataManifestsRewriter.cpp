#include <Storages/ObjectStorage/DataLakes/Iceberg/DataManifestsRewriter.h>

#if USE_AVRO

#include <Core/Block.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ChunkPartitioner.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorDump.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <limits>

namespace DB::ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
extern const int LOGICAL_ERROR;
}

namespace DB::Iceberg
{

DataManifestsRewriter::DataManifestsRewriter(
    Poco::JSON::Object::Ptr metadata_object_,
    const PersistentTableComponents & persistent_table_components_,
    ObjectStoragePtr object_storage_,
    ContextPtr context_,
    SharedHeader sample_block_,
    String write_format_,
    Grouping grouping_)
    : metadata_object(std::move(metadata_object_))
    , persistent_table_components(persistent_table_components_)
    , object_storage(std::move(object_storage_))
    , context(std::move(context_))
    , sample_block(std::move(sample_block_))
    , write_format(std::move(write_format_))
    , grouping(grouping_)
    , log(getLogger("IcebergDataManifestsRewriter"))
{
    current_schema_id = metadata_object->getValue<Int32>(f_current_schema_id);
    schemas = metadata_object->getArray(f_schemas);
    partition_specs = metadata_object->getArray(f_partition_specs);
}

void DataManifestsRewriter::collect(const ManifestFileCacheKeys & data_manifests)
{
    if (collected)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The data manifests of an Iceberg manifest rewrite are collected twice");
    collected = true;

    /// The entries of each manifest are resolved under the schema their files were written with, so every schema must be known.
    for (UInt32 i = 0; i < schemas->size(); ++i)
        persistent_table_components.schema_processor->addIcebergTableSchema(schemas->getObject(i));

    ManifestFilesPrefetcher prefetcher(object_storage, persistent_table_components, context, log, data_manifests);
    while (auto fetch = prefetcher.take())
    {
        const ManifestFileCacheKey & manifest = fetch->key();
        if (manifest.content_type != ManifestFileContentType::DATA)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Iceberg manifest {} passed to a data manifest rewrite is not a data manifest",
                manifest.manifest_file_path.serialize());

        auto manifest_file = fetch->get();

        /// A rewrite cannot round-trip per-file `key_metadata` (data-file encryption keys).
        if (!has_encrypted_data_files)
        {
            auto deserializer = manifest_file.getDeserializer();
            if (deserializer->hasPath(c_data_file_key_metadata))
                for (size_t row = 0; row < deserializer->rows(); ++row)
                    if (!deserializer->getValueFromRowByName(row, c_data_file_key_metadata).isNull())
                        has_encrypted_data_files = true;
        }

        auto files_handle = getManifestFileEntriesHandle(manifest_file, persistent_table_components, context, manifest, current_schema_id);

        for (const auto & data_file : files_handle.getFilesWithoutDeleted(FileContentType::DATA))
        {
            const auto & entry = *data_file->parsed_entry;

            /// Files of different specs or schemas are never merged into one manifest: a manifest carries a single spec
            /// and one `schema` header. `FieldVisitorDump`'s type tag prevents UInt64/Int64 collisions.
            String group_key = std::to_string(manifest.partition_spec_id) + "|" + std::to_string(data_file->resolved_schema_id) + "|";
            if (grouping == Grouping::PerPartition)
            {
                FieldVisitorDump dump_visitor;
                for (const auto & value : data_file->normalized_partition_key_value)
                    group_key += applyVisitor(dump_visitor, value) + "|";
            }

            auto & group = groups[group_key];
            group.partition_spec_id = manifest.partition_spec_id;
            group.schema_id = data_file->resolved_schema_id;

            /// A manifest should not list the same data file twice.
            if (!group.file_paths.insert(entry.file_path_key.serialize()).second)
                continue;

            DataFile file;
            file.path = entry.file_path_key;
            file.partition_values = data_file->normalized_partition_key_value;
            if (!std::ranges::all_of(file.partition_values, canWritePartitionValue))
                has_unsupported_partition_values = true;
            file.record_count = entry.record_count;
            file.file_size_in_bytes = entry.file_size_in_bytes;
            file.file_format = entry.file_format;
            file.sort_order_id = entry.sort_order_id;

            /// Resolve the inherited (null) snapshot id and sequence numbers from the manifest, since EXISTING entries require them.
            file.lineage.added_snapshot_id = entry.parsed_snapshot_id;
            if (!file.lineage.added_snapshot_id.has_value())
                file.lineage.added_snapshot_id = manifest.added_snapshot_id;
            file.lineage.sequence_number = entry.parsed_sequence_number;
            if (!file.lineage.sequence_number.has_value())
                file.lineage.sequence_number = manifest.added_sequence_number;
            /// `file_sequence_number` can differ from the data sequence number and, when null, inherits the manifest's sequence number.
            file.lineage.file_sequence_number = entry.parsed_file_sequence_number;
            if (!file.lineage.file_sequence_number.has_value())
                file.lineage.file_sequence_number = manifest.added_sequence_number;

            /// Carry the per-column stats over verbatim, keeping bounds as the raw serialized bytes so they round-trip.
            for (const auto & [field_id, column_info] : entry.columns_infos)
            {
                if (column_info.bytes_size.has_value())
                    file.statistics.column_sizes.emplace_back(field_id, *column_info.bytes_size);
                if (column_info.rows_count.has_value())
                    file.statistics.value_counts.emplace_back(field_id, *column_info.rows_count);
                if (column_info.nulls_count.has_value())
                    file.statistics.null_value_counts.emplace_back(field_id, *column_info.nulls_count);
            }
            for (const auto & [field_id, bounds] : entry.value_bounds)
            {
                if (!bounds.first.isNull())
                    file.statistics.lower_bounds.emplace_back(field_id, bounds.first.safeGet<String>());
                if (!bounds.second.isNull())
                    file.statistics.upper_bounds.emplace_back(field_id, bounds.second.safeGet<String>());
            }

            group.files.push_back(std::move(file));
            ++data_files_count;
        }
    }
}

size_t DataManifestsRewriter::getOutputManifestsCount(size_t max_entries_per_manifest) const
{
    if (grouping == Grouping::PerPartition || max_entries_per_manifest == 0)
        return groups.size();

    size_t result = 0;
    for (const auto & [_, group] : groups)
        result += (group.files.size() + max_entries_per_manifest - 1) / max_entries_per_manifest;
    return result;
}

void DataManifestsRewriter::write(
    FileNamesGenerator & generator, Poco::JSON::Object::Ptr new_snapshot, size_t max_entries_per_manifest, Result & result)
{
    if (has_encrypted_data_files)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg data manifests with encrypted data files cannot be rewritten");

    for (auto & [_, group] : groups)
    {
        if (grouping == Grouping::PerPartition || max_entries_per_manifest == 0)
        {
            writeManifest(generator, new_snapshot, group, group.files, result);
            continue;
        }

        /// Neighbouring partitions end up in the same manifest, which keeps the manifest-list partition bounds narrow.
        std::stable_sort(
            group.files.begin(), group.files.end(), [](const DataFile & lhs, const DataFile & rhs) { return lhs.partition_values < rhs.partition_values; });

        std::span<const DataFile> files = group.files;
        for (size_t begin = 0; begin < files.size(); begin += max_entries_per_manifest)
            writeManifest(generator, new_snapshot, group, files.subspan(begin, std::min(max_entries_per_manifest, files.size() - begin)), result);
    }
}

void DataManifestsRewriter::writeManifest(
    FileNamesGenerator & generator,
    Poco::JSON::Object::Ptr new_snapshot,
    const Group & group,
    std::span<const DataFile> files,
    Result & result)
{
    const auto & path_resolver = persistent_table_components.path_resolver;

    /// Rewrite the manifest under the partition spec and the schema its source files used, not the default and the current ones.
    const auto & resolved_spec = resolvePartitionSpec(group.partition_spec_id, group.schema_id);

    std::vector<IcebergPathFromMetadata> file_paths;
    std::vector<UInt64> file_row_counts;
    std::vector<UInt64> file_byte_counts;
    std::vector<String> file_formats;
    std::vector<DataFileColumnStatistics> file_statistics;
    std::vector<std::optional<Int32>> file_sort_order_ids;
    std::vector<DataFileEntryLineage> file_entry_lineage;
    std::vector<Row> file_partition_values;

    ManifestPartitionSummary partition_summary;
    partition_summary.reserve(resolved_spec.partition_types.size());
    for (const auto & partition_type : resolved_spec.partition_types)
        partition_summary.emplace_back(partition_type);

    ManifestListEntryCounts counts;
    /// Files keep their original sequence numbers, so `min_sequence_number` is the minimum over them.
    counts.min_sequence_number = std::numeric_limits<Int64>::max();
    for (const auto & file : files)
    {
        /// The partition tuple must match the resolved spec; a mismatch (corrupt or inconsistently-evolved metadata)
        /// would otherwise read past the end of the partition values while writing the manifest.
        if (file.partition_values.size() != resolved_spec.partition_columns.size())
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Iceberg manifest partition tuple has {} values but partition spec {} defines {} columns",
                file.partition_values.size(),
                group.partition_spec_id,
                resolved_spec.partition_columns.size());

        file_paths.push_back(file.path);
        file_row_counts.push_back(static_cast<UInt64>(file.record_count));
        file_byte_counts.push_back(static_cast<UInt64>(file.file_size_in_bytes));
        file_formats.push_back(file.file_format);
        file_statistics.push_back(file.statistics);
        file_sort_order_ids.push_back(file.sort_order_id);
        file_entry_lineage.push_back(file.lineage);
        file_partition_values.push_back(file.partition_values);

        for (size_t i = 0; i < partition_summary.size(); ++i)
            partition_summary[i].add(file.partition_values[i]);

        ++counts.files_count;
        counts.rows_count += file.record_count;
        counts.min_sequence_number = std::min(counts.min_sequence_number, file.lineage.sequence_number.value_or(0));
    }

    auto manifest_path = generator.generateManifestEntryName();
    auto storage_manifest_path = path_resolver.resolve(manifest_path);
    LOG_DEBUG(log, "Writing manifest {} with {} data files", storage_manifest_path, files.size());

    /// Track the path before the object is created, so the caller removes it even if a later step throws.
    result.paths.push_back(manifest_path);

    auto buffer = object_storage->writeObject(
        StoredObject(storage_manifest_path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    generateManifestFile(
        metadata_object,
        resolved_spec.partition_columns,
        files.front().partition_values,
        resolved_spec.partition_types,
        file_paths,
        file_row_counts,
        file_byte_counts,
        std::nullopt,
        sample_block,
        new_snapshot,
        write_format,
        resolved_spec.spec,
        group.partition_spec_id,
        *buffer,
        FileContentType::DATA,
        /* user_defined_sequence_number */ std::nullopt,
        /* user_defined_snapshot_id */ std::nullopt,
        file_formats,
        file_statistics,
        file_sort_order_ids,
        file_entry_lineage,
        /* schema_to_serialize */ getSchemaObjectById(group.schema_id),
        /* per_file_fresh_statistics */ nullptr,
        file_partition_values);

    buffer->finalize();
    Int64 manifest_size = buffer->count();
    if (manifest_size == 0)
        manifest_size = object_storage->getObjectMetadata(storage_manifest_path, /*with_tags=*/false).size_bytes;

    result.sizes.push_back(manifest_size);
    result.counts.push_back(counts);
    result.partition_spec_ids.push_back(group.partition_spec_id);
    result.partition_summaries.push_back(std::move(partition_summary));
}

const DataManifestsRewriter::ResolvedPartitionSpec & DataManifestsRewriter::resolvePartitionSpec(Int32 spec_id, Int32 files_schema_id)
{
    if (auto it = resolved_specs.find({spec_id, files_schema_id}); it != resolved_specs.end())
        return it->second;

    Poco::JSON::Object::Ptr spec;
    for (UInt32 i = 0; i < partition_specs->size(); ++i)
    {
        auto candidate = partition_specs->getObject(i);
        if (candidate->getValue<Int64>(f_spec_id) == spec_id)
        {
            spec = candidate;
            break;
        }
    }
    if (!spec)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Iceberg metadata does not contain a partition spec entry matching spec-id {}", spec_id);

    ResolvedPartitionSpec resolved;
    resolved.spec = spec;
    auto spec_fields = spec->getArray(f_fields);

    /// Partition field names and the schema source-ids they transform.
    std::vector<Int32> source_ids;
    for (UInt32 i = 0; i < spec_fields->size(); ++i)
    {
        auto spec_field = spec_fields->getObject(i);
        resolved.partition_columns.push_back(spec_field->getValue<String>(f_name));
        source_ids.push_back(spec_field->getValue<Int32>(f_source_id));
    }

    /// After schema evolution (e.g. widening `decimal(P, S)`) the partition values must be encoded under the schema the
    /// source files were written with. Derive the partition value types from a schema that defines every source column
    /// the spec references, preferring the schema of the source files, then the current one, then any historical one.
    auto build_sample_block = [&](Int32 schema_id) -> std::optional<Block>
    {
        auto fields_characteristics = persistent_table_components.schema_processor->tryGetFieldsCharacteristics(schema_id, source_ids);
        /// A short result means this schema does not define every partition source column.
        if (fields_characteristics.size() != source_ids.size())
            return std::nullopt;
        Block block;
        for (const auto & name_and_type : fields_characteristics)
            block.insert(ColumnWithTypeAndName(name_and_type.type, name_and_type.name));
        return block;
    };

    Int32 schema_id_for_spec = files_schema_id;
    std::optional<Block> spec_sample_block = build_sample_block(schema_id_for_spec);
    if (!spec_sample_block && files_schema_id != current_schema_id)
    {
        schema_id_for_spec = current_schema_id;
        spec_sample_block = build_sample_block(schema_id_for_spec);
    }
    if (!spec_sample_block)
    {
        for (UInt32 i = 0; i < schemas->size(); ++i)
        {
            Int32 candidate_id = schemas->getObject(i)->getValue<Int32>(f_schema_id);
            if (candidate_id == files_schema_id || candidate_id == current_schema_id)
                continue;
            spec_sample_block = build_sample_block(candidate_id);
            if (spec_sample_block)
            {
                schema_id_for_spec = candidate_id;
                break;
            }
        }
    }
    if (!spec_sample_block)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "No Iceberg schema defines all source columns referenced by partition spec {}", spec_id);

    auto schema_for_spec = persistent_table_components.schema_processor->getIcebergTableSchemaById(schema_id_for_spec);
    auto shared_sample_block = std::make_shared<const Block>(std::move(*spec_sample_block));
    resolved.partition_types = ChunkPartitioner(spec_fields, schema_for_spec->getArray(f_fields), context, shared_sample_block).getResultTypes();

    return resolved_specs.emplace(std::make_pair(spec_id, files_schema_id), std::move(resolved)).first->second;
}

Poco::JSON::Object::Ptr DataManifestsRewriter::getSchemaObjectById(Int32 schema_id) const
{
    /// Used verbatim as the Avro `schema` header of a rewritten manifest, so its data-file bounds resolve under the same
    /// schema the files were written with.
    for (UInt32 i = 0; i < schemas->size(); ++i)
        if (schemas->getObject(i)->getValue<Int32>(f_schema_id) == schema_id)
            return schemas->getObject(i);
    throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Iceberg metadata does not contain a schema entry matching schema-id {}", schema_id);
}

}

#endif
