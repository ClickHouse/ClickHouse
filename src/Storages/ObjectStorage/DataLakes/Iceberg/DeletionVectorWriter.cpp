#include <Storages/ObjectStorage/DataLakes/Iceberg/DeletionVectorWriter.h>

#if USE_AVRO

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Processors/Formats/Impl/AvroRowOutputFormat.h>
#include <Processors/Formats/Impl/Puffin/PuffinDeletionVector.h>
#include <Processors/Formats/Impl/Puffin/PuffinWriter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <DataFile.hh>
#include <Generic.hh>
#include <GenericDatum.hh>

#include <Poco/String.h>

namespace DB::ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace DB::Iceberg
{

namespace
{

Int64 getWrittenSize(WriteBuffer & buf, const IcebergPathFromMetadata & path, const IcebergPathResolver & path_resolver, const ObjectStoragePtr & object_storage)
{
    auto size = static_cast<Int64>(buf.count());
    if (size == 0)
        size = static_cast<Int64>(object_storage->getObjectMetadata(path_resolver.resolve(path), /*with_tags=*/ false).size_bytes);
    return size;
}

Int64 writeNewObject(
    const IcebergPathFromMetadata & path,
    const IcebergPathResolver & path_resolver,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context,
    const std::function<void(WriteBuffer &)> & write)
{
    const auto storage_path = path_resolver.resolve(path);
    auto buf = object_storage->writeObject(
        StoredObject(storage_path), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());
    try
    {
        write(*buf);
        buf->finalize();
        return getWrittenSize(*buf, path, path_resolver, object_storage);
    }
    catch (...)
    {
        buf->cancel();
        try
        {
            object_storage->removeObjectIfExists(StoredObject(storage_path));
        }
        catch (...)
        {
            tryLogCurrentException("IcebergDeletionVectorWriter", "Cannot remove a file of a failed write");
        }
        throw;
    }
}

std::optional<Int64> getOptionalLong(const avro::GenericRecord & record, const String & name, const String & manifest_path)
{
    if (!record.hasField(name))
        return std::nullopt;
    const auto & datum = record.field(name);
    if (datum.type() == avro::AVRO_NULL)
        return std::nullopt;
    if (datum.type() != avro::AVRO_LONG)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Field '{}' of manifest {} has Avro type {}, but a long is required",
            name,
            manifest_path,
            avro::toString(datum.type()));
    return datum.value<int64_t>();
}

std::optional<String> getOptionalString(const avro::GenericRecord & record, const String & name, const String & manifest_path)
{
    if (!record.hasField(name))
        return std::nullopt;
    const auto & datum = record.field(name);
    if (datum.type() == avro::AVRO_NULL)
        return std::nullopt;
    if (datum.type() != avro::AVRO_STRING)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Field '{}' of manifest {} has Avro type {}, but a string is required",
            name,
            manifest_path,
            avro::toString(datum.type()));
    return datum.value<std::string>();
}

template <typename T>
T getRequired(const avro::GenericRecord & record, const String & name, avro::Type type, const String & manifest_path)
{
    if (!record.hasField(name) || record.field(name).type() != type)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Manifest {} has no field '{}' of Avro type {}",
            manifest_path,
            name,
            avro::toString(type));
    return record.field(name).value<T>();
}

void setLong(avro::GenericRecord & record, const String & name, Int64 value, const String & manifest_path)
{
    if (!record.hasField(name))
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Manifest {} has no field '{}'", manifest_path, name);

    auto & datum = record.field(name);
    if (datum.isUnion())
    {
        const auto & union_schema = record.schema()->leafAt(record.fieldIndex(name));
        bool found = false;
        for (size_t branch = 0; branch < union_schema->leaves(); ++branch)
        {
            if (union_schema->leafAt(branch)->type() == avro::AVRO_LONG)
            {
                datum.selectBranch(branch);
                found = true;
                break;
            }
        }
        if (!found)
            throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Field '{}' of manifest {} cannot hold a long", name, manifest_path);
    }
    else if (datum.type() != avro::AVRO_LONG)
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Field '{}' of manifest {} cannot hold a long", name, manifest_path);

    datum.value<int64_t>() = value;
}

}

WrittenDeletionVectorsFile writeDeletionVectorsFile(
    std::map<String, roaring::Roaring64Map> & positions_by_data_file,
    FileNamesGenerator & filename_generator,
    const IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    ContextPtr context)
{
    WrittenDeletionVectorsFile result;
    result.path = filename_generator.generateDeletionVectorFile();
    result.size_in_bytes = writeNewObject(result.path, path_resolver, object_storage, context, [&](WriteBuffer & buf)
    {
        PuffinWriter puffin(buf);
        for (auto & [data_file, positions] : positions_by_data_file)
        {
            const String bytes = serializeDeletionVectorBlob(positions);
            const UInt64 cardinality = positions.cardinality();
            const auto blob = puffin.addBlob(makeDeletionVectorBlobMetadata(data_file, cardinality), bytes);
            result.vectors.emplace(
                data_file,
                WrittenDeletionVector{
                    .location = DeletionVectorLocation{
                        .referenced_data_file = IcebergPathFromMetadata::deserialize(data_file),
                        .content_offset = blob.offset,
                        .content_size_in_bytes = blob.length},
                    .cardinality = cardinality});
        }
        puffin.finalize();
    });
    return result;
}

RewrittenDeleteManifest rewriteDeleteManifest(
    const ManifestFileCacheKey & source,
    const std::unordered_set<String> & data_files_with_replaced_vectors,
    Int64 new_snapshot_id,
    FileNamesGenerator & filename_generator,
    const IcebergPathResolver & path_resolver,
    ObjectStoragePtr object_storage,
    ContextPtr context)
{
    if (source.content_type != ManifestFileContentType::DELETE)
        throw Exception(
            ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
            "Manifest {} holds a deletion vector, but it is not a delete manifest",
            source.manifest_file_path.serialize());

    const String source_path = source.manifest_file_path.serialize();
    RelativePathWithMetadata source_object(path_resolver.resolve(source.manifest_file_path));
    auto read_buf = createReadBuffer(source_object, object_storage, context, getLogger("IcebergDeletionVectorWriter"));
    auto reader_base = std::make_unique<avro::DataFileReaderBase>(
        std::make_unique<AvroInputStreamReadBufferAdapter>(*read_buf), MAX_AVRO_SCHEMA_DEPTH);
    const auto source_metadata = reader_base->metadata();
    avro::DataFileReader<avro::GenericDatum> reader(std::move(reader_base));
    const avro::ValidSchema schema = reader.dataSchema();
    if (schema.root()->type() != avro::AVRO_RECORD)
        throw Exception(ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION, "Entries of manifest {} are not Avro records", source_path);

    RewrittenDeleteManifest result;
    auto & list_entry = result.manifest_list_entry;
    list_entry.path = filename_generator.generateManifestEntryName();
    list_entry.partition_spec_id = source.partition_spec_id;
    if (!source.partition_summaries.empty())
        list_entry.serialized_partition_summary = source.partition_summaries;

    list_entry.length = writeNewObject(list_entry.path, path_resolver, object_storage, context, [&](WriteBuffer & buf)
    {
        avro::DataFileWriter<avro::GenericDatum> writer(std::make_unique<OutputStreamWriteBufferAdapter>(buf), schema);
        for (const auto & [key, value] : source_metadata)
            if (key == f_avro_schema || !key.starts_with("avro."))
                writer.setMetadata(key, String(value.begin(), value.end()));

        avro::GenericDatum datum(schema);
        while (reader.read(datum))
        {
            auto & entry = datum.value<avro::GenericRecord>();
            const auto status = static_cast<ManifestEntryStatus>(getRequired<int32_t>(entry, f_status, avro::AVRO_INT, source_path));
            if (status == ManifestEntryStatus::DELETED)
                continue;

            if (!getOptionalLong(entry, f_snapshot_id, source_path))
                setLong(entry, f_snapshot_id, source.added_snapshot_id, source_path);
            const auto sequence_number = getOptionalLong(entry, f_sequence_number, source_path).value_or(source.added_sequence_number);
            setLong(entry, f_sequence_number, sequence_number, source_path);
            if (!getOptionalLong(entry, f_file_sequence_number, source_path))
                setLong(entry, f_file_sequence_number, source.added_sequence_number, source_path);

            auto & data_file = getRequired<avro::GenericRecord>(entry, f_data_file, avro::AVRO_RECORD, source_path);
            const Int64 record_count = getRequired<int64_t>(data_file, f_record_count, avro::AVRO_LONG, source_path);
            const auto referenced_data_file = getOptionalString(data_file, f_referenced_data_file, source_path);
            const bool is_replaced = getRequired<int32_t>(data_file, f_content, avro::AVRO_INT, source_path) == static_cast<Int32>(FileContentType::POSITION_DELETE)
                && Poco::toLower(getRequired<std::string>(data_file, f_file_format, avro::AVRO_STRING, source_path)) == "puffin"
                && referenced_data_file && data_files_with_replaced_vectors.contains(*referenced_data_file);

            if (is_replaced)
            {
                entry.field(f_status) = static_cast<int32_t>(ManifestEntryStatus::DELETED);
                setLong(entry, f_snapshot_id, new_snapshot_id, source_path);
                ++list_entry.deleted_files_count;
                list_entry.deleted_rows_count += record_count;
                ++result.removed_files;
                result.removed_rows += record_count;
            }
            else
            {
                entry.field(f_status) = static_cast<int32_t>(ManifestEntryStatus::EXISTING);
                ++list_entry.existing_files_count;
                list_entry.existing_rows_count += record_count;
                if (!list_entry.min_sequence_number || sequence_number < *list_entry.min_sequence_number)
                    list_entry.min_sequence_number = sequence_number;
            }
            writer.write(datum);
        }
        writer.close();
    });

    return result;
}

}

#endif
