#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroSchema.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include "Common/Exception.h"
#include "Common/randomSeed.h"
#include <Common/isValidUTF8.h>

#include <Compiler.hh>
#include <Generic.hh>
#include <Encoder.hh>
#include <GenericDatum.hh>
#include <ValidSchema.hh>
#include <Specific.hh>
#include <Poco/JSON/Object.h>
#include <sstream>

#if USE_AVRO

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 output_format_compression_zstd_window_log;
}

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
    extern const int BAD_ARGUMENTS;
}

FileNamesGenerator::FileNamesGenerator(const String & data_dir_, const String & metadata_dir_)
    : data_dir(data_dir_)
    , metadata_dir(metadata_dir_)
{
}

String FileNamesGenerator::generateDataFileName()
{
    return fmt::format("{}/data-{}.parquet", data_dir, uuid_generator.createRandom().toString());
}

String FileNamesGenerator::generateManifestEntryName()
{
    return fmt::format("{}/manifest-entry-{}.avro", metadata_dir, uuid_generator.createRandom().toString());
}

String FileNamesGenerator::generateManifestListName()
{
    return fmt::format("{}/manifest-list-{}.avro", metadata_dir, uuid_generator.createRandom().toString());
}

String FileNamesGenerator::generateMetadataName()
{
    return fmt::format("{}/{}.metadata.json", metadata_dir, uuid_generator.createRandom().toString());
}

ManifestFileGenerator::ManifestFileGenerator(Poco::JSON::Object::Ptr metadata)
{
    Int32 version = metadata->getValue<Int32>(f_format_version);
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_entry_v1_schema;
    else if (version == 2)
        schema_representation = manifest_entry_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown iceberg version {}", version);
    std::istringstream iss(schema_representation);
    avro::compileJsonSchema(iss, schema);
}

String ManifestFileGenerator::generateManifestFile(
    const String & data_file_name,
    Poco::JSON::Object::Ptr new_snapshot)
{
    avro::GenericDatum entry_datum(schema.root());
    avro::GenericRecord& entry = entry_datum.value<avro::GenericRecord>();

    entry.field("status") = 1;

    entry.field("snapshot_id") = new_snapshot->getValue<Int32>(MetadataGenerator::f_snapshot_id);
    entry.field("sequence_number") = new_snapshot->getValue<Int32>(MetadataGenerator::f_sequence_number);
    entry.field("file_sequence_number") = 0; // TODO: maybe fix this

    avro::GenericDatum data_file_datum(entry.field("data_file").value<avro::GenericRecord>().schema());
    avro::GenericRecord & data_file = data_file_datum.value<avro::GenericRecord>();

    data_file.field("content") = 0;
    data_file.field("file_path") = data_file_name;
    data_file.field("file_format") = String("PARQUET");

    avro::GenericDatum part_datum(data_file.field("partition").value<avro::GenericRecord>().schema());
    data_file.field("partition") = part_datum;

    data_file.field("record_count") = new_snapshot->getObject(MetadataGenerator::f_summary)->getValue<Int32>(MetadataGenerator::f_added_records);
    data_file.field("file_size_in_bytes") = new_snapshot->getObject(MetadataGenerator::f_summary)->getValue<Int32>(MetadataGenerator::f_added_files_size);

    entry.field("data_file") = data_file_datum;

    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, entry_datum);
    encoder->flush();

    std::ostringstream oss;
    auto in = avro::memoryInputStream(*out);
    const uint8_t* data;
    size_t len;
    while (in->next(&data, &len)) {
        oss.write(reinterpret_cast<const char*>(data), len);
    }
    std::string bytes = oss.str();
    return bytes;
}

ManifestListGenerator::ManifestListGenerator(Poco::JSON::Object::Ptr metadata_)
    : metadata(metadata_)
{
    Int32 version = metadata->getValue<Int32>(ManifestFileGenerator::f_format_version);
    String schema_representation;
    if (version == 1)
        schema_representation = manifest_list_v1_schema;
    else if (version == 2)
        schema_representation = manifest_list_v2_schema;
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown iceberg version {}", version);
    std::istringstream iss(schema_representation);
    avro::compileJsonSchema(iss, schema);
}

String ManifestListGenerator::generateManifestList(
    const String & manifest_entry_name,
    Poco::JSON::Object::Ptr new_snapshot,
    Int32 manifest_length)
{
    avro::GenericDatum entry_datum(schema.root());
    avro::GenericRecord& entry = entry_datum.value<avro::GenericRecord>();

    entry.field(f_manifest_path) = manifest_entry_name;
    entry.field(f_manifest_length) = manifest_length;
    entry.field(f_partition_spec_id) = 1;
    entry.field(f_content) = 0;
    entry.field(f_sequence_number) = new_snapshot->getValue<Int32>(f_sequence_number);
    entry.field(f_min_sequence_number) = new_snapshot->getValue<Int32>(f_sequence_number);
    entry.field(f_min_sequence_number) = new_snapshot->getValue<Int32>(f_sequence_number);
    entry.field(f_added_snapshot_id) = new_snapshot->getValue<Int32>(MetadataGenerator::f_snapshot_id);
    entry.field(f_added_files_count) = 1;
    entry.field(f_existing_files_count) = new_snapshot->getObject(MetadataGenerator::f_summary)->getValue<Int32>(MetadataGenerator::f_total_data_files);
    entry.field(f_deleted_files_count) = 0;
    entry.field(f_added_rows_count) = new_snapshot->getObject(MetadataGenerator::f_summary)->getValue<Int32>(MetadataGenerator::f_added_records);
    entry.field(f_existing_rows_count) = new_snapshot->getObject(MetadataGenerator::f_summary)->getValue<Int32>(MetadataGenerator::f_total_records);
    entry.field(f_deleted_rows_count) = 0;

    std::unique_ptr<avro::OutputStream> out = avro::memoryOutputStream();
    avro::EncoderPtr encoder = avro::binaryEncoder();
    encoder->init(*out);
    avro::encode(*encoder, entry_datum);
    encoder->flush();

    std::ostringstream oss;
    auto in = avro::memoryInputStream(*out);
    const uint8_t* data;
    size_t len;
    while (in->next(&data, &len)) {
        oss.write(reinterpret_cast<const char*>(data), len);
    }
    std::string bytes = oss.str();
    return bytes;
}

MetadataGenerator::MetadataGenerator(Poco::JSON::Object::Ptr metadata_object_)
    : metadata_object(metadata_object_)
    , gen(randomSeed())
    , dis(0, INT32_MAX)
{
}

Int64 MetadataGenerator::getMaxSequenceNumber()
{
    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    Int64 max_seq_number = 0;

    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto seq_number = snapshot->getValue<Int64>(f_sequence_number);
        max_seq_number = std::max(max_seq_number, seq_number);
    }
    return max_seq_number;
}

Poco::JSON::Object::Ptr MetadataGenerator::getParentSnapshot(Int64 parent_snapshot_id)
{
    auto snapshots = metadata_object->get(f_snapshots).extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < snapshots->size(); ++i)
    {
        const auto snapshot = snapshots->getObject(static_cast<UInt32>(i));
        auto snapshot_id = snapshot->getValue<Int64>(f_snapshot_id);
        if (snapshot_id == parent_snapshot_id)
            return snapshot;
    }
    return nullptr;
}

Poco::JSON::Object::Ptr MetadataGenerator::generateNextMetadata(
    const String & manifest_list_name,
    Int64 parent_snapshot_id,
    Int32 added_files,
    Int32 added_records,
    Int32 added_files_size)
{
    Poco::JSON::Object::Ptr new_snapshot = new Poco::JSON::Object;
    new_snapshot->set(f_sequence_number, getMaxSequenceNumber() + 1);

    Int32 snapshot_id = dis(gen);
    new_snapshot->set(f_snapshot_id, snapshot_id);
    new_snapshot->set(f_parent_snapshot_id, parent_snapshot_id);
    
    auto now = std::chrono::system_clock::now();
    auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    new_snapshot->set(f_timestamp_ms, ms.count());

    auto parent_snapshot = getParentSnapshot(parent_snapshot_id);
    Poco::JSON::Object::Ptr summary = new Poco::JSON::Object;
    summary->set("operation", "append");
    summary->set(f_added_data_files, added_files);
    summary->set(f_added_records, added_records);
    summary->set(f_added_files_size, added_files_size);
    summary->set(f_changed_partition_count, "1"); // TODO: change this if needs
    
    auto sum_with_parent_snapshot = [&] (const char * field_name, Int32 snapshot_value) {
        Int32 prev_value = parent_snapshot ? parent_snapshot->getObject(f_summary)->getValue<Int32>(field_name) : 0;
        summary->set(field_name, prev_value + snapshot_value);
    };

    sum_with_parent_snapshot(f_total_records, added_records);
    sum_with_parent_snapshot(f_total_files_size, added_files_size);
    sum_with_parent_snapshot(f_total_data_files, added_files);
    sum_with_parent_snapshot(f_total_delete_files, 0);
    sum_with_parent_snapshot(f_total_position_deletes, 0);
    sum_with_parent_snapshot(f_total_equality_deletes, 0);
    new_snapshot->set(f_summary, summary);

    new_snapshot->set(f_schema_id, parent_snapshot ? parent_snapshot->getValue<Int32>(f_schema_id) : 0);
    new_snapshot->set(f_manifest_list, manifest_list_name);

    metadata_object->getArray(f_snapshots)->add(new_snapshot);
    return new_snapshot;
}

IcebergStorageSink::IcebergStorageSink(
    ObjectStoragePtr object_storage,
    ConfigurationPtr configuration,
    const std::optional<FormatSettings> & format_settings_,
    const Block & sample_block_,
    ContextPtr context)
    : SinkToStorage(sample_block_)
    , sample_block(sample_block_)
{
    const auto & settings = context->getSettingsRef();
    const auto chosen_compression_method = chooseCompressionMethod("I NEED TO SPECIFY ALL", configuration->compression_method);

    auto buffer = object_storage->writeObject(
        StoredObject("I NEED TO SPECIFY ALL"), WriteMode::Rewrite, std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, context->getWriteSettings());

    write_buf = wrapWriteBufferWithCompressionMethod(
        std::move(buffer),
        chosen_compression_method,
        static_cast<int>(settings[Setting::output_format_compression_level]),
        static_cast<int>(settings[Setting::output_format_compression_zstd_window_log]));

    writer = FormatFactory::instance().getOutputFormatParallelIfPossible(
        configuration->format, *write_buf, sample_block, context, format_settings_);
}

void IcebergStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;
    writer->write(getHeader().cloneWithColumns(chunk.getColumns()));
}

void IcebergStorageSink::onFinish()
{
    if (isCancelled())
        return;

    finalizeBuffers();
    releaseBuffers();
}

void IcebergStorageSink::finalizeBuffers()
{
    if (!writer)
        return;

    try
    {
        writer->flush();
        writer->finalize();
    }
    catch (...)
    {
        /// Stop ParallelFormattingOutputFormat correctly.
        cancelBuffers();
        releaseBuffers();
        throw;
    }

    write_buf->finalize();
}

void IcebergStorageSink::releaseBuffers()
{
    writer.reset();
    write_buf.reset();
}

void IcebergStorageSink::cancelBuffers()
{
    if (writer)
        writer->cancel();
    if (write_buf)
        write_buf->cancel();
}

}

#endif
