#include <cstddef>
#include <cstdint>

#include <Processors/Formats/fuzzers/AvroFuzzerCommon.h>

#include <Processors/Formats/Impl/AvroRowInputFormat.h>

#include <Formats/FormatFactory.h>

#include <IO/ReadBufferFromMemory.h>

#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#if !USE_AVRO_RUST
#include <DataFile.hh>
#include <Generic.hh>
#include <GenericDatum.hh>
#endif

#include <algorithm>
#include <vector>

using namespace DB;

#if !USE_AVRO_RUST
namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}
#endif

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

namespace
{

/// Rows and entries are not limited by the format, only the number of those that are looked at by one input.
constexpr size_t MAX_ROWS_TO_INSPECT = 16;
constexpr size_t MAX_ROWS_TO_WALK = 1 << 16;

template <typename F>
void attempt(F && f)
{
    try
    {
        f();
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: the readers of the manifests report malformed files as exceptions, and the next accessor must still run.
    }
}

/// The fields a manifest list and a manifest entry are read through by the Iceberg code, plus the paths
/// that do not exist in either of them.
const char * const accessed_paths[] = {
    Iceberg::f_manifest_path,
    Iceberg::f_sequence_number,
    Iceberg::f_min_sequence_number,
    Iceberg::f_content,
    Iceberg::f_added_snapshot_id,
    Iceberg::f_partition_spec_id,
    Iceberg::f_manifest_length,
    Iceberg::f_status,
    Iceberg::f_snapshot_id,
    Iceberg::f_data_file,
    Iceberg::c_data_file_file_path,
    Iceberg::c_data_file_file_format,
    Iceberg::c_data_file_content,
    Iceberg::c_data_file_partition,
    Iceberg::c_data_file_record_count,
    Iceberg::c_data_file_file_size_in_bytes,
    Iceberg::c_data_file_lower_bounds,
    Iceberg::c_data_file_upper_bounds,
    Iceberg::c_data_file_equality_ids,
    Iceberg::c_data_file_key_metadata,
    Iceberg::c_data_file_referenced_data_file,
    Iceberg::c_partitions_contains_null,
    Iceberg::f_partitions,
    Iceberg::c_partitions_contains_nan,
    Iceberg::c_partitions_lower_bound,
    Iceberg::c_partitions_upper_bound,
    Iceberg::f_added_files_count,
    Iceberg::f_existing_files_count,
    Iceberg::f_deleted_files_count,
    Iceberg::f_added_rows_count,
    Iceberg::f_existing_rows_count,
    Iceberg::f_deleted_rows_count,
    Iceberg::f_key_metadata,
    Iceberg::f_manifest_first_row_id,
    "no.such.path",
};

#if !USE_AVRO_RUST
constexpr size_t MAX_DATUMS_TO_WALK = 1 << 16;

/// The shape of what the callers of `Iceberg::forEachAvroEntry` do: look at the record's fields, and the elements of the
/// collections, without recursion (the data of a recursive schema is as deep as the input is long).
size_t touchDatum(const avro::GenericDatum & root)
{
    size_t touched = 0;
    std::vector<const avro::GenericDatum *> stack{&root};
    while (!stack.empty() && touched < MAX_DATUMS_TO_WALK)
    {
        const avro::GenericDatum * datum = stack.back();
        stack.pop_back();
        ++touched;

        if (datum->isUnion())
            touched += size_t(datum->unionBranch());

        switch (datum->type())
        {
            case avro::AVRO_RECORD:
            {
                const auto & record = datum->value<avro::GenericRecord>();
                (void) record.hasField(Iceberg::f_data_file);
                for (size_t i = 0; i != record.fieldCount(); ++i)
                    stack.push_back(&record.fieldAt(i));
                break;
            }
            case avro::AVRO_ARRAY:
                for (const auto & element : datum->value<avro::GenericArray>().value())
                    stack.push_back(&element);
                break;
            case avro::AVRO_MAP:
                for (const auto & [key, value] : datum->value<avro::GenericMap>().value())
                {
                    touched += key.size();
                    stack.push_back(&value);
                }
                break;
            case avro::AVRO_STRING:
                touched += datum->value<std::string>().size();
                break;
            case avro::AVRO_BYTES:
                touched += datum->value<std::vector<uint8_t>>().size();
                break;
            case avro::AVRO_FIXED:
                touched += datum->value<avro::GenericFixed>().value().size();
                break;
            case avro::AVRO_ENUM:
                (void) datum->value<avro::GenericEnum>().symbol();
                break;
            default:
                break;
        }
    }
    return touched;
}
#endif

}

/** Fuzzes how the Iceberg (and Paimon) code reads its Avro files: manifest lists and manifests.
  *
  * The input is the bytes of an Object Container File. Two independent readers run on it, as in the server:
  *
  *  1. `AvroForIcebergDeserializer`, constructed over a memory file as `ManifestFileCacheableInfo::getDeserializer`
  *     does: with `USE_AVRO_RUST` it reads every row through `AvroRust::OcfReader` and
  *     `AvroRust::Plan`, into one `Tuple` column inferred from the whole schema (`AvroRust::inferType`), and keeps which rows of
  *     a field that cannot be `Nullable` are an Avro null, otherwise it reads through `AvroDeserializer`; and it keeps the file
  *     metadata. Then the accessors used by `ManifestFileIterator`, `StatelessMetadataFileGetter`, `PaimonClient`
  *     and `generateManifestList` are called for the first rows: `hasPath`, `getTypeForPath`,
  *     `getDataTypeForPath`, `getValueFromRowByName`, `isNullInRow` (`USE_AVRO_RUST`), `getParsedManifestFileEntry`,
  *     `getContent`, `getMetadataContent`, `getFormatVersionFromManifestFileMetadata`.
  *
  *  1b. With `USE_AVRO_RUST`, the reader of the metadata files that streams them in batches: `forEachBatch`, which the copy of
  *     the parent manifest list in `generateManifestList` goes through.
  *
  *  2. Without `USE_AVRO_RUST`, the shape of `Iceberg::forEachAvroEntry`, which streams the entries of the parent manifest list
  *     for the writer and the replaced files for the Hudi reader: `DataFileReaderBase` on an
  *     `AvroInputStreamReadBufferAdapter` with `MAX_AVRO_SCHEMA_DEPTH`, wrapped in `DataFileReader<GenericDatum>`, the root must
  *     be a record, and every datum is decoded into a `GenericDatum`, which is then walked.
  *
  * Iceberg errors (`ICEBERG_SPECIFICATION_VIOLATION`, ...) and anything else are caught and are not findings; a
  * sanitizer report, a timeout or an out-of-memory is.
  *
  * Compile the code as follows:
  *   mkdir build_asan_fuzz
  *   cd build_asan_fuzz
  *   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
  *   ninja avro_iceberg_manifest_fuzzer
  *
  * The fuzzer can be run as follows:
  *   ../../../build_asan_fuzz/src/Processors/Formats/fuzzers/avro_iceberg_manifest_fuzzer corpus -jobs=64 -rss_limit_mb=8192 \
  *       -dict=../../../tests/fuzz/avro.dict
  *
  * The seed corpus is `tests/fuzz/avro_iceberg_manifest_fuzzer.in`.
  */
extern "C" int LLVMFuzzerInitialize(const int *, char ***)
{
    AvroFuzzer::initialize();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        AvroFuzzer::resetMemoryLimits();

        const std::string_view bytes(reinterpret_cast<const char *>(data), size);

        /// 1. `AvroForIcebergDeserializer`.
        attempt(
            [&]
            {
                const auto manifest_path = Iceberg::IcebergPathFromMetadata::deserialize("metadata/fuzzer-m0.avro");
                const Iceberg::AvroForIcebergDeserializer deserializer(
                    std::make_unique<ReadBufferFromOutsideMemoryFile>(manifest_path.serialize(), bytes),
                    manifest_path,
                    getFormatSettings(AvroFuzzer::getContext()));

                attempt([&] { (void) deserializer.bytesRead(); });
                attempt([&] { (void) deserializer.getFormatVersionFromManifestFileMetadata(); });
                attempt([&] { (void) deserializer.getMetadataContent(); });
                attempt([&] { (void) deserializer.tryGetAvroMetadataValue("avro.schema"); });

                for (const char * path : accessed_paths)
                {
                    attempt([&] { (void) deserializer.hasPath(path); });
                    attempt([&] { (void) deserializer.getTypeForPath(path); });
                }

                const size_t rows = std::min(deserializer.rows(), MAX_ROWS_TO_INSPECT);
                for (size_t row = 0; row != rows; ++row)
                {
                    for (const char * path : accessed_paths)
                        attempt([&] { (void) deserializer.getValueFromRowByName(row, path); });
#if USE_AVRO_RUST
                    /// The null flags of the fields that are not `Nullable`, which tell a null `partitions` from an empty one.
                    for (const char * path : accessed_paths)
                        attempt([&] { (void) deserializer.isNullInRow(row, path); });
#endif

                    attempt([&] { (void) deserializer.getContent(row); });
                    attempt([&] { (void) deserializer.getParsedManifestFileEntry(row); });
                }

                /// Out of range, as asked by a corrupt `row_number`.
                attempt([&] { (void) deserializer.getParsedManifestFileEntry(deserializer.rows()); });

                /// The readers that take a few fields of every entry (the copy of the parent manifest list in
                /// `generateManifestList`): all the rows, but only through the paths of the type they expect, as any other would
                /// throw on every row.
                auto walk_rows = [&](const char * path, TypeIndex expected_type)
                {
                    if (!deserializer.hasPath(path) || deserializer.getTypeForPath(path) != expected_type)
                        return;
                    const size_t walked_rows = std::min(deserializer.rows(), MAX_ROWS_TO_WALK);
                    for (size_t row = 0; row != walked_rows; ++row)
                        attempt([&] { (void) deserializer.getValueFromRowByName(row, path, expected_type); });
                };
                walk_rows(Iceberg::f_manifest_path, TypeIndex::String);
                walk_rows(Iceberg::c_data_file_file_path, TypeIndex::String);
                walk_rows(Iceberg::c_data_file_record_count, TypeIndex::Int64);
                for (const char * path : accessed_paths)
                    attempt([&] { (void) deserializer.getDataTypeForPath(path); });
            });

#if USE_AVRO_RUST
        /// 1b. The readers that stream the file in batches.
        attempt(
            [&]
            {
                const auto manifest_path = Iceberg::IcebergPathFromMetadata::deserialize("metadata/fuzzer-m1.avro");
                /// Over all batches: a file of many small blocks has as many batches as blocks.
                size_t inspected_rows = 0;
                Iceberg::AvroForIcebergDeserializer::forEachBatch(
                    std::make_unique<ReadBufferFromOutsideMemoryFile>(manifest_path.serialize(), bytes),
                    manifest_path,
                    getFormatSettings(AvroFuzzer::getContext()),
                    [&](const Iceberg::AvroForIcebergDeserializer & batch)
                    {
                        const size_t rows = std::min(batch.rows(), MAX_ROWS_TO_INSPECT - inspected_rows);
                        inspected_rows += rows;
                        for (size_t row = 0; row != rows; ++row)
                        {
                            for (const char * path : accessed_paths)
                            {
                                attempt([&] { (void) batch.getValueFromRowByName(row, path); });
                                attempt([&] { (void) batch.isNullInRow(row, path); });
                            }
                            attempt([&] { (void) batch.getParsedManifestFileEntry(row); });
                        }
                    });
            });
#endif

        /// 2. Without `USE_AVRO_RUST`, the shape of `Iceberg::forEachAvroEntry`, which streams the entries of the parent manifest
        /// list for the writer and the replaced files for the Hudi reader: it decodes the entries into `GenericDatum`.
        /// `DataFileReaderBase` on an `AvroInputStreamReadBufferAdapter`, wrapped in `DataFileReader<GenericDatum>`, every datum
        /// is decoded into a `GenericDatum`, which is then walked.
#if !USE_AVRO_RUST
        attempt(
            [&]
            {
                ReadBufferFromMemory in(data, size);
                auto input_stream = std::make_unique<AvroInputStreamReadBufferAdapter>(in);
                auto reader_base = std::make_unique<avro::DataFileReaderBase>(std::move(input_stream), MAX_AVRO_SCHEMA_DEPTH);
                avro::DataFileReader<avro::GenericDatum> reader(std::move(reader_base));

                if (reader.readerSchema().root()->type() != avro::AVRO_RECORD)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Avro file root schema type is not a record");

                avro::GenericDatum datum(reader.readerSchema());
                size_t walked = 0;
                while (reader.read(datum) && walked < MAX_DATUMS_TO_WALK)
                    walked += touchDatum(datum);
            });
#endif
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Ok: malformed input is expected to throw.
    }

    return 0;
}
