#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/ColumnsWithTypeAndName.h>
#include "config.h"

#if USE_AVRO

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Common/SharedMutex.h>

#include <functional>
#include <map>
#include <memory>
#include <unordered_map>

#include <base/defines.h>

namespace DB::Iceberg
{

struct ParsedManifestFileEntry;
using ParsedManifestFileEntryPtr = std::shared_ptr<const ParsedManifestFileEntry>;

/// In Iceberg manifest files and manifest lists are store in Avro format: https://avro.apache.org/
/// This format is some kind of mix between JSON and binary schemaful format like protobuf.
/// It has rich types system, with it's own binary representation and it's really tricky
/// to parse some of them.
///
/// In ClickHouse we already support avro as input format, so we can parse it: the whole file is decoded into one
/// nested Tuple column (with `USE_AVRO_RUST` by `AvroRust::Plan` over `AvroRust::OcfReader`, otherwise by `AvroDeserializer`),
/// which is really hard to operate in key-value fashion. That is why this class is written on top of our avro parser.
/// It allows to access files in avro files using syntax like "data_file.partition.XXXX" and return
/// Field values back. Also manages avro file metadata which is basically just mapping string -> string.
/// With `USE_AVRO_RUST` it is the reader of every Iceberg and Paimon metadata file that is read for its values: the
/// manifest files and manifest lists of Iceberg and Paimon, read whole by the constructor, and the parent manifest list
/// that the writer copies, read in bounded batches by `forEachBatch`. Without `USE_AVRO_RUST`, the writer and the
/// Hudi reader stream the entries as `avro::GenericDatum` through `Iceberg::forEachAvroEntry` instead of loading the whole
/// file.
class AvroForIcebergDeserializer
{
private:
    Iceberg::IcebergPathFromMetadata manifest_file_path;
    DB::ColumnPtr parsed_column;
    std::shared_ptr<const DB::DataTypeTuple> parsed_column_data_type;
    mutable std::optional<ColumnsWithTypeAndName> cache_parsed_columns TSA_GUARDED_BY(cache_mutex);
    mutable std::unordered_map<std::string, std::optional<std::pair<ColumnPtr, DataTypePtr>>>
        cache_extracted_subcolumns_with_types TSA_GUARDED_BY(cache_mutex);

    std::map<std::string, std::vector<uint8_t>> metadata;

    size_t bytes_read = 0;

#if USE_AVRO_RUST
    /// By top level field, whether each row is an Avro null; only for the fields of a non-`Nullable` type that have a null.
    std::unordered_map<std::string, std::vector<UInt8>> null_rows;

    /// By top level field, the kind of its Avro type before the schema inference coalesces kinds: `bytes` and `string` both
    /// become `String`, and so does `fixed`. A union of `null` and one other type is that other type.
    std::unordered_map<std::string, UInt8> field_kinds;
#endif

#if USE_AVRO_RUST
    /// The batch of a streamed file: `rows` rows of the top level fields in `columns`, in the order of the fields of the schema.
    AvroForIcebergDeserializer(
        const Iceberg::IcebergPathFromMetadata & manifest_file_path_,
        std::shared_ptr<const DB::DataTypeTuple> data_type,
        MutableColumns columns,
        size_t rows_,
        std::unordered_map<std::string, std::vector<UInt8>> null_rows_,
        std::unordered_map<std::string, UInt8> field_kinds_,
        const std::map<std::string, std::vector<uint8_t>> & metadata_,
        size_t bytes_read_);
#endif

    /// Shared mutex to protect mutable cache members for thread safety
    mutable SharedMutex cache_mutex;

    std::optional<std::pair<ColumnPtr, DataTypePtr>> & extractSubcolumnWithType(const std::string & path) const;

    /// Helper to format a row as JSON. Assumes cache_parsed_columns is initialized and lock is held.
    String formatRowAsJSON(const ColumnsWithTypeAndName & parsed_columns, size_t row_number) const TSA_REQUIRES_SHARED(cache_mutex);

    mutable std::vector<ParsedManifestFileEntryPtr> parsed_manifest_file_entries TSA_GUARDED_BY(cache_mutex);

    ParsedManifestFileEntryPtr createParsedManifestFileEntry(size_t row_index) const;
public:
    /// Returns the Iceberg format version of this manifest list / manifest file. Reads it
    /// from the file's own Avro metadata (the "format-version" key) when present. Older
    /// ClickHouse versions wrote both manifest lists and manifest files without that key,
    /// so we fall back to inspecting the schema: the `sequence_number` field appears in v2
    /// manifest lists and v2 manifest entries but not in their v1 counterparts.
    Int64 getFormatVersionFromManifestFileMetadata() const;

    AvroForIcebergDeserializer(
        std::unique_ptr<DB::ReadBufferFromFileBase> buffer_,
        const Iceberg::IcebergPathFromMetadata & manifest_file_path_,
        const DB::FormatSettings & format_settings);

    size_t rows() const;

    /// The constructor consumes the whole file, so this is its size on storage.
    size_t bytesRead() const { return bytes_read; }

    /// Allow to access avro paths like "a.b.c"
    bool hasPath(const std::string & path) const;
    DB::TypeIndex getTypeForPath(const std::string & path) const;
    /// The type of the column at `path` as the schema inference types it, e.g. `Nullable(Int64)` for an Avro `["null", "long"]`.
    /// Throws if there is no such path.
    DB::DataTypePtr getDataTypeForPath(const std::string & path) const;
    /// Allow to access avro paths like "a.b.c".
    /// If expected type is provided will throw an exception if types don't match
    DB::Field getValueFromRowByName(size_t row_num, const std::string & path, std::optional<DB::TypeIndex> expected_type = std::nullopt) const;

#if USE_AVRO_RUST
    /// Whether the value of the top level field `field` in row `row_num` is an Avro null that the column could not keep as
    /// one: `getValueFromRowByName` gives the default value for it, which for a field of a type that cannot be `Nullable`,
    /// like the `partitions` array of a manifest list, is an empty array, the same as for a value that is empty.
    /// False for any other field, including one that does not exist.
    bool isNullInRow(size_t row_num, const std::string & field) const;

    /// The `AvroKind` (of `avro_rust.h`) of the Avro type of the top level field `field`, as the file's schema declares it,
    /// where the column type is the kind after the schema inference has coalesced `bytes` with `string` and so on: for the
    /// readers that have to accept exactly the Avro type that the legacy `avro::GenericDatum` reader accepted. A union of
    /// `null` and one other type is that other type, a union of several types is `AVRO_KIND_UNION`. `nullopt` for a field
    /// that does not exist.
    std::optional<UInt8> getAvroKindOfField(const std::string & field) const;

    /// Reads the file in batches of at most `CANCELLATION_CHECK_PERIOD_ROWS` rows, so only one batch is in memory at a
    /// time, and calls `callback` with a deserializer of the rows of each batch, numbered from 0. Errors of the file are
    /// reported as by the constructor; an exception of `callback` is rethrown as it is.
    static void forEachBatch(
        std::unique_ptr<DB::ReadBufferFromFileBase> buffer,
        const Iceberg::IcebergPathFromMetadata & manifest_file_path,
        const DB::FormatSettings & format_settings,
        const std::function<void(const AvroForIcebergDeserializer & batch)> & callback);
#endif

    std::optional<std::string> tryGetAvroMetadataValue(std::string metadata_key) const;

    ParsedManifestFileEntryPtr getParsedManifestFileEntry(size_t row_index) const;

    String getContent(size_t row_number) const;
    String getMetadataContent() const;
};

}

#endif
