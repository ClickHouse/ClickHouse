#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Block_fwd.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>
#include <Formats/FormatFilterInfo.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <DataFile.hh>
#include <Encoder.hh>
#include <Schema.hh>
#include <ValidSchema.hh>

#include <string>

namespace DB
{
class Block;
class WriteBuffer;

/// The key of the Avro file metadata entry with the JSON array of the dotted paths of the `UUID2` values.
inline constexpr auto AVRO_UUID2_PATHS_METADATA_KEY = "ClickHouse:uuid2_paths";

/// The path of the alternative `index` of a `Variant` with `size` alternatives at `path`, as recorded under
/// `AVRO_UUID2_PATHS_METADATA_KEY`. The writer appends the null branch after the alternatives, so `index` is also
/// the index of the branch in the Avro union. A `Variant` with a single alternative is written as a union with null,
/// which is read back as `Nullable` of the alternative, so it keeps the path.
inline std::string getAvroVariantAlternativePath(const std::string & path, size_t size, size_t index)
{
    if (size == 1)
        return path;
    return path + ".variant" + std::to_string(index);
}

class AvroSerializerTraits;
class ConfluentSchemaRegistry;
class OutputStreamWriteBufferAdapter : public avro::OutputStream
{
public:
    explicit OutputStreamWriteBufferAdapter(WriteBuffer & out_) : out(out_) {}

    bool next(uint8_t ** data, size_t * len) override;

    void backup(size_t len) override { out.position() -= len; }

    uint64_t byteCount() const override { return out.count(); }
    void flush() override {}

private:
    WriteBuffer & out;
};


class AvroSerializer
{
public:
    AvroSerializer(const ColumnsWithTypeAndName & columns, std::unique_ptr<AvroSerializerTraits>, const FormatSettings & settings_, ColumnMapperPtr column_mapper_ = nullptr);
    const avro::ValidSchema & getSchema() const { return valid_schema; }

    /// Dotted paths (see `column_path` below) of the `UUID2` values. Avro has a single `uuid` logical type for both
    /// ClickHouse UUID types, so the writer records them in the file metadata (`AVRO_UUID2_PATHS_METADATA_KEY`) to
    /// let schema inference tell `UUID2` apart from `UUID`.
    const Strings & getUUID2Paths() const { return uuid2_paths; }
    void serializeRow(const Columns & columns, size_t row_num, avro::Encoder & encoder);

    using SerializeFn = std::function<void(const IColumn & column, size_t row_num, avro::Encoder & encoder)>;
    struct SchemaWithSerializeFn
    {
        avro::Schema schema;
        SerializeFn serialize;
    };

private:
    /// Type names for different complex types (e.g. enums, fixed strings) must be unique. We use simple incremental number to give them different names.
    /// `column_path` is the field's dotted path (`t.x`, `arr.element`, `m.value`); on the Iceberg
    /// path it picks Avro `string` vs `bytes` for a String column from the source logical type.
    SchemaWithSerializeFn createSchemaWithSerializeFn(const DataTypePtr & data_type, size_t & type_name_increment, const String & column_name, const String & column_path);

    /// Same, but for a site that owns a field (top-level column, tuple field, array element, map
    /// value). A complex container is never wrapped in Nullable, so its Iceberg optionality is not
    /// recoverable from the type: consult the per-path metadata and add the `["null", T]` union an
    /// optional field needs. A schema that is already a union or null carries its own optionality,
    /// so the union is added exactly once per path.
    SchemaWithSerializeFn createFieldSchemaWithSerializeFn(const DataTypePtr & data_type, size_t & type_name_increment, const String & column_name, const String & column_path);

    /// Sets the Iceberg `field-id` on every Avro record field, descending through union
    /// (Nullable/Variant), array and map wrappers so nested records also get ids. No-op without a mapper.
    void setIcebergFieldIds(const avro::NodePtr & node, const String & path);

    std::vector<SerializeFn> serialize_fns;
    Strings uuid2_paths;
    avro::ValidSchema valid_schema;
    std::unique_ptr<AvroSerializerTraits> traits;
    const FormatSettings & settings;
    /// Non-null only for Iceberg writes; maps dotted column name -> Iceberg field-id.
    ColumnMapperPtr column_mapper;
};

class AvroRowOutputFormat final : public IRowOutputFormat
{
public:
    AvroRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_, ColumnMapperPtr column_mapper_ = nullptr);
    ~AvroRowOutputFormat() override;

    String getName() const override { return "AvroRowOutputFormat"; }

private:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void writePrefix() override;
    void finalizeImpl() override;
    void resetFormatterImpl() override;

    void createFileWriter();

    FormatSettings settings;
    AvroSerializer serializer;
    std::unique_ptr<avro::DataFileWriterBase> file_writer_ptr;
};

/// Confluent wire format output: each row is prefixed with a 5-byte header
/// (magic byte 0x00 + 4-byte big-endian schema ID) followed by a raw Avro
/// binary datum (not OCF). The schema is registered with the Confluent Schema
/// Registry on first write.
class AvroConfluentRowOutputFormat final : public IRowOutputFormat
{
public:
    AvroConfluentRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_);
    ~AvroConfluentRowOutputFormat() override;

    String getName() const override { return "AvroConfluentRowOutputFormat"; }

private:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    void finalizeImpl() override {}

    FormatSettings settings;
    AvroSerializer serializer;
    std::shared_ptr<ConfluentSchemaRegistry> schema_registry;

    std::unique_ptr<OutputStreamWriteBufferAdapter> output_stream;
    avro::EncoderPtr encoder;
    uint32_t schema_id = 0;
    bool schema_registered = false;
};

}
#endif
