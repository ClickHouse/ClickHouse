#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Block_fwd.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <DataFile.hh>
#include <Encoder.hh>
#include <Schema.hh>
#include <ValidSchema.hh>

namespace DB
{
class Block;
class WriteBuffer;

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
    AvroSerializer(const ColumnsWithTypeAndName & columns, std::unique_ptr<AvroSerializerTraits>, const FormatSettings & settings_);
    const avro::ValidSchema & getSchema() const { return valid_schema; }
    void serializeRow(const Columns & columns, size_t row_num, avro::Encoder & encoder);

    using SerializeFn = std::function<void(const IColumn & column, size_t row_num, avro::Encoder & encoder)>;
    struct SchemaWithSerializeFn
    {
        avro::Schema schema;
        SerializeFn serialize;
    };

private:
    /// Type names for different complex types (e.g. enums, fixed strings) must be unique. We use simple incremental number to give them different names.
    SchemaWithSerializeFn createSchemaWithSerializeFn(const DataTypePtr & data_type, size_t & type_name_increment, const String & column_name);

    std::vector<SerializeFn> serialize_fns;
    avro::ValidSchema valid_schema;
    std::unique_ptr<AvroSerializerTraits> traits;
    const FormatSettings & settings;
};

class AvroRowOutputFormat final : public IRowOutputFormat
{
public:
    AvroRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_);
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
