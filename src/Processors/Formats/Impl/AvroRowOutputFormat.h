#pragma once
#include "config_formats.h"
#if USE_AVRO
#include <unordered_map>

#include <Core/Block.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>

#include <DataFile.hh>
#include <Schema.hh>
#include <ValidSchema.hh>


namespace DB
{
class WriteBuffer;

class AvroSerializerTraits;

class AvroSerializer
{
public:
    AvroSerializer(const ColumnsWithTypeAndName & columns, std::unique_ptr<AvroSerializerTraits>);
    const avro::ValidSchema & getSchema() const { return valid_schema; }
    void serializeRow(const Columns & columns, size_t row_num, avro::Encoder & encoder);

private:
    using SerializeFn = std::function<void(const IColumn & column, size_t row_num, avro::Encoder & encoder)>;
    struct SchemaWithSerializeFn
    {
        avro::Schema schema;
        SerializeFn serialize;
    };

    /// Type names for different complex types (e.g. enums, fixed strings) must be unique. We use simple incremental number to give them different names.
    SchemaWithSerializeFn createSchemaWithSerializeFn(DataTypePtr data_type, size_t & type_name_increment, const String & column_name);

    std::vector<SerializeFn> serialize_fns;
    avro::ValidSchema valid_schema;
    std::unique_ptr<AvroSerializerTraits> traits;
};

class AvroRowOutputFormat final : public IRowOutputFormat
{
public:
    AvroRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & settings_);
    virtual ~AvroRowOutputFormat() override;

    void consume(Chunk) override;
    String getName() const override { return "AvroRowOutputFormat"; }

private:
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override {}
    virtual void writePrefix() override;
    virtual void writeSuffix() override;

    void createFileWriter();

    FormatSettings settings;
    AvroSerializer serializer;
    std::unique_ptr<avro::DataFileWriterBase> file_writer_ptr;

    void consumeImpl(Chunk);
    void consumeImplWithCallback(Chunk);

};

}
#endif
