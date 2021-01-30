#pragma once
#include "config_formats.h"
#if USE_AVRO
#include <unordered_map>

#include <Core/Block.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>

#include <avro/DataFile.hh>
#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>


namespace DB
{
class WriteBuffer;

class AvroSerializer
{
public:
    AvroSerializer(const ColumnsWithTypeAndName & columns);
    const avro::ValidSchema & getSchema() const { return schema; }
    void serializeRow(const Columns & columns, size_t row_num, avro::Encoder & encoder);

private:
    using SerializeFn = std::function<void(const IColumn & column, size_t row_num, avro::Encoder & encoder)>;
    struct SchemaWithSerializeFn
    {
        avro::Schema schema;
        SerializeFn serialize;
    };

    /// Type names for different complex types (e.g. enums, fixed strings) must be unique. We use simple incremental number to give them different names.
    static SchemaWithSerializeFn createSchemaWithSerializeFn(DataTypePtr data_type, size_t & type_name_increment);

    std::vector<SerializeFn> serialize_fns;
    avro::ValidSchema schema;
};

class AvroRowOutputFormat : public IRowOutputFormat
{
public:
    AvroRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & settings_);
    virtual ~AvroRowOutputFormat() override;

    String getName() const override { return "AvroRowOutputFormat"; }
    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const IDataType &, size_t) override {}
    virtual void writePrefix() override;
    virtual void writeSuffix() override;

private:
    FormatSettings settings;
    AvroSerializer serializer;
    avro::DataFileWriterBase file_writer;
};

}
#endif
