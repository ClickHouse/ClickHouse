#pragma once
#include "config_formats.h"
#if USE_AVRO

#include <unordered_map>
#include <vector>

#include <Core/Block.h>
#include <Formats/FormatSchemaInfo.h>
#include <Processors/Formats/IRowInputFormat.h>

#include <avro/DataFile.hh>
#include <avro/Decoder.hh>
#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>


namespace DB
{
class AvroDeserializer
{
public:
    AvroDeserializer(const DB::ColumnsWithTypeAndName & columns, avro::ValidSchema schema);
    void deserializeRow(MutableColumns & columns, avro::Decoder & decoder);

private:
    using DeserializeFn = std::function<void(IColumn & column, avro::Decoder & decoder)>;
    using SkipFn = std::function<void(avro::Decoder & decoder)>;
    static DeserializeFn createDeserializeFn(avro::NodePtr root_node, DataTypePtr target_type);
    static SkipFn createSkipFn(avro::NodePtr root_node);

    std::vector<int> field_mapping;
    std::vector<SkipFn> skip_fns;
    std::vector<DeserializeFn> deserialize_fns;
};

class AvroRowInputFormat : public IRowInputFormat
{
public:
    AvroRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_);
    virtual bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "AvroRowInputFormat"; }

private:
    avro::DataFileReaderBase file_reader;
    AvroDeserializer deserializer;
};

class AvroConfluentRowInputFormat : public IRowInputFormat
{
public:
    AvroConfluentRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);
    virtual bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "AvroConfluentRowInputFormat"; }

private:
    const DB::ColumnsWithTypeAndName columns;

    class SchemaRegistry;
    std::unique_ptr<SchemaRegistry> schema_registry;

    using SchemaId = uint32_t;
    std::unordered_map<SchemaId, AvroDeserializer> deserializer_cache;
    AvroDeserializer & getOrCreateDeserializer(SchemaId schema_id);

    avro::InputStreamPtr input_stream;
    avro::DecoderPtr decoder;
};
}
#endif
