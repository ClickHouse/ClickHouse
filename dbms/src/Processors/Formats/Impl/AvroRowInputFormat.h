#pragma once
#include "config_formats.h"
#include "config_core.h"
#if USE_AVRO

#include <unordered_map>
#include <map>
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
    AvroDeserializer(const ColumnsWithTypeAndName & columns, avro::ValidSchema schema);
    void deserializeRow(MutableColumns & columns, avro::Decoder & decoder);

private:
    using DeserializeFn = std::function<void(IColumn & column, avro::Decoder & decoder)>;
    using SkipFn = std::function<void(avro::Decoder & decoder)>;
    struct SkipFnHolder { SkipFn skip_fn; };
    static DeserializeFn createDeserializeFn(avro::NodePtr root_node, DataTypePtr target_type);
    SkipFn createSkipFn(avro::NodePtr root_node);

    /// Map from field index in Avro schema to column number in block header. Or -1 if there is no corresponding column.
    std::vector<int> field_mapping;

    /// How to skip the corresponding field in Avro schema.
    std::vector<SkipFn> skip_fns;

    /// How to deserialize the corresponding field in Avro schema.
    std::vector<DeserializeFn> deserialize_fns;

    /// Map from name of named Avro type (record, enum, fixed) to SkipFn holder.
    /// This is to avoid infinite recursion when  Avro schema contains self-references. e.g. LinkedList
    std::map<avro::Name, SkipFnHolder> symbolic_skip_fn_map;
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

#if USE_POCO_JSON
class AvroConfluentRowInputFormat : public IRowInputFormat
{
public:
    AvroConfluentRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);
    virtual bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "AvroConfluentRowInputFormat"; }

private:
    const ColumnsWithTypeAndName header_columns;

    class SchemaRegistry;
    std::unique_ptr<SchemaRegistry> schema_registry;

    using SchemaId = uint32_t;
    std::unordered_map<SchemaId, AvroDeserializer> deserializer_cache;
    AvroDeserializer & getOrCreateDeserializer(SchemaId schema_id);

    avro::InputStreamPtr input_stream;
    avro::DecoderPtr decoder;
};
#endif

}
#endif
