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
    AvroDeserializer(const Block & header, avro::ValidSchema schema);
    void deserializeRow(MutableColumns & columns, avro::Decoder & decoder) const;

private:
    using DeserializeFn = std::function<void(IColumn & column, avro::Decoder & decoder)>;
    using SkipFn = std::function<void(avro::Decoder & decoder)>;
    static DeserializeFn createDeserializeFn(avro::NodePtr root_node, DataTypePtr target_type);
    SkipFn createSkipFn(avro::NodePtr root_node);

    struct Action
    {
        enum Type { Deserialize, Skip };
        Type type;
        /// Deserialize
        int target_column_idx;
        DeserializeFn deserialize_fn;
        /// Skip
        SkipFn skip_fn;

        Action(int target_column_idx_, DeserializeFn deserialize_fn_)
            : type(Deserialize)
            , target_column_idx(target_column_idx_)
            , deserialize_fn(deserialize_fn_) {}

        Action(SkipFn skip_fn_)
            : type(Skip)
            , skip_fn(skip_fn_) {}

        void execute(MutableColumns & columns, avro::Decoder & decoder) const
        {
            switch (type)
            {
                case Deserialize:
                    deserialize_fn(*columns[target_column_idx], decoder);
                    break;
                case Skip:
                    skip_fn(decoder);
                    break;
            }
        }
    };

    /// Populate actions by recursively traversing root schema
    void createActions(const Block & header, const avro::NodePtr& node, std::string current_path = "");

    /// Bitmap of columns found in Avro schema
    std::vector<bool> column_found;
    /// Deserialize/Skip actions for a row
    std::vector<Action> actions;
    /// Map from name of named Avro type (record, enum, fixed) to SkipFn.
    /// This is to avoid infinite recursion when  Avro schema contains self-references. e.g. LinkedList
    std::map<avro::Name, SkipFn> symbolic_skip_fn_map;
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
/// Confluent framing + Avro binary datum encoding. Mainly used for Kafka.
/// Uses 3 caches:
/// 1. global: schema registry cache (base_url -> SchemaRegistry)
/// 2. SchemaRegistry: schema cache (schema_id -> schema)
/// 3. AvroConfluentRowInputFormat: deserializer cache (schema_id -> AvroDeserializer)
/// This is needed because KafkaStorage creates a new instance of InputFormat per a batch of messages
class AvroConfluentRowInputFormat : public IRowInputFormat
{
public:
    AvroConfluentRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);
    virtual bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "AvroConfluentRowInputFormat"; }

    class SchemaRegistry;
private:
    std::shared_ptr<SchemaRegistry> schema_registry;
    using SchemaId = uint32_t;
    std::unordered_map<SchemaId, AvroDeserializer> deserializer_cache;
    const AvroDeserializer & getOrCreateDeserializer(SchemaId schema_id);

    avro::InputStreamPtr input_stream;
    avro::DecoderPtr decoder;
};
#endif

}
#endif
