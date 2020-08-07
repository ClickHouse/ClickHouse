#pragma once

#include "config_formats.h"
#include "config_core.h"

#if USE_AVRO

#include <unordered_map>
#include <map>
#include <vector>

#include <Core/Block.h>
#include <Formats/FormatSettings.h>
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
    AvroDeserializer(const Block & header, avro::ValidSchema schema, const FormatSettings & format_settings);
    void deserializeRow(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const;

private:
    using DeserializeFn = std::function<void(IColumn & column, avro::Decoder & decoder)>;
    using SkipFn = std::function<void(avro::Decoder & decoder)>;
    static DeserializeFn createDeserializeFn(avro::NodePtr root_node, DataTypePtr target_type);
    SkipFn createSkipFn(avro::NodePtr root_node);

    struct Action
    {
        enum Type {Noop, Deserialize, Skip, Record, Union};
        Type type;
        /// Deserialize
        int target_column_idx;
        DeserializeFn deserialize_fn;
        /// Skip
        SkipFn skip_fn;
        /// Record | Union
        std::vector<Action> actions;


        Action() : type(Noop) {}

        Action(int target_column_idx_, DeserializeFn deserialize_fn_)
            : type(Deserialize)
            , target_column_idx(target_column_idx_)
            , deserialize_fn(deserialize_fn_) {}

        Action(SkipFn skip_fn_)
            : type(Skip)
            , skip_fn(skip_fn_) {}

        static Action recordAction(std::vector<Action> field_actions) { return Action(Type::Record, field_actions); }

        static Action unionAction(std::vector<Action> branch_actions) { return Action(Type::Union, branch_actions); }


        void execute(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const
        {
            switch (type)
            {
                case Noop:
                    break;
                case Deserialize:
                    deserialize_fn(*columns[target_column_idx], decoder);
                    ext.read_columns[target_column_idx] = true;
                    break;
                case Skip:
                    skip_fn(decoder);
                    break;
                case Record:
                    for (const auto & action : actions)
                        action.execute(columns, decoder, ext);
                    break;
                case Union:
                    actions[decoder.decodeUnionIndex()].execute(columns, decoder, ext);
                    break;
            }
        }
    private:
        Action(Type type_, std::vector<Action> actions_)
            : type(type_)
            , actions(actions_) {}
    };

    /// Populate actions by recursively traversing root schema
    AvroDeserializer::Action createAction(const Block & header, const avro::NodePtr & node, const std::string & current_path = "");

    /// Bitmap of columns found in Avro schema
    std::vector<bool> column_found;
    /// Deserialize/Skip actions for a row
    Action row_action;
    /// Map from name of named Avro type (record, enum, fixed) to SkipFn.
    /// This is to avoid infinite recursion when  Avro schema contains self-references. e.g. LinkedList
    std::map<avro::Name, SkipFn> symbolic_skip_fn_map;
};

class AvroRowInputFormat : public IRowInputFormat
{
public:
    AvroRowInputFormat(const Block & header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);
    virtual bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    String getName() const override { return "AvroRowInputFormat"; }

private:
    avro::DataFileReaderBase file_reader;
    AvroDeserializer deserializer;
};

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
    FormatSettings format_settings;
};

}

#endif
