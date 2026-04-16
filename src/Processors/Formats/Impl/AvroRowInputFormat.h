#pragma once

#include "config.h"

#if USE_AVRO

#include <unordered_map>
#include <map>
#include <vector>

#include <Formats/FormatSettings.h>
#include <Formats/FormatSchemaInfo.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

#include <DataFile.hh>
#include <Decoder.hh>
#include <Schema.hh>
#include <ValidSchema.hh>
#include <Columns/IColumn.h>
#include <Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

class Block;

class AvroInputStreamReadBufferAdapter : public avro::InputStream
{
public:
    explicit AvroInputStreamReadBufferAdapter(ReadBuffer & in_) : in(in_) {}

    bool next(const uint8_t ** data, size_t * len) override;

    void backup(size_t len) override;

    void skip(size_t len) override;

    size_t byteCount() const override;

private:
    ReadBuffer & in;
};

class AvroDeserializer
{
public:
    AvroDeserializer(const Block & header, avro::ValidSchema schema, bool allow_missing_fields, bool null_as_default_, const FormatSettings & settings_);
    AvroDeserializer(DataTypePtr data_type, const std::string & column_name, avro::ValidSchema schema, bool allow_missing_fields, bool null_as_default_, const FormatSettings & settings_);
    void deserializeRow(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const;

    using DeserializeFn = std::function<bool(IColumn & column, avro::Decoder & decoder)>;
    using DeserializeNestedFn = std::function<bool(IColumn & column, avro::Decoder & decoder)>;

private:
    using SkipFn = std::function<void(avro::Decoder & decoder)>;
    DeserializeFn createDeserializeFn(const avro::NodePtr & root_node, const DataTypePtr & target_type);
    SkipFn createSkipFn(const avro::NodePtr & root_node);

    struct Action
    {
        enum Type {Noop, Deserialize, Skip, Record, Union, Nested, UnionName};
        Type type;
        /// Deserialize | UnionName (value column, -1 if not requested)
        int target_column_idx;
        DeserializeFn deserialize_fn;
        /// Skip
        SkipFn skip_fn;
        /// Record | Union
        std::vector<Action> actions;
        /// For flattened Nested column | UnionName (per-branch value deserializers)
        std::vector<size_t> nested_column_indexes;
        std::vector<DeserializeFn> nested_deserializers;
        /// For UnionName: index of the $name column
        int name_column_idx = -1;
        /// For UnionName: per-branch name strings; empty string means the null branch (insert NULL)
        std::vector<std::string> branch_names;
        /// For UnionName: per-branch skip functions (used when value column is not requested)
        std::vector<SkipFn> branch_skip_fns;


        Action() : type(Noop), target_column_idx(0) {}

        Action(int target_column_idx_, DeserializeFn deserialize_fn_)
            : type(Deserialize)
            , target_column_idx(target_column_idx_)
            , deserialize_fn(deserialize_fn_) {}

        explicit Action(SkipFn skip_fn_)
            : type(Skip)
            , target_column_idx(0)
            , skip_fn(skip_fn_) {}

        Action(const std::vector<size_t> & nested_column_indexes_, const std::vector<DeserializeFn> & nested_deserializers_)
            : type(Nested)
            , target_column_idx(0)
            , nested_column_indexes(nested_column_indexes_)
            , nested_deserializers(nested_deserializers_) {}

        static Action recordAction(const std::vector<Action> & field_actions) { return Action(Type::Record, field_actions); }

        static Action unionAction(const std::vector<Action> & branch_actions) { return Action(Type::Union, branch_actions); }

        static Action unionNameAction(
            int name_col_idx,
            int value_col_idx,
            std::vector<std::string> bnames,
            std::vector<DeserializeFn> value_fns,
            std::vector<SkipFn> skip_fns)
        {
            Action a;
            a.type = UnionName;
            a.name_column_idx = name_col_idx;
            a.target_column_idx = value_col_idx;
            a.branch_names = std::move(bnames);
            a.nested_deserializers = std::move(value_fns);
            a.branch_skip_fns = std::move(skip_fns);
            return a;
        }


        void execute(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const
        {
            switch (type)
            {
                case Noop:
                    break;
                case Deserialize:
                    ext.read_columns[target_column_idx] = deserialize_fn(*columns[target_column_idx], decoder);
                    break;
                case Skip:
                    skip_fn(decoder);
                    break;
                case Record:
                    for (const auto & action : actions)
                        action.execute(columns, decoder, ext);
                    break;
                case Nested:
                    deserializeNested(columns, decoder, ext);
                    break;
                case Union:
                {
                    auto index = decoder.decodeUnionIndex();
                    if (index >= actions.size())
                    {
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Union index out of boundary");
                    }
                    actions[index].execute(columns, decoder, ext);
                    break;
                }
                case UnionName:
                    executeUnionName(columns, decoder, ext);
                    break;
            }
        }
    private:
        Action(Type type_, std::vector<Action> actions_)
            : type(type_)
            , target_column_idx(0)
            , actions(actions_) {}

        void deserializeNested(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const;
        void executeUnionName(MutableColumns & columns, avro::Decoder & decoder, RowReadExtension & ext) const;
    };

    /// Populate actions by recursively traversing root schema
    AvroDeserializer::Action createAction(const Block & header, const avro::NodePtr & node, const std::string & current_path = "");

    /// Build an Action that reads the union index once and populates both the optional value column
    /// and the `$name` sub-column.  `value_col_idx` is -1 when only the name is requested.
    AvroDeserializer::Action createUnionWithNameAction(
        const Block & header,
        const avro::NodePtr & node,
        const std::string & current_path,
        int value_col_idx,
        const DataTypePtr & value_type);

    /// Create a per-branch DeserializeFn that inserts directly into the outer union column type
    /// (e.g. `Nullable(T)` or `Variant(T1,T2)`), given the Avro node for one specific branch.
    /// Unlike `createDeserializeFn` for union nodes, the caller has already consumed the union
    /// index from the decoder, so this function must NOT call `decodeUnionIndex`.
    DeserializeFn createBranchValueDeserializeFn(
        const avro::NodePtr & branch_node,
        const DataTypePtr & outer_type,
        const avro::NodePtr & union_node);

    /// Bitmap of columns found in Avro schema
    std::vector<bool> column_found;
    /// Deserialize/Skip actions for a row
    Action row_action;
    /// Map from name of named Avro type (record, enum, fixed) to SkipFn.
    /// This is to avoid infinite recursion when  Avro schema contains self-references. e.g. LinkedList
    std::map<avro::Name, SkipFn> symbolic_skip_fn_map;

    bool null_as_default = false;

    const FormatSettings & settings;
};

class AvroRowInputFormat final : public IRowInputFormat
{
public:
    AvroRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);

    String getName() const override { return "AvroRowInputFormat"; }

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;

    bool supportsCountRows() const override { return true; }
    size_t countRows(size_t max_block_size) override;

    std::unique_ptr<avro::DataFileReaderBase> file_reader_ptr;
    std::unique_ptr<AvroDeserializer> deserializer_ptr;
    FormatSettings format_settings;
};

/// Confluent framing + Avro binary datum encoding. Mainly used for Kafka.
/// Uses 3 caches:
/// 1. global: schema registry cache (base_url -> SchemaRegistry)
/// 2. SchemaRegistry: schema cache (schema_id -> schema)
/// 3. AvroConfluentRowInputFormat: deserializer cache (schema_id -> AvroDeserializer)
/// This is needed because KafkaStorage creates a new instance of InputFormat per a batch of messages
class AvroConfluentRowInputFormat final : public IRowInputFormat
{
public:
    AvroConfluentRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_);
    String getName() const override { return "AvroConfluentRowInputFormat"; }

    class SchemaRegistry;

private:
    bool readRow(MutableColumns & columns, RowReadExtension & ext) override;
    void readPrefix() override;

    bool allowSyncAfterError() const override { return true; }
    void syncAfterError() override;

    std::shared_ptr<SchemaRegistry> schema_registry;
    using SchemaId = uint32_t;
    std::unordered_map<SchemaId, AvroDeserializer> deserializer_cache;
    const AvroDeserializer & getOrCreateDeserializer(SchemaId schema_id);

    avro::InputStreamPtr input_stream;
    avro::DecoderPtr decoder;
    FormatSettings format_settings;
};

class AvroSchemaReader : public ISchemaReader
{
public:
    AvroSchemaReader(ReadBuffer & in_, bool confluent_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

    static DataTypePtr avroNodeToDataType(avro::NodePtr node);
private:

    bool confluent;
    const FormatSettings format_settings;
};

}

#endif
