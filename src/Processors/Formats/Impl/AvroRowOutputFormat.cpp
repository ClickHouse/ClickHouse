#include "AvroRowOutputFormat.h"
#if USE_AVRO

#include <Core/Field.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>

#include <DataFile.hh>
#include <Encoder.hh>
#include <Node.hh>
#include <Schema.hh>

#include <re2/re2.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_COMPILE_REGEXP;
}

class AvroSerializerTraits
{
public:
    explicit AvroSerializerTraits(const FormatSettings & settings_)
        : string_to_string_regexp(settings_.avro.string_column_pattern)
    {
        if (!string_to_string_regexp.ok())
            throw DB::Exception(
                "Avro: cannot compile re2: " + settings_.avro.string_column_pattern + ", error: " + string_to_string_regexp.error()
                    + ". Look at https://github.com/google/re2/wiki/Syntax for reference.",
                DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
    }

    bool isStringAsString(const String & column_name)
    {
        return RE2::PartialMatch(column_name, string_to_string_regexp);
    }

private:
    const RE2 string_to_string_regexp;
};


class OutputStreamWriteBufferAdapter : public avro::OutputStream
{
public:
    explicit OutputStreamWriteBufferAdapter(WriteBuffer & out_) : out(out_) {}

    bool next(uint8_t ** data, size_t * len) override
    {
        out.nextIfAtEnd();
        *data = reinterpret_cast<uint8_t *>(out.position());
        *len = out.available();
        out.position() += out.available();

        return true;
    }

    void backup(size_t len) override { out.position() -= len; }

    uint64_t byteCount() const override { return out.count(); }
    void flush() override { }

private:
    WriteBuffer & out;
};


AvroSerializer::SchemaWithSerializeFn AvroSerializer::createSchemaWithSerializeFn(DataTypePtr data_type, size_t & type_name_increment, const String & column_name)
{
    ++type_name_increment;

    switch (data_type->getTypeId())
    {
        case TypeIndex::UInt8:
            return {avro::IntSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeInt(assert_cast<const ColumnUInt8 &>(column).getElement(row_num));
            }};
        case TypeIndex::Int8:
            return {avro::IntSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeInt(assert_cast<const ColumnInt8 &>(column).getElement(row_num));
            }};
        case TypeIndex::UInt16:
            return {avro::IntSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeInt(assert_cast<const ColumnUInt16 &>(column).getElement(row_num));
            }};
        case TypeIndex::Int16:
            return {avro::IntSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeInt(assert_cast<const ColumnInt16 &>(column).getElement(row_num));
            }};
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::DateTime:
            return {avro::IntSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeInt(assert_cast<const ColumnUInt32 &>(column).getElement(row_num));
            }};
        case TypeIndex::Int32:
            return {avro::IntSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeInt(assert_cast<const ColumnInt32 &>(column).getElement(row_num));
            }};
        case TypeIndex::UInt64:
            return {avro::LongSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeLong(assert_cast<const ColumnUInt64 &>(column).getElement(row_num));
            }};
        case TypeIndex::Int64:
            return {avro::LongSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeLong(assert_cast<const ColumnInt64 &>(column).getElement(row_num));
            }};
        case TypeIndex::Float32:
            return {avro::FloatSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeFloat(assert_cast<const ColumnFloat32 &>(column).getElement(row_num));
            }};
        case TypeIndex::Float64:
            return {avro::DoubleSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                encoder.encodeDouble(assert_cast<const ColumnFloat64 &>(column).getElement(row_num));
            }};
        case TypeIndex::Date:
        {
            auto schema = avro::IntSchema();
            schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::DATE));
            return {schema, [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                UInt16 date = assert_cast<const DataTypeDate::ColumnType &>(column).getElement(row_num);
                encoder.encodeInt(date);
            }};
        }
        case TypeIndex::DateTime64:
        {
            auto schema = avro::LongSchema();
            const auto & provided_type = assert_cast<const DataTypeDateTime64 &>(*data_type);

            if (provided_type.getScale() == 3)
                schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::TIMESTAMP_MILLIS));
            else if (provided_type.getScale() == 6)
                schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS));
            else
                break;

            return {schema, [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const auto & col = assert_cast<const DataTypeDateTime64::ColumnType &>(column);
                encoder.encodeLong(col.getElement(row_num));
            }};
        }
        case TypeIndex::String:
            if (traits->isStringAsString(column_name))
                return {avro::StringSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
                    {
                        const std::string_ref & s = assert_cast<const ColumnString &>(column).getDataAt(row_num).toView();
                        encoder.encodeString(std::string(s));
                    }
                };
            else
                return {avro::BytesSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
                    {
                        const std::string_view & s = assert_cast<const ColumnString &>(column).getDataAt(row_num).toString();
                        encoder.encodeBytes(reinterpret_cast<const uint8_t *>(s.data()), s.size());
                    }
                };
        case TypeIndex::FixedString:
        {
            auto size = data_type->getSizeOfValueInMemory();
            auto schema = avro::FixedSchema(size, "fixed_" + toString(type_name_increment));
            return {schema, [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const std::string_view & s = assert_cast<const ColumnFixedString &>(column).getDataAt(row_num).toView();
                encoder.encodeFixed(reinterpret_cast<const uint8_t *>(s.data()), s.size());
            }};
        }
        case TypeIndex::Enum8:
        {
            auto schema = avro::EnumSchema("enum8_" + toString(type_name_increment));    /// type names must be different for different types.
            std::unordered_map<DataTypeEnum8::FieldType, size_t> enum_mapping;
            const auto & enum_values = assert_cast<const DataTypeEnum8 &>(*data_type).getValues();
            for (size_t i = 0; i < enum_values.size(); ++i)
            {
                schema.addSymbol(enum_values[i].first);
                enum_mapping.emplace(enum_values[i].second, i);
            }
            return {schema, [enum_mapping](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                auto enum_value = assert_cast<const DataTypeEnum8::ColumnType &>(column).getElement(row_num);
                encoder.encodeEnum(enum_mapping.at(enum_value));
            }};
        }
        case TypeIndex::Enum16:
        {
            auto schema = avro::EnumSchema("enum16" + toString(type_name_increment));
            std::unordered_map<DataTypeEnum16::FieldType, size_t> enum_mapping;
            const auto & enum_values = assert_cast<const DataTypeEnum16 &>(*data_type).getValues();
            for (size_t i = 0; i < enum_values.size(); ++i)
            {
                schema.addSymbol(enum_values[i].first);
                enum_mapping.emplace(enum_values[i].second, i);
            }
            return {schema, [enum_mapping](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                auto enum_value = assert_cast<const DataTypeEnum16::ColumnType &>(column).getElement(row_num);
                encoder.encodeEnum(enum_mapping.at(enum_value));
            }};
        }
        case TypeIndex::UUID:
        {
            auto schema = avro::StringSchema();
            schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::UUID));
            return {schema, [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const auto & uuid = assert_cast<const DataTypeUUID::ColumnType &>(column).getElement(row_num);
                std::array<UInt8, 36> s;
                formatUUID(std::reverse_iterator<const UInt8 *>(reinterpret_cast<const UInt8 *>(&uuid) + 16), s.data());
                encoder.encodeBytes(reinterpret_cast<const uint8_t *>(s.data()), s.size());
            }};
        }
        case TypeIndex::Array:
        {
            const auto & array_type = assert_cast<const DataTypeArray &>(*data_type);
            auto nested_mapping = createSchemaWithSerializeFn(array_type.getNestedType(), type_name_increment, column_name);
            auto schema = avro::ArraySchema(nested_mapping.schema);
            return {schema, [nested_mapping](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
                const ColumnArray::Offsets & offsets = column_array.getOffsets();
                size_t offset = offsets[row_num - 1];
                size_t next_offset = offsets[row_num];
                size_t row_count = next_offset - offset;
                const IColumn & nested_column = column_array.getData();

                encoder.arrayStart();
                if (row_count > 0)
                {
                    encoder.setItemCount(row_count);
                }
                for (size_t i = offset; i < next_offset; ++i)
                {
                    nested_mapping.serialize(nested_column, i, encoder);
                }
                encoder.arrayEnd();
            }};
        }
        case TypeIndex::Nullable:
        {
            auto nested_type = removeNullable(data_type);
            auto nested_mapping = createSchemaWithSerializeFn(nested_type, type_name_increment, column_name);
            if (nested_type->getTypeId() == TypeIndex::Nothing)
            {
                return nested_mapping;
            }
            else
            {
                avro::UnionSchema union_schema;
                union_schema.addType(avro::NullSchema());
                union_schema.addType(nested_mapping.schema);
                return {union_schema, [nested_mapping](const IColumn & column, size_t row_num, avro::Encoder & encoder)
                {
                    const ColumnNullable & col = assert_cast<const ColumnNullable &>(column);
                    if (!col.isNullAt(row_num))
                    {
                        encoder.encodeUnionIndex(1);
                        nested_mapping.serialize(col.getNestedColumn(), row_num, encoder);
                    }
                    else
                    {
                        encoder.encodeUnionIndex(0);
                        encoder.encodeNull();
                    }
                }};
            }
        }
        case TypeIndex::LowCardinality:
        {
            const auto & nested_type = removeLowCardinality(data_type);
            auto nested_mapping = createSchemaWithSerializeFn(nested_type, type_name_increment, column_name);
            return {nested_mapping.schema, [nested_mapping](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const auto & col = assert_cast<const ColumnLowCardinality &>(column);
                nested_mapping.serialize(*col.getDictionary().getNestedColumn(), col.getIndexAt(row_num), encoder);
            }};
        }
        case TypeIndex::Nothing:
            return {avro::NullSchema(), [](const IColumn &, size_t, avro::Encoder & encoder) { encoder.encodeNull(); }};
        case TypeIndex::Tuple:
        {
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*data_type);
            const auto & nested_types = tuple_type.getElements();
            const auto & nested_names = tuple_type.getElementNames();
            std::vector<SerializeFn> nested_serializers;
            nested_serializers.reserve(nested_types.size());
            auto schema = avro::RecordSchema(column_name);
            for (size_t i = 0; i != nested_types.size(); ++i)
            {
                auto nested_mapping = createSchemaWithSerializeFn(nested_types[i], type_name_increment, nested_names[i]);
                schema.addField(nested_names[i], nested_mapping.schema);
                nested_serializers.push_back(nested_mapping.serialize);
            }

            return {schema, [nested_serializers](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const ColumnTuple & column_tuple = assert_cast<const ColumnTuple &>(column);
                const auto & nested_columns = column_tuple.getColumns();
                for (size_t i = 0; i != nested_serializers.size(); ++i)
                    nested_serializers[i](*nested_columns[i], row_num, encoder);
            }};
        }
        case TypeIndex::Map:
        {
            const auto & map_type = assert_cast<const DataTypeMap &>(*data_type);
            const auto & keys_type = map_type.getKeyType();
            if (!isStringOrFixedString(keys_type))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Avro Maps support only keys with type String, got {}", keys_type->getName());

            auto keys_serializer = [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const StringRef & s = column.getDataAt(row_num).toView();
                encoder.encodeString(std::string(s));
            };

            const auto & values_type = map_type.getValueType();
            auto values_mapping = createSchemaWithSerializeFn(values_type, type_name_increment, column_name + ".value");
            auto schema = avro::MapSchema(values_mapping.schema);

            return {schema, [keys_serializer, values_mapping](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const ColumnMap & column_map = assert_cast<const ColumnMap &>(column);
                const ColumnArray & column_array = column_map.getNestedColumn();
                const ColumnArray::Offsets & offsets = column_array.getOffsets();
                size_t offset = offsets[row_num - 1];
                size_t next_offset = offsets[row_num];
                size_t row_count = next_offset - offset;
                const ColumnTuple & nested_columns = column_map.getNestedData();
                const IColumn & keys_column = nested_columns.getColumn(0);
                const IColumn & values_column = nested_columns.getColumn(1);

                encoder.mapStart();
                if (row_count > 0)
                    encoder.setItemCount(row_count);

                for (size_t i = offset; i < next_offset; ++i)
                {
                    keys_serializer(keys_column, i, encoder);
                    values_mapping.serialize(values_column, i, encoder);
                }
                encoder.mapEnd();
            }};
        }
        default:
            break;
    }
    throw Exception("Type " + data_type->getName() + " is not supported for Avro output", ErrorCodes::ILLEGAL_COLUMN);
}


AvroSerializer::AvroSerializer(const ColumnsWithTypeAndName & columns, std::unique_ptr<AvroSerializerTraits> traits_)
    : traits(std::move(traits_))
{
    avro::RecordSchema record_schema("row");

    size_t type_name_increment = 0;
    for (const auto & column : columns)
    {
        try
        {
            auto field_mapping = createSchemaWithSerializeFn(column.type, type_name_increment, column.name);
            serialize_fns.push_back(field_mapping.serialize);
            //TODO: verify name starts with A-Za-z_
            record_schema.addField(column.name, field_mapping.schema);
        }
        catch (Exception & e)
        {
            e.addMessage("column " + column.name);
            throw;
        }
    }
    valid_schema.setSchema(record_schema);
}

void AvroSerializer::serializeRow(const Columns & columns, size_t row_num, avro::Encoder & encoder)
{
    size_t num_columns = columns.size();
    for (size_t i = 0; i < num_columns; ++i)
    {
        serialize_fns[i](*columns[i], row_num, encoder);
    }
}

static avro::Codec getCodec(const std::string & codec_name)
{
    if (codec_name.empty())
    {
#ifdef SNAPPY_CODEC_AVAILABLE
        return avro::Codec::SNAPPY_CODEC;
#else
        return avro::Codec::DEFLATE_CODEC;
#endif
    }

    if (codec_name == "null")    return avro::Codec::NULL_CODEC;
    if (codec_name == "deflate") return avro::Codec::DEFLATE_CODEC;
#ifdef SNAPPY_CODEC_AVAILABLE
    if (codec_name == "snappy")  return avro::Codec::SNAPPY_CODEC;
#endif

    throw Exception("Avro codec " + codec_name + " is not available", ErrorCodes::BAD_ARGUMENTS);
}

AvroRowOutputFormat::AvroRowOutputFormat(
    WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & settings_)
    : IRowOutputFormat(header_, out_, params_)
    , settings(settings_)
    , serializer(header_.getColumnsWithTypeAndName(), std::make_unique<AvroSerializerTraits>(settings))
{
}

AvroRowOutputFormat::~AvroRowOutputFormat() = default;

void AvroRowOutputFormat::createFileWriter()
{
    file_writer_ptr = std::make_unique<avro::DataFileWriterBase>(
        std::make_unique<OutputStreamWriteBufferAdapter>(out),
        serializer.getSchema(),
        settings.avro.output_sync_interval,
        getCodec(settings.avro.output_codec));
}

void AvroRowOutputFormat::writePrefix()
{
    // we have to recreate avro::DataFileWriterBase object due to its interface limitations
    createFileWriter();

    file_writer_ptr->syncIfNeeded();
}

void AvroRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    if (!file_writer_ptr)
        createFileWriter();
    file_writer_ptr->syncIfNeeded();
    serializer.serializeRow(columns, row_num, file_writer_ptr->encoder());
    file_writer_ptr->incr();
}

void AvroRowOutputFormat::writeSuffix()
{
    file_writer_ptr.reset();
}

void AvroRowOutputFormat::consume(DB::Chunk chunk)
{
    if (params.callback)
        consumeImplWithCallback(std::move(chunk));
    else
        consumeImpl(std::move(chunk));
}

void AvroRowOutputFormat::consumeImpl(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    for (size_t row = 0; row < num_rows; ++row)
    {
        write(columns, row);
    }

}

void AvroRowOutputFormat::consumeImplWithCallback(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    for (size_t row = 0; row < num_rows;)
    {
        size_t current_row = row;
        /// used by WriteBufferToKafkaProducer to obtain auxiliary data
        ///   from the starting row of a file

        writePrefixIfNot();
        for (size_t row_in_file = 0;
             row_in_file < settings.avro.output_rows_in_file && row < num_rows;
             ++row, ++row_in_file)
        {
            write(columns, row);
        }

        file_writer_ptr->flush();
        writeSuffix();
        need_write_prefix = true;

        params.callback(columns, current_row);
    }
}

void registerOutputFormatAvro(FormatFactory & factory)
{
    factory.registerOutputFormat("Avro", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<AvroRowOutputFormat>(buf, sample, params, settings);
    });
    factory.markFormatHasNoAppendSupport("Avro");
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatAvro(FormatFactory &)
{
}
}

#endif
