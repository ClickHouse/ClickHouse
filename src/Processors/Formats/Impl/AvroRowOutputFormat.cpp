#include "AvroRowOutputFormat.h"
#if USE_AVRO

#include <Core/Defines.h>
#include <Core/Field.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <Formats/verbosePrintString.h>
#include <Formats/FormatFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeUUID.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Node.hh>
#include <avro/NodeConcepts.hh>
#include <avro/NodeImpl.hh>
#include <avro/Reader.hh>
#include <avro/Schema.hh>
#include <avro/Specific.hh>
#include <avro/ValidSchema.hh>
#include <avro/Writer.hh>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

class OutputStreamWriteBufferAdapter : public avro::OutputStream
{
public:
    explicit OutputStreamWriteBufferAdapter(WriteBuffer & out_) : out(out_) {}

    virtual bool next(uint8_t ** data, size_t * len) override
    {
        out.nextIfAtEnd();
        *data = reinterpret_cast<uint8_t *>(out.position());
        *len = out.available();
        out.position() += out.available();

        return true;
    }

    virtual void backup(size_t len) override { out.position() -= len; }

    virtual uint64_t byteCount() const override { return out.count(); }
    virtual void flush() override { out.next(); }

private:
    WriteBuffer & out;
};


AvroSerializer::SchemaWithSerializeFn AvroSerializer::createSchemaWithSerializeFn(DataTypePtr data_type, size_t & type_name_increment)
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
            return {avro::BytesSchema(), [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const StringRef & s = assert_cast<const ColumnString &>(column).getDataAt(row_num);
                encoder.encodeBytes(reinterpret_cast<const uint8_t *>(s.data), s.size);
            }};
        case TypeIndex::FixedString:
        {
            auto size = data_type->getSizeOfValueInMemory();
            auto schema = avro::FixedSchema(size, "fixed_" + toString(type_name_increment));
            return {schema, [](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const StringRef & s = assert_cast<const ColumnFixedString &>(column).getDataAt(row_num);
                encoder.encodeFixed(reinterpret_cast<const uint8_t *>(s.data), s.size);
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
            auto nested_mapping = createSchemaWithSerializeFn(array_type.getNestedType(), type_name_increment);
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
            auto nested_mapping = createSchemaWithSerializeFn(nested_type, type_name_increment);
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
            auto nested_mapping = createSchemaWithSerializeFn(nested_type, type_name_increment);
            return {nested_mapping.schema, [nested_mapping](const IColumn & column, size_t row_num, avro::Encoder & encoder)
            {
                const auto & col = assert_cast<const ColumnLowCardinality &>(column);
                nested_mapping.serialize(*col.getDictionary().getNestedColumn(), col.getIndexAt(row_num), encoder);
            }};
        }
        case TypeIndex::Nothing:
            return {avro::NullSchema(), [](const IColumn &, size_t, avro::Encoder & encoder) { encoder.encodeNull(); }};
        default:
            break;
    }
    throw Exception("Type " + data_type->getName() + " is not supported for Avro output", ErrorCodes::ILLEGAL_COLUMN);
}


AvroSerializer::AvroSerializer(const ColumnsWithTypeAndName & columns)
{
    avro::RecordSchema record_schema("row");

    size_t type_name_increment = 0;
    for (const auto & column : columns)
    {
        try
        {
            auto field_mapping = createSchemaWithSerializeFn(column.type, type_name_increment);
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
    schema.setSchema(record_schema);
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
    WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & settings_)
    : IRowOutputFormat(header_, out_, callback)
    , settings(settings_)
    , serializer(header_.getColumnsWithTypeAndName())
    , file_writer(
        std::make_unique<OutputStreamWriteBufferAdapter>(out_),
        serializer.getSchema(),
        settings.avro.output_sync_interval,
        getCodec(settings.avro.output_codec))
{
}

AvroRowOutputFormat::~AvroRowOutputFormat() = default;

void AvroRowOutputFormat::writePrefix()
{
    file_writer.syncIfNeeded();
}

void AvroRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    file_writer.syncIfNeeded();
    serializer.serializeRow(columns, row_num, file_writer.encoder());
    file_writer.incr();
}

void AvroRowOutputFormat::writeSuffix()
{
    file_writer.close();
}

void registerOutputFormatProcessorAvro(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("Avro", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback callback,
        const FormatSettings & settings)
    {
        return std::make_shared<AvroRowOutputFormat>(buf, sample, callback, settings);
    });
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatProcessorAvro(FormatFactory &)
{
}
}

#endif
