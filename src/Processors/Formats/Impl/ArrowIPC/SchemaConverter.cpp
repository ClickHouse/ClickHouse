#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>

#if USE_ARROW

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/DateLUTImpl.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/NetUtils.h>
#include <Common/PODArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_TYPE;
}
}

namespace DB::ArrowIPC
{

namespace
{

KeyValueMetadata parseMetadata(const flatbuffers::Vector<flatbuffers::Offset<flatbuf::KeyValue>> * kv)
{
    KeyValueMetadata result;
    if (!kv)
        return result;
    for (const auto * entry : *kv)
    {
        if (entry && entry->key())
            result[entry->key()->str()] = entry->value() ? entry->value()->str() : std::string{};
    }
    return result;
}

ArrowType parseType(const flatbuf::Field & field);

std::vector<ArrowField> parseChildren(const flatbuffers::Vector<flatbuffers::Offset<flatbuf::Field>> * children)
{
    std::vector<ArrowField> result;
    if (!children)
        return result;
    result.reserve(children->size());
    for (const auto * child : *children)
    {
        if (!child)
            continue;
        ArrowField f;
        f.name = child->name() ? child->name()->str() : std::string{};
        f.nullable = child->nullable();
        f.type = parseType(*child);
        if (const auto * dict = child->dictionary())
        {
            DictionaryEncoding enc;
            enc.id = dict->id();
            enc.is_ordered = dict->isOrdered();
            if (const auto * index_type = dict->indexType())
            {
                enc.index_bit_width = index_type->bitWidth();
                enc.index_is_signed = index_type->is_signed();
            }
            f.dictionary = enc;
        }
        f.custom_metadata = parseMetadata(child->custom_metadata());
        result.push_back(std::move(f));
    }
    return result;
}

ArrowType parseType(const flatbuf::Field & field)
{
    ArrowType type;
    switch (field.type_type())
    {
        case flatbuf::Type_Null:
            type.kind = TypeKind::Null;
            break;
        case flatbuf::Type_Int:
        {
            const auto * t = field.type_as_Int();
            type.kind = TypeKind::Int;
            type.bit_width = t->bitWidth();
            type.is_signed = t->is_signed();
            break;
        }
        case flatbuf::Type_FloatingPoint:
        {
            type.kind = TypeKind::FloatingPoint;
            type.float_precision = field.type_as_FloatingPoint()->precision();
            break;
        }
        case flatbuf::Type_Bool:
            type.kind = TypeKind::Bool;
            break;
        case flatbuf::Type_Decimal:
        {
            const auto * t = field.type_as_Decimal();
            type.kind = TypeKind::Decimal;
            type.precision = t->precision();
            type.scale = t->scale();
            type.decimal_bit_width = t->bitWidth();
            break;
        }
        case flatbuf::Type_Date:
            type.kind = TypeKind::Date;
            type.unit = field.type_as_Date()->unit();
            break;
        case flatbuf::Type_Time:
        {
            const auto * t = field.type_as_Time();
            type.kind = TypeKind::Time;
            type.unit = t->unit();
            type.time_bit_width = t->bitWidth();
            break;
        }
        case flatbuf::Type_Timestamp:
        {
            const auto * t = field.type_as_Timestamp();
            type.kind = TypeKind::Timestamp;
            type.unit = t->unit();
            if (t->timezone())
                type.timezone = t->timezone()->str();
            break;
        }
        case flatbuf::Type_Duration:
            type.kind = TypeKind::Duration;
            type.unit = field.type_as_Duration()->unit();
            break;
        case flatbuf::Type_Interval:
            type.kind = TypeKind::Interval;
            type.unit = field.type_as_Interval()->unit();
            break;
        case flatbuf::Type_Utf8:
            type.kind = TypeKind::Utf8;
            break;
        case flatbuf::Type_LargeUtf8:
            type.kind = TypeKind::LargeUtf8;
            break;
        case flatbuf::Type_Binary:
            type.kind = TypeKind::Binary;
            break;
        case flatbuf::Type_LargeBinary:
            type.kind = TypeKind::LargeBinary;
            break;
        case flatbuf::Type_BinaryView:
            type.kind = TypeKind::BinaryView;
            break;
        case flatbuf::Type_Utf8View:
            type.kind = TypeKind::Utf8View;
            break;
        case flatbuf::Type_FixedSizeBinary:
            type.kind = TypeKind::FixedSizeBinary;
            type.byte_width = field.type_as_FixedSizeBinary()->byteWidth();
            break;
        case flatbuf::Type_List:
            type.kind = TypeKind::List;
            type.children = parseChildren(field.children());
            break;
        case flatbuf::Type_LargeList:
            type.kind = TypeKind::LargeList;
            type.children = parseChildren(field.children());
            break;
        case flatbuf::Type_FixedSizeList:
            type.kind = TypeKind::FixedSizeList;
            type.list_size = field.type_as_FixedSizeList()->listSize();
            type.children = parseChildren(field.children());
            break;
        case flatbuf::Type_Struct_:
            type.kind = TypeKind::Struct;
            type.children = parseChildren(field.children());
            break;
        case flatbuf::Type_Map:
            type.kind = TypeKind::Map;
            type.children = parseChildren(field.children());
            break;
        case flatbuf::Type_Union:
        {
            const auto * t = field.type_as_Union();
            type.kind = TypeKind::Union;
            type.union_mode = t->mode();
            if (t->typeIds())
                for (int id : *t->typeIds())
                    type.union_type_ids.push_back(id);
            type.children = parseChildren(field.children());
            break;
        }
        default:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Arrow IPC type {} is not supported by the native reader yet",
                flatbuf::EnumNameType(field.type_type()));
    }
    return type;
}

}

ArrowSchema parseSchema(const flatbuf::Schema & schema)
{
    ArrowSchema result;
    result.little_endian = schema.endianness() == flatbuf::Endianness_Little;
    if (!result.little_endian)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Big-endian Arrow IPC streams are not supported by the native reader");

    const auto * fields = schema.fields();
    if (fields)
    {
        result.fields.reserve(fields->size());
        for (const auto * field : *fields)
        {
            if (!field)
                continue;
            ArrowField f;
            f.name = field->name() ? field->name()->str() : std::string{};
            f.nullable = field->nullable();
            f.type = parseType(*field);
            if (const auto * dict = field->dictionary())
            {
                DictionaryEncoding enc;
                enc.id = dict->id();
                enc.is_ordered = dict->isOrdered();
                if (const auto * index_type = dict->indexType())
                {
                    enc.index_bit_width = index_type->bitWidth();
                    enc.index_is_signed = index_type->is_signed();
                }
                f.dictionary = enc;
            }
            f.custom_metadata = parseMetadata(field->custom_metadata());
            result.fields.push_back(std::move(f));
        }
    }

    result.custom_metadata = parseMetadata(schema.custom_metadata());
    return result;
}

bool isUUIDField(const ArrowField & field)
{
    if (field.type.kind != TypeKind::FixedSizeBinary || field.type.byte_width != 16)
        return false;
    auto it = field.custom_metadata.find("ARROW:extension:name");
    if (it != field.custom_metadata.end() && it->second == "arrow.uuid")
        return true;
    auto logical = field.custom_metadata.find("PARQUET:logical_type");
    return logical != field.custom_metadata.end() && logical->second == "UUID";
}

namespace
{

int arrowTimeUnitForScale(UInt32 scale)
{
    if (scale == 0)
        return flatbuf::TimeUnit_SECOND;
    if (scale <= 3)
        return flatbuf::TimeUnit_MILLISECOND;
    if (scale <= 6)
        return flatbuf::TimeUnit_MICROSECOND;
    return flatbuf::TimeUnit_NANOSECOND;
}

flatbuffers::Offset<flatbuf::Field>
buildField(
    flatbuffers::FlatBufferBuilder & b, const std::string & name, const DataTypePtr & type, const FormatSettings & settings,
    flatbuffers::Offset<flatbuf::DictionaryEncoding> dictionary = 0);

flatbuffers::Offset<flatbuf::Field>
buildMapEntriesField(flatbuffers::FlatBufferBuilder & b, const DataTypeMap & map_type, const FormatSettings & settings)
{
    /// Arrow map child: a non-nullable struct "entries" with children "key" (non-nullable) and "value".
    std::vector<flatbuffers::Offset<flatbuf::Field>> kv;
    kv.push_back(buildField(b, "key", removeNullable(map_type.getKeyType()), settings));
    kv.push_back(buildField(b, "value", map_type.getValueType(), settings));
    auto kv_vec = b.CreateVector(kv);
    auto entries_name = b.CreateString("entries");
    auto struct_type = flatbuf::CreateStruct_(b).Union();
    return flatbuf::CreateField(b, entries_name, false, flatbuf::Type_Struct_, struct_type, 0, kv_vec);
}

flatbuffers::Offset<flatbuf::Field>
buildField(
    flatbuffers::FlatBufferBuilder & b, const std::string & name, const DataTypePtr & type, const FormatSettings & settings,
    flatbuffers::Offset<flatbuf::DictionaryEncoding> dictionary)
{
    DataTypePtr t = removeLowCardinality(type);
    const bool nullable = t->isNullable();
    t = removeNullable(t);

    flatbuf::Type type_type = flatbuf::Type_NONE;
    flatbuffers::Offset<void> type_offset;
    std::vector<flatbuffers::Offset<flatbuf::Field>> children;

    auto make_int = [&](int bits, bool is_signed) {
        type_type = flatbuf::Type_Int;
        type_offset = flatbuf::CreateInt(b, bits, is_signed).Union();
    };

    if (isBool(t))
    {
        type_type = flatbuf::Type_Bool;
        type_offset = flatbuf::CreateBool(b).Union();
    }
    else
    {
        const WhichDataType which(t);
        switch (which.idx)
        {
            case TypeIndex::UInt8: make_int(8, false); break;
            case TypeIndex::UInt16: make_int(16, false); break;
            case TypeIndex::UInt32: make_int(32, false); break;
            case TypeIndex::UInt64: make_int(64, false); break;
            case TypeIndex::Int8: case TypeIndex::Enum8: make_int(8, true); break;
            case TypeIndex::Int16: case TypeIndex::Enum16: make_int(16, true); break;
            case TypeIndex::Int32: make_int(32, true); break;
            case TypeIndex::Int64: make_int(64, true); break;
            case TypeIndex::DateTime: make_int(32, false); break;
            case TypeIndex::Float32:
                type_type = flatbuf::Type_FloatingPoint;
                type_offset = flatbuf::CreateFloatingPoint(b, flatbuf::Precision_SINGLE).Union();
                break;
            case TypeIndex::Float64:
                type_type = flatbuf::Type_FloatingPoint;
                type_offset = flatbuf::CreateFloatingPoint(b, flatbuf::Precision_DOUBLE).Union();
                break;
            case TypeIndex::Date:
                if (settings.arrow.output_date_as_uint16)
                {
                    make_int(16, false);
                }
                else
                {
                    type_type = flatbuf::Type_Date;
                    type_offset = flatbuf::CreateDate(b, flatbuf::DateUnit_DAY).Union();
                }
                break;
            case TypeIndex::Date32:
                type_type = flatbuf::Type_Date;
                type_offset = flatbuf::CreateDate(b, flatbuf::DateUnit_DAY).Union();
                break;
            case TypeIndex::DateTime64:
            {
                const auto & dt64 = assert_cast<const DataTypeDateTime64 &>(*t);
                const int unit = arrowTimeUnitForScale(dt64.getScale());
                flatbuffers::Offset<flatbuffers::String> tz_off = 0;
                if (dt64.hasExplicitTimeZone())
                    tz_off = b.CreateString(dt64.getTimeZone().getTimeZone());
                type_type = flatbuf::Type_Timestamp;
                type_offset = flatbuf::CreateTimestamp(b, static_cast<flatbuf::TimeUnit>(unit), tz_off).Union();
                break;
            }
            case TypeIndex::Decimal32: case TypeIndex::Decimal64: case TypeIndex::Decimal128: case TypeIndex::Decimal256:
            {
                const int bit_width = which.idx == TypeIndex::Decimal256 ? 256 : 128;
                type_type = flatbuf::Type_Decimal;
                type_offset = flatbuf::CreateDecimal(
                    b, static_cast<int32_t>(getDecimalPrecision(*t)), static_cast<int32_t>(getDecimalScale(*t)), bit_width).Union();
                break;
            }
            case TypeIndex::String:
                if (settings.arrow.output_string_as_string)
                {
                    type_type = flatbuf::Type_Utf8;
                    type_offset = flatbuf::CreateUtf8(b).Union();
                }
                else
                {
                    type_type = flatbuf::Type_Binary;
                    type_offset = flatbuf::CreateBinary(b).Union();
                }
                break;
            case TypeIndex::FixedString:
                if (settings.arrow.output_fixed_string_as_fixed_byte_array)
                {
                    type_type = flatbuf::Type_FixedSizeBinary;
                    type_offset = flatbuf::CreateFixedSizeBinary(
                        b, static_cast<int32_t>(assert_cast<const DataTypeFixedString &>(*t).getN())).Union();
                }
                else if (settings.arrow.output_string_as_string)
                {
                    type_type = flatbuf::Type_Utf8;
                    type_offset = flatbuf::CreateUtf8(b).Union();
                }
                else
                {
                    type_type = flatbuf::Type_Binary;
                    type_offset = flatbuf::CreateBinary(b).Union();
                }
                break;
            case TypeIndex::Array:
                children.push_back(buildField(b, "item", assert_cast<const DataTypeArray &>(*t).getNestedType(), settings));
                type_type = flatbuf::Type_List;
                type_offset = flatbuf::CreateList(b).Union();
                break;
            case TypeIndex::Tuple:
            {
                const auto & tuple = assert_cast<const DataTypeTuple &>(*t);
                const auto & elems = tuple.getElements();
                const auto & elem_names = tuple.getElementNames();
                for (size_t i = 0; i < elems.size(); ++i)
                    children.push_back(buildField(b, elem_names[i], elems[i], settings));
                type_type = flatbuf::Type_Struct_;
                type_offset = flatbuf::CreateStruct_(b).Union();
                break;
            }
            case TypeIndex::Map:
            {
                children.push_back(buildMapEntriesField(b, assert_cast<const DataTypeMap &>(*t), settings));
                type_type = flatbuf::Type_Map;
                type_offset = flatbuf::CreateMap(b, false).Union();
                break;
            }
            case TypeIndex::IPv4:
                make_int(32, false);
                break;
            case TypeIndex::UUID:
            case TypeIndex::IPv6:
            case TypeIndex::Int128:
            case TypeIndex::UInt128:
                type_type = flatbuf::Type_FixedSizeBinary;
                type_offset = flatbuf::CreateFixedSizeBinary(b, 16).Union();
                break;
            case TypeIndex::Int256:
            case TypeIndex::UInt256:
                type_type = flatbuf::Type_FixedSizeBinary;
                type_offset = flatbuf::CreateFixedSizeBinary(b, 32).Union();
                break;
            case TypeIndex::Interval:
            {
                const auto kind = assert_cast<const DataTypeInterval &>(*t).getKind().kind;
                int unit = -1;
                if (kind == IntervalKind::Kind::Second) unit = flatbuf::TimeUnit_SECOND;
                else if (kind == IntervalKind::Kind::Millisecond) unit = flatbuf::TimeUnit_MILLISECOND;
                else if (kind == IntervalKind::Kind::Microsecond) unit = flatbuf::TimeUnit_MICROSECOND;
                else if (kind == IntervalKind::Kind::Nanosecond) unit = flatbuf::TimeUnit_NANOSECOND;
                if (unit >= 0)
                {
                    type_type = flatbuf::Type_Duration;
                    type_offset = flatbuf::CreateDuration(b, static_cast<flatbuf::TimeUnit>(unit)).Union();
                }
                else
                {
                    make_int(64, true);
                }
                break;
            }
            case TypeIndex::Variant:
            {
                /// Mirror the library writer: a dense union with one child per variant (in global order)
                /// plus a trailing `null`-typed child that NULL rows refer to.
                const auto & variant_type = assert_cast<const DataTypeVariant &>(*t);
                const auto & variants = variant_type.getVariants();
                for (const auto & variant : variants)
                    children.push_back(buildField(b, variant->getFamilyName(), variant, settings));
                children.push_back(flatbuf::CreateField(
                    b, b.CreateString("NULL"), false, flatbuf::Type_Null, flatbuf::CreateNull(b).Union(), 0, 0));

                std::vector<int32_t> type_id_list(variants.size() + 1);
                for (size_t i = 0; i < type_id_list.size(); ++i)
                    type_id_list[i] = static_cast<int32_t>(i);
                auto type_ids_off = b.CreateVector(type_id_list);
                type_type = flatbuf::Type_Union;
                type_offset = flatbuf::CreateUnion(b, flatbuf::UnionMode_Dense, type_ids_off).Union();
                break;
            }
            default:
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "Native Arrow IPC writer does not support type {} yet", type->getName());
        }
    }

    /// UUID is an Arrow extension type over fixed_size_binary(16); flag it in the field metadata.
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuf::KeyValue>>> custom_metadata_off = 0;
    if (isUUID(t))
    {
        std::vector<flatbuffers::Offset<flatbuf::KeyValue>> kvs;
        kvs.push_back(flatbuf::CreateKeyValue(b, b.CreateString("ARROW:extension:name"), b.CreateString("arrow.uuid")));
        kvs.push_back(flatbuf::CreateKeyValue(b, b.CreateString("ARROW:extension:metadata"), b.CreateString("")));
        custom_metadata_off = b.CreateVector(kvs);
    }

    auto name_off = b.CreateString(name);
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuf::Field>>> children_off = 0;
    if (!children.empty())
        children_off = b.CreateVector(children);
    return flatbuf::CreateField(b, name_off, nullable, type_type, type_offset, dictionary, children_off, custom_metadata_off);
}

flatbuffers::Offset<flatbuf::Schema> buildSchemaTable(
    flatbuffers::FlatBufferBuilder & builder, const Names & names, const DataTypes & types, const FormatSettings & settings)
{
    const auto dictionaries = assignOutputDictionaries(types, settings);

    std::vector<flatbuffers::Offset<flatbuf::Field>> field_offsets;
    field_offsets.reserve(names.size());
    for (size_t i = 0; i < names.size(); ++i)
    {
        flatbuffers::Offset<flatbuf::DictionaryEncoding> dict_off = 0;
        if (dictionaries[i])
        {
            auto index_type = flatbuf::CreateInt(builder, dictionaries[i]->index_bit_width, dictionaries[i]->index_is_signed);
            dict_off = flatbuf::CreateDictionaryEncoding(builder, dictionaries[i]->id, index_type, /*isOrdered=*/false);
        }
        field_offsets.push_back(buildField(builder, names[i], types[i], settings, dict_off));
    }
    auto fields_vec = builder.CreateVector(field_offsets);

    flatbuffers::Offset<flatbuffers::Vector<int64_t>> features = 0;
    if (settings.arrow.output_compression_method != FormatSettings::ArrowCompression::NONE)
    {
        const std::vector<int64_t> feature_list{flatbuf::Feature_COMPRESSED_BODY};
        features = builder.CreateVector(feature_list);
    }
    return flatbuf::CreateSchema(builder, flatbuf::Endianness_Little, fields_vec, 0, features);
}

}

void buildSchemaMessage(
    flatbuffers::FlatBufferBuilder & builder, const Names & names, const DataTypes & types, const FormatSettings & settings)
{
    auto schema = buildSchemaTable(builder, names, types, settings);
    auto message = flatbuf::CreateMessage(builder, flatbuf::MetadataVersion_V5, flatbuf::MessageHeader_Schema, schema.Union(), 0);
    builder.Finish(message);
}

std::vector<std::optional<OutputDictionary>> assignOutputDictionaries(const DataTypes & types, const FormatSettings & settings)
{
    std::vector<std::optional<OutputDictionary>> result(types.size());
    if (!settings.arrow.low_cardinality_as_dictionary)
        return result;

    /// Arrow dictionary indexes must be 32- or 64-bit (signed or unsigned).
    const int bit_width = settings.arrow.use_64_bit_indexes_for_dictionary ? 64 : 32;
    const bool is_signed = settings.arrow.use_signed_indexes_for_dictionary;

    int64_t next_id = 0;
    for (size_t i = 0; i < types.size(); ++i)
        if (types[i]->lowCardinality())
            result[i] = OutputDictionary{next_id++, bit_width, is_signed};
    return result;
}

void buildFooter(
    flatbuffers::FlatBufferBuilder & builder,
    const Names & names,
    const DataTypes & types,
    const FormatSettings & settings,
    const std::vector<ArrowFileBlock> & dictionary_blocks,
    const std::vector<ArrowFileBlock> & record_blocks)
{
    auto schema = buildSchemaTable(builder, names, types, settings);

    std::vector<flatbuf::Block> dict_blocks;
    dict_blocks.reserve(dictionary_blocks.size());
    for (const auto & b : dictionary_blocks)
        dict_blocks.emplace_back(b.offset, b.metadata_length, b.body_length);
    auto dictionaries = builder.CreateVectorOfStructs(dict_blocks);

    std::vector<flatbuf::Block> blocks;
    blocks.reserve(record_blocks.size());
    for (const auto & b : record_blocks)
        blocks.emplace_back(b.offset, b.metadata_length, b.body_length);
    auto record_batches = builder.CreateVectorOfStructs(blocks);

    auto footer = flatbuf::CreateFooter(builder, flatbuf::MetadataVersion_V5, schema, dictionaries, record_batches, 0);
    builder.Finish(footer);
}

ArrowFileFooter readArrowFileFooter(SeekableReadBuffer & in, size_t file_size_)
{
    static constexpr std::string_view ARROW_MAGIC = "ARROW1";

    const off_t file_size = static_cast<off_t>(file_size_);
    /// Minimum: leading "ARROW1" + 2 padding bytes, a footer length and the trailing "ARROW1".
    if (file_size < static_cast<off_t>(2 * ARROW_MAGIC.size() + 2 + sizeof(int32_t)))
        throw Exception(ErrorCodes::INCORRECT_DATA, "File is too small to be an Arrow file");

    char magic[6];
    in.seek(0, SEEK_SET);
    in.readStrict(magic, ARROW_MAGIC.size());
    if (std::string_view(magic, ARROW_MAGIC.size()) != ARROW_MAGIC)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow file: missing leading magic");

    /// Trailing layout: <footer> <int32 footer length LE> <"ARROW1">.
    in.seek(file_size - static_cast<off_t>(ARROW_MAGIC.size()), SEEK_SET);
    in.readStrict(magic, ARROW_MAGIC.size());
    if (std::string_view(magic, ARROW_MAGIC.size()) != ARROW_MAGIC)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not an Arrow file: missing trailing magic");

    int32_t footer_length;
    in.seek(file_size - static_cast<off_t>(ARROW_MAGIC.size()) - static_cast<off_t>(sizeof(int32_t)), SEEK_SET);
    in.readStrict(reinterpret_cast<char *>(&footer_length), sizeof(footer_length));
    footer_length = DB::fromLittleEndian(footer_length);
    const off_t footer_offset
        = file_size - static_cast<off_t>(ARROW_MAGIC.size()) - static_cast<off_t>(sizeof(int32_t)) - footer_length;
    if (footer_length <= 0 || footer_offset < static_cast<off_t>(ARROW_MAGIC.size()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted Arrow file footer length {}", footer_length);

    PODArray<char> footer_storage(footer_length);
    in.seek(footer_offset, SEEK_SET);
    in.readStrict(footer_storage.data(), footer_length);

    flatbuffers::Verifier verifier(reinterpret_cast<const uint8_t *>(footer_storage.data()), footer_length);
    if (!flatbuf::VerifyFooterBuffer(verifier))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted Arrow file footer");
    const auto * footer = flatbuf::GetFooter(footer_storage.data());

    if (!footer->schema())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow file footer has no schema");

    ArrowFileFooter result;
    result.schema = parseSchema(*footer->schema());
    if (footer->dictionaries())
        for (const auto * block : *footer->dictionaries())
            result.dictionary_blocks.push_back({.offset = block->offset(), .metadata_length = 0, .body_length = block->bodyLength()});
    if (footer->recordBatches())
        for (const auto * block : *footer->recordBatches())
            result.record_batch_blocks.push_back({.offset = block->offset(), .metadata_length = 0, .body_length = block->bodyLength()});
    return result;
}

namespace
{

DataTypePtr intToCHType(const ArrowType & type)
{
    if (type.is_signed)
    {
        switch (type.bit_width)
        {
            case 8: return std::make_shared<DataTypeInt8>();
            case 16: return std::make_shared<DataTypeInt16>();
            case 32: return std::make_shared<DataTypeInt32>();
            case 64: return std::make_shared<DataTypeInt64>();
            default: break;
        }
    }
    else
    {
        switch (type.bit_width)
        {
            case 8: return std::make_shared<DataTypeUInt8>();
            case 16: return std::make_shared<DataTypeUInt16>();
            case 32: return std::make_shared<DataTypeUInt32>();
            case 64: return std::make_shared<DataTypeUInt64>();
            default: break;
        }
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow integer bit width {}", type.bit_width);
}

IntervalKind::Kind timeUnitToIntervalKind(int unit)
{
    switch (unit)
    {
        case flatbuf::TimeUnit_SECOND: return IntervalKind::Kind::Second;
        case flatbuf::TimeUnit_MILLISECOND: return IntervalKind::Kind::Millisecond;
        case flatbuf::TimeUnit_MICROSECOND: return IntervalKind::Kind::Microsecond;
        case flatbuf::TimeUnit_NANOSECOND: return IntervalKind::Kind::Nanosecond;
        default: break;
    }
    throw Exception(ErrorCodes::INCORRECT_DATA, "Unsupported Arrow duration unit {}", unit);
}

}

DataTypePtr fieldToCHType(const ArrowField & field, [[maybe_unused]] const FormatSettings & settings, bool make_nullable)
{
    const ArrowType & type = field.type;
    DataTypePtr result;

    switch (type.kind)
    {
        case TypeKind::Int:
            result = intToCHType(type);
            break;
        case TypeKind::FloatingPoint:
            result = type.float_precision == flatbuf::Precision_DOUBLE
                ? DataTypePtr(std::make_shared<DataTypeFloat64>())
                : DataTypePtr(std::make_shared<DataTypeFloat32>());
            break;
        case TypeKind::Bool:
            result = DataTypeFactory::instance().get("Bool");
            break;
        case TypeKind::Decimal:
            result = createDecimal<DataTypeDecimal>(type.precision, type.scale);
            break;
        case TypeKind::Date:
            result = type.unit == flatbuf::DateUnit_DAY
                ? DataTypePtr(std::make_shared<DataTypeDate32>())
                : DataTypePtr(std::make_shared<DataTypeDateTime>());
            break;
        case TypeKind::Timestamp:
        case TypeKind::Time:
        {
            UInt32 scale = static_cast<UInt32>(type.unit) * 3;
            result = type.timezone.empty()
                ? std::make_shared<DataTypeDateTime64>(scale)
                : std::make_shared<DataTypeDateTime64>(scale, type.timezone);
            break;
        }
        case TypeKind::Duration:
            result = std::make_shared<DataTypeInterval>(timeUnitToIntervalKind(type.unit));
            break;
        case TypeKind::Utf8:
        case TypeKind::LargeUtf8:
        case TypeKind::Binary:
        case TypeKind::LargeBinary:
        case TypeKind::BinaryView:
        case TypeKind::Utf8View:
            result = std::make_shared<DataTypeString>();
            break;
        case TypeKind::FixedSizeBinary:
            if (isUUIDField(field))
                result = std::make_shared<DataTypeUUID>();
            else
                result = std::make_shared<DataTypeFixedString>(type.byte_width);
            break;
        case TypeKind::List:
        case TypeKind::LargeList:
        case TypeKind::FixedSizeList:
        {
            const ArrowField & element = type.children.at(0);
            result = std::make_shared<DataTypeArray>(fieldToCHType(element, settings, element.nullable));
            break;
        }
        case TypeKind::Struct:
        {
            DataTypes elems;
            Names names;
            elems.reserve(type.children.size());
            names.reserve(type.children.size());
            for (const ArrowField & child : type.children)
            {
                elems.push_back(fieldToCHType(child, settings, child.nullable));
                names.push_back(child.name);
            }
            result = std::make_shared<DataTypeTuple>(elems, names);
            break;
        }
        case TypeKind::Map:
        {
            /// Arrow Map is List<Struct<key, value>>: children[0] is the entries struct.
            const ArrowField & entries = type.children.at(0);
            const ArrowField & key = entries.type.children.at(0);
            const ArrowField & value = entries.type.children.at(1);
            /// ClickHouse map keys are never nullable.
            result = std::make_shared<DataTypeMap>(
                fieldToCHType(key, settings, /*make_nullable=*/false),
                fieldToCHType(value, settings, value.nullable));
            break;
        }
        case TypeKind::Union:
        {
            /// Map an Arrow union to a ClickHouse Variant. Children of the Arrow `null` type are the
            /// placeholder used by the ClickHouse writer for NULL rows; they are not Variant elements.
            DataTypes variants;
            variants.reserve(type.children.size());
            for (const ArrowField & child : type.children)
            {
                if (child.type.kind == TypeKind::Null)
                    continue;
                variants.push_back(removeNullable(fieldToCHType(child, settings, /*make_nullable=*/false)));
            }
            result = std::make_shared<DataTypeVariant>(variants);
            break;
        }
        case TypeKind::Null:
        case TypeKind::Interval:
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Native Arrow IPC reader does not support this type yet (field '{}')",
                field.name);
    }

    if (!result)
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Could not map Arrow field '{}' to a ClickHouse type", field.name);

    if (make_nullable && result->canBeInsideNullable())
        result = std::make_shared<DataTypeNullable>(result);

    return result;
}

}

#endif
