#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>

#if USE_ARROW

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeFactory.h>

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
            result = std::make_shared<DataTypeString>();
            break;
        case TypeKind::FixedSizeBinary:
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
        case TypeKind::Null:
        case TypeKind::Interval:
        case TypeKind::Union:
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
