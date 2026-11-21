#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Processors/Formats/Impl/Avro/AvroRustSchema.h>

#if USE_AVRO_RUST

#include <Common/Exception.h>
#include <Processors/Formats/Impl/AvroRowInputFormat.h>
#include <Common/checkStackSize.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <algorithm>
#include <string>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int AVRO_EXCEPTION;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int TOO_DEEP_RECURSION;
}

namespace AvroRust
{

Int32 check(Int32 status, const char * err)
{
    if (status >= 0)
        return status;

    const char * message = err ? err : "";
    switch (-status)
    {
        case AVRO_ERR_ENUM_INDEX_OUT_OF_RANGE:
        case AVRO_ERR_UNION_INDEX_OUT_OF_RANGE:
            throw Exception(ErrorCodes::INCORRECT_DATA, "{}", message);
        case AVRO_ERR_DATA_TOO_DEEP:
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "{}", message);
        default:
            throw Exception(ErrorCodes::AVRO_EXCEPTION, "avro::Exception: {}", message);
    }
}

const char * kindName(UInt8 kind)
{
    switch (kind)
    {
        case AVRO_KIND_NULL: return "null";
        case AVRO_KIND_BOOLEAN: return "boolean";
        case AVRO_KIND_INT: return "int";
        case AVRO_KIND_LONG: return "long";
        case AVRO_KIND_FLOAT: return "float";
        case AVRO_KIND_DOUBLE: return "double";
        case AVRO_KIND_BYTES: return "bytes";
        case AVRO_KIND_STRING: return "string";
        case AVRO_KIND_RECORD: return "record";
        case AVRO_KIND_ENUM: return "enum";
        case AVRO_KIND_ARRAY: return "array";
        case AVRO_KIND_MAP: return "map";
        case AVRO_KIND_UNION: return "union";
        case AVRO_KIND_FIXED: return "fixed";
        default: return "unknown";
    }
}

Schema::Schema(std::string_view json, UInt32 max_depth)
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    AvroSchema * parsed = nullptr;
    check(avro_schema_parse(reinterpret_cast<const uint8_t *>(json.data()), json.size(), max_depth, &parsed, err, sizeof(err)), err);

    schema = parsed;
    root_id = avro_schema_root(schema);
    node_count = avro_schema_node_count(schema);
}

Schema::~Schema()
{
    avro_schema_free(schema);
}

Node Schema::node(UInt32 id) const
{
    if (id >= node_count)
        throw Exception(ErrorCodes::AVRO_EXCEPTION, "avro::Exception: Avro schema node {} is out of range, the schema has {} nodes", id, node_count);
    return Node(*this, id);
}

std::string_view Schema::canonical() const
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    const char * out = nullptr;
    size_t len = 0;
    check(avro_schema_canonical(schema, &out, &len, err, sizeof(err)), err);
    return {out, len};
}

std::string Schema::canonicalPrefix(size_t max_bytes) const
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    std::string result(max_bytes, '\0');
    size_t len = 0;
    uint8_t truncated = 0;
    check(avro_schema_canonical_prefix(schema, result.data(), result.size(), &len, &truncated, err, sizeof(err)), err);
    result.resize(len);
    if (truncated)
        result += "...";
    return result;
}

std::string_view Schema::source() const
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    const uint8_t * out = nullptr;
    size_t len = 0;
    check(avro_schema_source(schema, &out, &len, err, sizeof(err)), err);
    return {reinterpret_cast<const char *>(out), len};
}

AvroNodeInfo Node::info() const
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    AvroNodeInfo result{};
    check(avro_schema_node(schema->get(), node_id, &result, err, sizeof(err)), err);
    return result;
}

Node Node::child(UInt32 i) const
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    UInt32 child_id = 0;
    check(avro_schema_child(schema->get(), node_id, i, &child_id, nullptr, nullptr, err, sizeof(err)), err);
    return Node(*schema, child_id);
}

std::string_view Node::fieldName(UInt32 i) const
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    UInt32 child_id = 0;
    const char * field_name = nullptr;
    size_t field_name_len = 0;
    check(avro_schema_child(schema->get(), node_id, i, &child_id, &field_name, &field_name_len, err, sizeof(err)), err);
    if (!field_name)
        return {};
    return {field_name, field_name_len};
}

std::string_view Node::symbol(UInt32 i) const
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    const char * symbol_data = nullptr;
    size_t symbol_len = 0;
    check(avro_schema_symbol(schema->get(), node_id, i, &symbol_data, &symbol_len, err, sizeof(err)), err);
    if (!symbol_data)
        return {};
    return {symbol_data, symbol_len};
}

std::string_view Node::name() const
{
    const AvroNodeInfo node_info = info();
    if (!node_info.name)
        return {};
    return {node_info.name, node_info.name_len};
}

std::string_view Node::simpleName() const
{
    const AvroNodeInfo node_info = info();
    if (!node_info.name)
        return {};
    std::string_view full_name(node_info.name, node_info.name_len);
    full_name.remove_prefix(std::min<size_t>(node_info.simple_name_offset, full_name.size()));
    return full_name;
}

std::optional<AvroAttr> Node::attr(std::string_view key, Int64 field) const
{
    char err[ERROR_BUFFER_SIZE];
    err[0] = '\0';

    AvroAttr out{};
    const Int32 status
        = check(avro_schema_attr(schema->get(), node_id, field, reinterpret_cast<const uint8_t *>(key.data()), key.size(), &out, err, sizeof(err)), err);
    if (status == AVRO_NOT_FOUND)
        return std::nullopt;
    return out;
}

namespace
{

/// The legacy library named a node by its simple name, or by the type name if the node has none.
std::string nodeDescription(Node node, const AvroNodeInfo & info)
{
    if (info.name)
        return std::string(node.simpleName());
    return kindName(info.kind);
}

/// Named types are followed by reference, so the schema JSON being shallow (`max_depth` of `Schema`) does not bound the
/// recursion: only `checkStackSize` does, as in the legacy `avroNodeToDataTypeImpl`.
DataTypePtr inferTypeImpl(Node node, UnorderedSetWithMemoryTracking<UInt32> & records_in_progress, bool allow_nullable_tuple_type)
{
    checkStackSize();

    const AvroNodeInfo info = node.info();
    switch (static_cast<AvroKind>(info.kind))
    {
        case AVRO_KIND_INT:
        {
            if (info.logical == AVRO_LOGICAL_DATE)
                return std::make_shared<DataTypeDate32>();

            return std::make_shared<DataTypeInt32>();
        }
        case AVRO_KIND_LONG:
        {
            if (info.logical == AVRO_LOGICAL_TIMESTAMP_MILLIS)
                return std::make_shared<DataTypeDateTime64>(3);
            if (info.logical == AVRO_LOGICAL_TIMESTAMP_MICROS)
                return std::make_shared<DataTypeDateTime64>(6);

            return std::make_shared<DataTypeInt64>();
        }
        case AVRO_KIND_BOOLEAN:
            return DataTypeFactory::instance().get("Bool");
        case AVRO_KIND_FLOAT:
            return std::make_shared<DataTypeFloat32>();
        case AVRO_KIND_DOUBLE:
            return std::make_shared<DataTypeFloat64>();
        case AVRO_KIND_STRING:
        case AVRO_KIND_BYTES:
        {
            if (info.logical == AVRO_LOGICAL_UUID)
                return std::make_shared<DataTypeUUID>();

            if (info.logical == AVRO_LOGICAL_DECIMAL)
                return createDecimal<DataTypeDecimal>(info.precision, info.scale);

            return std::make_shared<DataTypeString>();
        }
        case AVRO_KIND_ENUM:
        {
            if (info.symbol_count < 128)
            {
                EnumValues<Int8>::Values values;
                values.reserve(info.symbol_count);
                for (UInt32 i = 0; i != info.symbol_count; ++i)
                    values.emplace_back(std::string(node.symbol(i)), static_cast<Int8>(i));
                return std::make_shared<DataTypeEnum8>(std::move(values));
            }
            if (info.symbol_count < 32768)
            {
                EnumValues<Int16>::Values values;
                values.reserve(info.symbol_count);
                for (UInt32 i = 0; i != info.symbol_count; ++i)
                    values.emplace_back(std::string(node.symbol(i)), static_cast<Int16>(i));
                return std::make_shared<DataTypeEnum16>(std::move(values));
            }

            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ClickHouse supports only 8 and 16-bit Enum.");
        }
        case AVRO_KIND_FIXED:
        {
            if (info.logical == AVRO_LOGICAL_UUID)
                return std::make_shared<DataTypeUUID>();

            if (info.logical == AVRO_LOGICAL_DECIMAL)
                return createDecimal<DataTypeDecimal>(info.precision, info.scale);

            return std::make_shared<DataTypeFixedString>(info.fixed_size);
        }
        case AVRO_KIND_ARRAY:
            return std::make_shared<DataTypeArray>(inferTypeImpl(node.child(0), records_in_progress, allow_nullable_tuple_type));
        case AVRO_KIND_NULL:
            return std::make_shared<DataTypeNothing>();
        case AVRO_KIND_UNION:
        {
            /// Treat union[T] as just T.
            if (info.child_count == 1)
                return inferTypeImpl(node.child(0), records_in_progress, allow_nullable_tuple_type);

            /// Treat union[T, NULL] and union[NULL, T] as Nullable(T).
            if (info.child_count == 2)
            {
                const Node first = node.child(0);
                const Node second = node.child(1);
                const bool first_is_null = first.info().kind == AVRO_KIND_NULL;
                if (first_is_null || second.info().kind == AVRO_KIND_NULL)
                {
                    auto nested_type = inferTypeImpl(first_is_null ? second : first, records_in_progress, allow_nullable_tuple_type);
                    if (isTuple(nested_type) && !allow_nullable_tuple_type)
                        return nested_type;
                    return nested_type->canBeInsideNullable() ? makeNullable(nested_type) : nested_type;
                }
            }

            /// Treat union[T1, T2, ...] as Variant(T1, T2).
            DataTypes nested_types;
            nested_types.reserve(info.child_count);

            for (UInt32 i = 0; i != info.child_count; ++i)
            {
                const Node branch = node.child(i);
                /// We skip the null union type in Variant, since it is encoded using the null discriminator (implicitly all Variants can "contain null").
                if (branch.info().kind == AVRO_KIND_NULL)
                    continue;

                nested_types.push_back(inferTypeImpl(branch, records_in_progress, allow_nullable_tuple_type));
            }
            return std::make_shared<DataTypeVariant>(nested_types);
        }
        case AVRO_KIND_RECORD:
        {
            /// A named type referenced by name is the same node as its definition, so a record that is
            /// reached again while its fields are being inferred is recursive.
            if (!records_in_progress.insert(node.id()).second)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Recursive Avro schema is not supported: type '{}' references itself",
                    node.name());

            DataTypes nested_types;
            nested_types.reserve(info.child_count);
            Names nested_names;
            nested_names.reserve(info.child_count);
            for (UInt32 i = 0; i != info.child_count; ++i)
            {
                nested_types.push_back(inferTypeImpl(node.child(i), records_in_progress, allow_nullable_tuple_type));
                nested_names.emplace_back(node.fieldName(i));
            }

            records_in_progress.erase(node.id());
            return std::make_shared<DataTypeTuple>(nested_types, nested_names);
        }
        case AVRO_KIND_MAP:
            /// Avro map keys are always strings.
            return std::make_shared<DataTypeMap>(
                std::make_shared<DataTypeString>(), inferTypeImpl(node.child(0), records_in_progress, allow_nullable_tuple_type));
    }

    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Avro column {} is not supported for inserting.", nodeDescription(node, info));
}

}

DataTypePtr inferType(Node node, bool allow_nullable_tuple_type)
{
    UnorderedSetWithMemoryTracking<UInt32> records_in_progress;
    return inferTypeImpl(node, records_in_progress, allow_nullable_tuple_type);
}

}

}

#endif
