#include <Processors/Formats/Impl/ORCBlockOutputFormat.h>

#include <unordered_map>

#include <fmt/ranges.h>

#if USE_ORC

#include <Common/assert_cast.h>
#include <Formats/FormatFactory.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnsCommon.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/NestedUtils.h>

#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

ORC::CompressionKind getORCCompression(FormatSettings::ORCCompression method)
{
    if (method == FormatSettings::ORCCompression::NONE)
        return ORC::CompressionKind::NONE;

#if USE_SNAPPY
    if (method == FormatSettings::ORCCompression::SNAPPY)
        return ORC::CompressionKind::SNAPPY;
#endif

    if (method == FormatSettings::ORCCompression::ZSTD)
        return ORC::CompressionKind::ZSTD;

    if (method == FormatSettings::ORCCompression::LZ4)
        return ORC::CompressionKind::LZ4;

    if (method == FormatSettings::ORCCompression::ZLIB)
        return ORC::CompressionKind::ZLIB;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported compression method");
}

/// Two ORC types get the same key exactly when `parseORCType` (NativeORCBlockInputFormat.cpp) reads
/// them back as the same ClickHouse type: `BINARY` and `STRING` both read as `String`, and a nested
/// `UNION` reads as a `Variant`, which sorts its branches, while ORC keeps them positional.
/// A field name is length-prefixed so a name containing `:` or `,` cannot forge another key.
String orcTypeDedupKey(const ORC::Type & type)
{
    const auto kind = type.getKind();
    if (kind == ORC::TypeKind::BINARY || kind == ORC::TypeKind::STRING)
        return "string";
    if (kind == ORC::TypeKind::DECIMAL)
        return fmt::format("decimal({},{})", type.getPrecision(), type.getScale());

    std::vector<String> children;
    for (size_t i = 0; i < type.getSubtypeCount(); ++i)
    {
        auto child = orcTypeDedupKey(*type.getSubtype(i));
        if (kind == ORC::TypeKind::STRUCT)
            child = fmt::format("{}:{}:{}", type.getFieldName(i).size(), type.getFieldName(i), child);
        children.push_back(std::move(child));
    }
    if (kind == ORC::TypeKind::UNION)
        std::sort(children.begin(), children.end());
    /// Every remaining kind the writer emits is a parameterless primitive mapping to a distinct
    /// ClickHouse type, so for those the kind alone is the key and there are no children.
    return fmt::format("{}<{}>", static_cast<int>(kind), fmt::join(children, ","));
}

}

ORCOutputStream::ORCOutputStream(WriteBuffer & out_) : out(out_) {}

void ORCOutputStream::write(const void* buf, size_t length)
{
    out.write(static_cast<const char *>(buf), length);
}

ORCBlockOutputFormat::ORCBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_, ColumnMapperPtr column_mapper_)
    : IOutputFormat(header_, out_)
    , format_settings{format_settings_}
    , output_stream(out_)
    , column_mapper(std::move(column_mapper_))
{
    for (const auto & type : header_->getDataTypes())
        data_types.push_back(recursiveRemoveLowCardinality(type));
}

std::unique_ptr<ORC::Type> ORCBlockOutputFormat::getORCType(const DataTypePtr & type, const String & column_path)
{
    /// Nullable(T) maps to the same ORC node as T; only the Iceberg `required` flag differs.
    /// A complex container (list/map/struct) is never wrapped in Nullable, so its Iceberg
    /// optionality is not recoverable from the type: consult the per-path Iceberg metadata when
    /// present, else fall back to the type (also for non-Iceberg writes and field-id-only mappers).
    bool required = !type->isNullable();
    if (column_mapper && column_mapper->hasIcebergRequiredInfo())
        required = !column_mapper->isIcebergOptionalPath(column_path);
    const DataTypePtr unwrapped = removeNullable(type);

    std::unique_ptr<ORC::Type> result;
    switch (unwrapped->getTypeId())
    {
        case TypeIndex::UInt8:
        {
            if (isBool(unwrapped))
                result = ORC::Type::createPrimitiveType(ORC::TypeKind::BOOLEAN);
            else
                result = ORC::Type::createPrimitiveType(ORC::TypeKind::BYTE);
            break;
        }
        case TypeIndex::Enum8: [[fallthrough]];
        case TypeIndex::Int8:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::BYTE);
            break;
        }
        case TypeIndex::Enum16: [[fallthrough]];
        case TypeIndex::UInt16: [[fallthrough]];
        case TypeIndex::Int16:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::SHORT);
            break;
        }
        case TypeIndex::UInt32: [[fallthrough]];
        case TypeIndex::IPv4: [[fallthrough]];
        case TypeIndex::Int32:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::INT);
            break;
        }
        case TypeIndex::UInt64: [[fallthrough]];
        case TypeIndex::Int64:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::LONG);
            break;
        }
        case TypeIndex::Float32:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::FLOAT);
            break;
        }
        case TypeIndex::Float64:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::DOUBLE);
            break;
        }
        case TypeIndex::Date32: [[fallthrough]];
        case TypeIndex::Date:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::DATE);
            break;
        }
        case TypeIndex::DateTime: [[fallthrough]];
        case TypeIndex::DateTime64:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::TIMESTAMP);
            break;
        }
        case TypeIndex::Int128: [[fallthrough]];
        case TypeIndex::UInt128: [[fallthrough]];
        case TypeIndex::Int256: [[fallthrough]];
        case TypeIndex::UInt256: [[fallthrough]];
        case TypeIndex::Decimal256:
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::BINARY);
            break;
        case TypeIndex::String:
        {
            /// Iceberg `string` and `binary` both read as DataTypeString; force ORC `string` only
            /// for genuine `string` paths, else fall back to the setting (also for non-Iceberg writes).
            bool force_string = format_settings.orc.output_string_as_string;
            if (column_mapper && column_mapper->hasIcebergStringInfo())
                force_string = column_mapper->isIcebergStringPath(column_path);
            result = ORC::Type::createPrimitiveType(force_string ? ORC::TypeKind::STRING : ORC::TypeKind::BINARY);
            break;
        }
        case TypeIndex::FixedString:
        {
            /// Iceberg `fixed[L]` (FixedString) must stay ORC `binary`, so for Iceberg writes
            /// force `binary` regardless of the setting; keep the setting-driven choice otherwise.
            if (format_settings.orc.output_string_as_string && !column_mapper)
                result = ORC::Type::createPrimitiveType(ORC::TypeKind::STRING);
            else
                result = ORC::Type::createPrimitiveType(ORC::TypeKind::BINARY);
            break;
        }
        case TypeIndex::IPv6:
        {
            result = ORC::Type::createPrimitiveType(ORC::TypeKind::BINARY);
            break;
        }
        case TypeIndex::Array:
        {
            const auto * array_type = assert_cast<const DataTypeArray *>(unwrapped.get());
            result = ORC::Type::createListType(getORCType(array_type->getNestedType(), Nested::concatenateName(column_path, "element")));
            break;
        }
        case TypeIndex::Decimal32:
        {
            const auto * decimal_type = assert_cast<const DataTypeDecimal<Decimal32> *>(unwrapped.get());
            result = ORC::Type::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
            break;
        }
        case TypeIndex::Decimal64:
        {
            const auto * decimal_type = assert_cast<const DataTypeDecimal<Decimal64> *>(unwrapped.get());
            result = ORC::Type::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
            break;
        }
        case TypeIndex::Decimal128:
        {
            const auto * decimal_type = assert_cast<const DataTypeDecimal<Decimal128> *>(unwrapped.get());
            result = ORC::Type::createDecimalType(decimal_type->getPrecision(), decimal_type->getScale());
            break;
        }
        case TypeIndex::Tuple:
        {
            const auto * tuple_type = assert_cast<const DataTypeTuple *>(unwrapped.get());
            const auto & nested_names = tuple_type->getElementNames();
            const auto & nested_types = tuple_type->getElements();
            auto struct_type = ORC::Type::createStructType();
            for (size_t i = 0; i < nested_types.size(); ++i)
                struct_type->addStructField(nested_names[i], getORCType(nested_types[i], Nested::concatenateName(column_path, nested_names[i])));
            result = std::move(struct_type);
            break;
        }
        case TypeIndex::Map:
        {
            const auto * map_type = assert_cast<const DataTypeMap *>(unwrapped.get());
            result = ORC::Type::createMapType(
                getORCType(map_type->getKeyType(), Nested::concatenateName(column_path, "key")),
                getORCType(map_type->getValueType(), Nested::concatenateName(column_path, "value"))
                );
            break;
        }
        case TypeIndex::Variant:
        {
            const auto & variant_type = assert_cast<const DataTypeVariant &>(*unwrapped);
            auto union_type = ORC::Type::createUnionType();
            /// ORC keeps one physical stream per union branch and identifies a branch only by its
            /// ORC type, so two variants that map to the same ORC type (e.g. `Int32` and `UInt32`,
            /// both ORC `int`) would produce a union with duplicate branches, which the reader
            /// rejects. Branches whose ORC types merely read back as one ClickHouse type collide the
            /// same way, so they are keyed by `orcTypeDedupKey` rather than by the ORC type itself.
            std::unordered_map<String, std::pair<String, String>> variant_by_orc_type;
            /// A union child is addressed by its type name, the way a `Variant` subcolumn is
            /// (`v.Int32`). Iceberg has no union type, so no field id is expected to be found there.
            for (const auto & nested_type : variant_type.getVariants())
            {
                auto child_type = getORCType(nested_type, Nested::concatenateName(column_path, nested_type->getName()));
                auto [it, inserted] = variant_by_orc_type.emplace(
                    orcTypeDedupKey(*child_type), std::pair{nested_type->getName(), child_type->toString()});
                if (!inserted)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Type {} is not supported for ORC output format: variant {} is written as ORC type '{}' and variant {} as "
                        "'{}', which are read back as the same type, and ORC unions with duplicate branch types cannot be read back",
                        unwrapped->getName(), it->second.first, it->second.second, nested_type->getName(), child_type->toString());
                union_type->addChild(std::move(child_type));
            }
            result = std::move(union_type);
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} is not supported for ORC output format", unwrapped->getName());
        }
    }

    /// Attach Iceberg field-id attributes so spec-compliant readers project columns by id.
    if (column_mapper)
    {
        const auto & encoding = column_mapper->getStorageColumnEncoding();
        if (auto it = encoding.find(column_path); it != encoding.end())
        {
            result->setAttribute("iceberg.id", std::to_string(it->second));
            result->setAttribute("iceberg.required", required ? "true" : "false");
        }
    }

    return result;
}

void ORCBlockOutputFormat::consume(Chunk chunk)
{
    if (!writer)
        prepareWriter();

    size_t rows_num = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = recursiveRemoveLowCardinality(column);

    /// The values are encoded directly from the columns.
    writer->add(columns, rows_num);
}

void ORCBlockOutputFormat::finalizeImpl()
{
    if (!writer)
        prepareWriter();

    writer->close();
}

void ORCBlockOutputFormat::resetFormatterImpl()
{
    writer.reset();
}

void ORCBlockOutputFormat::prepareWriter()
{
    const Block & header = getPort(PortKind::Main).getHeader();
    schema = ORC::Type::createStructType();
    options.compression = getORCCompression(format_settings.orc.output_compression_method);
    options.row_index_stride = format_settings.orc.output_row_index_stride;
    options.dictionary_key_size_threshold = format_settings.orc.output_dictionary_key_size_threshold;
    options.compression_block_size = format_settings.orc.output_compression_block_size;
    options.timezone_name = format_settings.orc.writer_time_zone_name;
    size_t columns_count = header.columns();
    for (size_t i = 0; i != columns_count; ++i)
    {
        const auto & column_name = header.safeGetByPosition(i).name;
        schema->addStructField(column_name, getORCType(recursiveRemoveLowCardinality(data_types[i]), column_name));
    }
    writer = std::make_unique<ORC::Writer>(*schema, data_types, output_stream, options);
}

void registerOutputFormatORC(FormatFactory & factory);
void registerOutputFormatORC(FormatFactory & factory)
{
    factory.registerOutputFormat("ORC", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & format_settings,
            FormatFilterInfoPtr format_filter_info)
    {
        ColumnMapperPtr column_mapper = format_filter_info ? format_filter_info->column_mapper : nullptr;
        return std::make_shared<ORCBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings, column_mapper);
    });
    factory.markFormatHasNoAppendSupport("ORC");
    factory.markOutputFormatPrefersLargeBlocks("ORC");
    factory.markOutputFormatNotTTYFriendly("ORC");
    factory.setContentType("ORC", "application/octet-stream");
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerOutputFormatORC(FormatFactory &);
    void registerOutputFormatORC(FormatFactory &)
    {
    }
}

#endif
