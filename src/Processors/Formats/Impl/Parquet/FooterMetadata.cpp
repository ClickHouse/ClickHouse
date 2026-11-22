#include <Processors/Formats/Impl/Parquet/FooterMetadata.h>

#if USE_PARQUET

#include <Processors/Formats/Impl/Parquet/Prefetcher.h>
#include <Processors/Formats/Impl/Parquet/ReadCommon.h>
#include <Formats/FormatSettings.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/StringUtils.h>
#include <Common/checkStackSize.h>

#include <charconv>
#include <cmath>
#include <cstring>
#include <limits>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_DEEP_RECURSION;
}

namespace DB::Parquet
{

namespace
{

/// parquet-cpp's `LogicalType`, with just enough to print it and to derive the sort order.
struct LogicalType
{
    enum class Kind : UInt8
    {
        None,
        Undefined,
        String,
        Map,
        List,
        Enum,
        Decimal,
        Date,
        Time,
        Timestamp,
        Interval,
        Int,
        Null,
        JSON,
        BSON,
        UUID,
        Float16,
        Geometry,
        Geography,
        Variant,
    };

    Kind kind = Kind::None;
    Int32 precision = 0;
    Int32 scale = 0;
    bool is_adjusted_to_utc = false;
    /// 0 = milliseconds, 1 = microseconds, 2 = nanoseconds.
    int unit = 0;
    bool is_from_converted_type = false;
    int bit_width = 0;
    bool is_signed = false;
    String crs;
    /// `std::nullopt` for a value out of the range of `EdgeInterpolationAlgorithm`.
    std::optional<parq::EdgeInterpolationAlgorithm::type> algorithm;
    Int32 variant_specification_version = 1;

    [[nodiscard]] bool isNested() const { return kind == Kind::Map || kind == Kind::List || kind == Kind::Variant; }

    [[nodiscard]] String toString() const
    {
        static constexpr std::string_view units[] = {"milliseconds", "microseconds", "nanoseconds"};
        auto bool_string = [](bool value) { return value ? "true" : "false"; };
        switch (kind)
        {
            case Kind::None: return "None";
            case Kind::Undefined: return "Undefined";
            case Kind::String: return "String";
            case Kind::Map: return "Map";
            case Kind::List: return "List";
            case Kind::Enum: return "Enum";
            case Kind::Date: return "Date";
            case Kind::Interval: return "Interval";
            case Kind::Null: return "Null";
            case Kind::JSON: return "JSON";
            case Kind::BSON: return "BSON";
            case Kind::UUID: return "UUID";
            case Kind::Float16: return "Float16";
            case Kind::Decimal: return fmt::format("Decimal(precision={}, scale={})", precision, scale);
            case Kind::Time:
                return fmt::format("Time(isAdjustedToUTC={}, timeUnit={})", bool_string(is_adjusted_to_utc), units[unit]);
            case Kind::Timestamp:
                return fmt::format(
                    "Timestamp(isAdjustedToUTC={}, timeUnit={}, is_from_converted_type={}, force_set_converted_type=false)",
                    bool_string(is_adjusted_to_utc), units[unit], bool_string(is_from_converted_type));
            case Kind::Int: return fmt::format("Int(bitWidth={}, isSigned={})", bit_width, bool_string(is_signed));
            case Kind::Geometry: return fmt::format("Geometry(crs={})", crs);
            case Kind::Geography:
            {
                std::string_view algorithm_name = "unknown";
                if (algorithm)
                {
                    switch (*algorithm)
                    {
                        case parq::EdgeInterpolationAlgorithm::SPHERICAL: algorithm_name = "spherical"; break;
                        case parq::EdgeInterpolationAlgorithm::VINCENTY: algorithm_name = "vincenty"; break;
                        case parq::EdgeInterpolationAlgorithm::THOMAS: algorithm_name = "thomas"; break;
                        case parq::EdgeInterpolationAlgorithm::ANDOYER: algorithm_name = "andoyer"; break;
                        case parq::EdgeInterpolationAlgorithm::KARNEY: algorithm_name = "karney"; break;
                    }
                }
                return fmt::format("Geography(crs={}, algorithm={})", crs, algorithm_name);
            }
            case Kind::Variant: return fmt::format("Variant({})", variant_specification_version);
        }
    }
};

/// parquet-cpp's converted types are the Thrift ones shifted by one, with `NONE` in front.
enum class ConvertedType : int
{
    NONE = 0,
    UTF8 = 1,
    MAP = 2,
    MAP_KEY_VALUE = 3,
    LIST = 4,
    ENUM = 5,
    DECIMAL = 6,
    DATE = 7,
    TIME_MILLIS = 8,
    TIME_MICROS = 9,
    TIMESTAMP_MILLIS = 10,
    TIMESTAMP_MICROS = 11,
    UINT_8 = 12,
    UINT_16 = 13,
    UINT_32 = 14,
    UINT_64 = 15,
    INT_8 = 16,
    INT_16 = 17,
    INT_32 = 18,
    INT_64 = 19,
    JSON = 20,
    BSON = 21,
    INTERVAL = 22,
    NA = 25,
    UNDEFINED = 26,
};

ConvertedType loadConvertedType(const parq::SchemaElement & element)
{
    if (!element.__isset.converted_type)
        return ConvertedType::NONE;
    const int raw = thriftEnumToInt(element.converted_type);
    if (raw < 0 || raw >= static_cast<int>(ConvertedType::UNDEFINED))
        return ConvertedType::UNDEFINED;
    return static_cast<ConvertedType>(raw + 1);
}

std::string_view convertedTypeName(ConvertedType type)
{
    switch (type)
    {
        case ConvertedType::NONE: return "NONE";
        case ConvertedType::UTF8: return "UTF8";
        case ConvertedType::MAP: return "MAP";
        case ConvertedType::MAP_KEY_VALUE: return "MAP_KEY_VALUE";
        case ConvertedType::LIST: return "LIST";
        case ConvertedType::ENUM: return "ENUM";
        case ConvertedType::DECIMAL: return "DECIMAL";
        case ConvertedType::DATE: return "DATE";
        case ConvertedType::TIME_MILLIS: return "TIME_MILLIS";
        case ConvertedType::TIME_MICROS: return "TIME_MICROS";
        case ConvertedType::TIMESTAMP_MILLIS: return "TIMESTAMP_MILLIS";
        case ConvertedType::TIMESTAMP_MICROS: return "TIMESTAMP_MICROS";
        case ConvertedType::UINT_8: return "UINT_8";
        case ConvertedType::UINT_16: return "UINT_16";
        case ConvertedType::UINT_32: return "UINT_32";
        case ConvertedType::UINT_64: return "UINT_64";
        case ConvertedType::INT_8: return "INT_8";
        case ConvertedType::INT_16: return "INT_16";
        case ConvertedType::INT_32: return "INT_32";
        case ConvertedType::INT_64: return "INT_64";
        case ConvertedType::JSON: return "JSON";
        case ConvertedType::BSON: return "BSON";
        case ConvertedType::INTERVAL: return "INTERVAL";
        case ConvertedType::NA: return "NA";
        default: return "UNKNOWN";
    }
}

/// The enums of a footer are read with `thriftEnumToInt`, never directly: Thrift stores the raw integer of
/// a malformed file into the enum field, and loading a value out of the range of the enum is undefined behavior.
std::optional<parq::Type::type> loadPhysicalType(const parq::Type::type & type)
{
    const int raw = thriftEnumToInt(type);
    if (raw < parq::Type::BOOLEAN || raw > parq::Type::FIXED_LEN_BYTE_ARRAY)
        return std::nullopt;
    return static_cast<parq::Type::type>(raw);
}

std::string_view physicalTypeName(std::optional<parq::Type::type> type)
{
    if (!type)
        return "UNKNOWN";
    return parq::_Type_VALUES_TO_NAMES.at(*type);
}

[[noreturn]] void throwMalformed(const String & message)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Malformed Parquet metadata: {}", message);
}

LogicalType fromConvertedType(ConvertedType converted_type, Int32 precision, Int32 scale)
{
    LogicalType result;
    using Kind = LogicalType::Kind;
    switch (converted_type)
    {
        case ConvertedType::UTF8: result.kind = Kind::String; break;
        case ConvertedType::MAP:
        case ConvertedType::MAP_KEY_VALUE: result.kind = Kind::Map; break;
        case ConvertedType::LIST: result.kind = Kind::List; break;
        case ConvertedType::ENUM: result.kind = Kind::Enum; break;
        case ConvertedType::DECIMAL:
            if (precision < 1)
                throwMalformed("Precision must be greater than or equal to 1 for Decimal logical type");
            if (scale < 0 || scale > precision)
                throwMalformed("Scale must be a non-negative integer that does not exceed precision for Decimal logical type");
            result.kind = Kind::Decimal;
            result.precision = precision;
            result.scale = scale;
            break;
        case ConvertedType::DATE: result.kind = Kind::Date; break;
        case ConvertedType::TIME_MILLIS:
        case ConvertedType::TIME_MICROS:
            result.kind = Kind::Time;
            result.is_adjusted_to_utc = true;
            result.unit = converted_type == ConvertedType::TIME_MILLIS ? 0 : 1;
            break;
        case ConvertedType::TIMESTAMP_MILLIS:
        case ConvertedType::TIMESTAMP_MICROS:
            result.kind = Kind::Timestamp;
            result.is_adjusted_to_utc = true;
            result.unit = converted_type == ConvertedType::TIMESTAMP_MILLIS ? 0 : 1;
            result.is_from_converted_type = true;
            break;
        case ConvertedType::INTERVAL: result.kind = Kind::Interval; break;
        case ConvertedType::INT_8: result.kind = Kind::Int; result.bit_width = 8; result.is_signed = true; break;
        case ConvertedType::INT_16: result.kind = Kind::Int; result.bit_width = 16; result.is_signed = true; break;
        case ConvertedType::INT_32: result.kind = Kind::Int; result.bit_width = 32; result.is_signed = true; break;
        case ConvertedType::INT_64: result.kind = Kind::Int; result.bit_width = 64; result.is_signed = true; break;
        case ConvertedType::UINT_8: result.kind = Kind::Int; result.bit_width = 8; break;
        case ConvertedType::UINT_16: result.kind = Kind::Int; result.bit_width = 16; break;
        case ConvertedType::UINT_32: result.kind = Kind::Int; result.bit_width = 32; break;
        case ConvertedType::UINT_64: result.kind = Kind::Int; result.bit_width = 64; break;
        case ConvertedType::JSON: result.kind = Kind::JSON; break;
        case ConvertedType::BSON: result.kind = Kind::BSON; break;
        case ConvertedType::NA: result.kind = Kind::Null; break;
        case ConvertedType::NONE: result.kind = Kind::None; break;
        default: result.kind = Kind::Undefined; break;
    }
    return result;
}

int timeUnit(const parq::TimeUnit & unit, std::string_view what)
{
    if (unit.__isset.MILLIS)
        return 0;
    if (unit.__isset.MICROS)
        return 1;
    if (unit.__isset.NANOS)
        return 2;
    throwMalformed(fmt::format("TimeUnit must be one of MILLIS, MICROS, or NANOS for {} logical type", what));
}

LogicalType fromThrift(const parq::LogicalType & type)
{
    LogicalType result;
    using Kind = LogicalType::Kind;
    const auto & isset = type.__isset;
    if (isset.STRING)
        result.kind = Kind::String;
    else if (isset.MAP)
        result.kind = Kind::Map;
    else if (isset.LIST)
        result.kind = Kind::List;
    else if (isset.ENUM)
        result.kind = Kind::Enum;
    else if (isset.DECIMAL)
    {
        result = fromConvertedType(ConvertedType::DECIMAL, type.DECIMAL.precision, type.DECIMAL.scale);
    }
    else if (isset.DATE)
        result.kind = Kind::Date;
    else if (isset.TIME)
    {
        result.kind = Kind::Time;
        result.is_adjusted_to_utc = type.TIME.isAdjustedToUTC;
        result.unit = timeUnit(type.TIME.unit, "Time");
    }
    else if (isset.TIMESTAMP)
    {
        result.kind = Kind::Timestamp;
        result.is_adjusted_to_utc = type.TIMESTAMP.isAdjustedToUTC;
        result.unit = timeUnit(type.TIMESTAMP.unit, "Timestamp");
    }
    else if (isset.INTEGER)
    {
        /// A negative width, read as unsigned, is as invalid as it was.
        const int bit_width = static_cast<UInt8>(type.INTEGER.bitWidth);
        if (bit_width != 8 && bit_width != 16 && bit_width != 32 && bit_width != 64)
            throwMalformed("Bit width must be exactly 8, 16, 32, or 64 for Int logical type");
        result.kind = Kind::Int;
        result.bit_width = bit_width;
        result.is_signed = type.INTEGER.isSigned;
    }
    else if (isset.UNKNOWN)
        result.kind = Kind::Null;
    else if (isset.JSON)
        result.kind = Kind::JSON;
    else if (isset.BSON)
        result.kind = Kind::BSON;
    else if (isset.UUID)
        result.kind = Kind::UUID;
    else if (isset.FLOAT16)
        result.kind = Kind::Float16;
    else if (isset.GEOMETRY)
    {
        result.kind = Kind::Geometry;
        if (type.GEOMETRY.__isset.crs)
            result.crs = type.GEOMETRY.crs;
    }
    else if (isset.GEOGRAPHY)
    {
        result.kind = Kind::Geography;
        if (type.GEOGRAPHY.__isset.crs)
            result.crs = type.GEOGRAPHY.crs;
        result.algorithm = parq::EdgeInterpolationAlgorithm::SPHERICAL;
        if (type.GEOGRAPHY.__isset.algorithm)
        {
            const int raw = thriftEnumToInt(type.GEOGRAPHY.algorithm);
            if (raw < parq::EdgeInterpolationAlgorithm::SPHERICAL || raw > parq::EdgeInterpolationAlgorithm::KARNEY)
                result.algorithm.reset();
            else
                result.algorithm = static_cast<parq::EdgeInterpolationAlgorithm::type>(raw);
        }
    }
    else if (isset.VARIANT)
    {
        result.kind = Kind::Variant;
        if (type.VARIANT.__isset.specification_version)
            result.variant_specification_version = type.VARIANT.specification_version; /// NOLINT(bugprone-signed-char-misuse,cert-str34-c): a number, not a character
    }
    else
        result.kind = Kind::Undefined;
    return result;
}

/// Whether a logical type annotates a physical type, as `LogicalType::is_applicable` decides.
bool isApplicable(const LogicalType & type, std::optional<parq::Type::type> physical_type, Int32 type_length)
{
    using Kind = LogicalType::Kind;
    switch (type.kind)
    {
        case Kind::String:
        case Kind::Enum:
        case Kind::JSON:
        case Kind::BSON:
        case Kind::Geometry:
        case Kind::Geography:
            return physical_type == parq::Type::BYTE_ARRAY;
        case Kind::Date:
            return physical_type == parq::Type::INT32;
        case Kind::Time:
            return (physical_type == parq::Type::INT32 && type.unit == 0)
                || (physical_type == parq::Type::INT64 && type.unit != 0);
        case Kind::Timestamp:
            return physical_type == parq::Type::INT64;
        case Kind::Int:
            return (physical_type == parq::Type::INT32 && type.bit_width <= 32)
                || (physical_type == parq::Type::INT64 && type.bit_width == 64);
        case Kind::Decimal:
            if (physical_type == parq::Type::INT32)
                return type.precision >= 1 && type.precision <= 9;
            if (physical_type == parq::Type::INT64)
                return type.precision >= 1 && type.precision <= 18;
            if (physical_type == parq::Type::FIXED_LEN_BYTE_ARRAY)
            {
                if (type_length < 1 || type_length > 891723282)
                    return false;
                return type.precision <= static_cast<Int32>(std::floor(std::log10(2) * (8.0 * type_length - 1)));
            }
            return physical_type == parq::Type::BYTE_ARRAY;
        case Kind::UUID:
            return physical_type == parq::Type::FIXED_LEN_BYTE_ARRAY && type_length == 16;
        case Kind::Float16:
            return physical_type == parq::Type::FIXED_LEN_BYTE_ARRAY && type_length == 2;
        case Kind::Null:
        case Kind::None:
        case Kind::Undefined:
            return true;
        case Kind::Interval:
            return physical_type == parq::Type::FIXED_LEN_BYTE_ARRAY && type_length == 12;
        case Kind::Map:
        case Kind::List:
        case Kind::Variant:
            return false;
    }
}

/// Checks a converted type on a leaf the way parquet-cpp's legacy `PrimitiveNode` constructor does.
void checkConvertedType(ConvertedType converted_type, std::optional<parq::Type::type> physical_type, const parq::SchemaElement & element)
{
    const auto name = convertedTypeName(converted_type);
    switch (converted_type)
    {
        case ConvertedType::NONE:
        case ConvertedType::NA:
            return;
        case ConvertedType::UTF8:
        case ConvertedType::JSON:
        case ConvertedType::BSON:
        case ConvertedType::ENUM:
            if (physical_type != parq::Type::BYTE_ARRAY)
                throwMalformed(fmt::format("{} can only annotate BYTE_ARRAY fields", name));
            return;
        case ConvertedType::DECIMAL:
            if (physical_type != parq::Type::INT32 && physical_type != parq::Type::INT64 && physical_type != parq::Type::BYTE_ARRAY
                && physical_type != parq::Type::FIXED_LEN_BYTE_ARRAY)
                throwMalformed("DECIMAL can only annotate INT32, INT64, BYTE_ARRAY, and FIXED");
            if (element.precision <= 0)
                throwMalformed(fmt::format("Invalid DECIMAL precision: {}. Precision must be a number between 1 and 38 inclusive", element.precision));
            if (element.scale < 0)
                throwMalformed(fmt::format("Invalid DECIMAL scale: {}. Scale must be a number between 0 and precision inclusive", element.scale));
            if (element.scale > element.precision)
                throwMalformed(fmt::format("Invalid DECIMAL scale {} cannot be greater than precision {}", element.scale, element.precision));
            return;
        case ConvertedType::DATE:
        case ConvertedType::TIME_MILLIS:
        case ConvertedType::UINT_8:
        case ConvertedType::UINT_16:
        case ConvertedType::UINT_32:
        case ConvertedType::INT_8:
        case ConvertedType::INT_16:
        case ConvertedType::INT_32:
            if (physical_type != parq::Type::INT32)
                throwMalformed(fmt::format("{} can only annotate INT32", name));
            return;
        case ConvertedType::TIME_MICROS:
        case ConvertedType::TIMESTAMP_MILLIS:
        case ConvertedType::TIMESTAMP_MICROS:
        case ConvertedType::UINT_64:
        case ConvertedType::INT_64:
            if (physical_type != parq::Type::INT64)
                throwMalformed(fmt::format("{} can only annotate INT64", name));
            return;
        case ConvertedType::INTERVAL:
            if (physical_type != parq::Type::FIXED_LEN_BYTE_ARRAY || element.type_length != 12)
                throwMalformed("INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)");
            return;
        default:
            throwMalformed(fmt::format("{} cannot be applied to a primitive type", name));
    }
}

FooterMetadata::SortOrder sortOrder(const LogicalType & type, std::optional<parq::Type::type> physical_type)
{
    using Kind = LogicalType::Kind;
    using SortOrder = FooterMetadata::SortOrder;
    switch (type.kind)
    {
        case Kind::Undefined:
            return SortOrder::Unknown;
        case Kind::None:
            if (!physical_type)
                return SortOrder::Unknown;
            switch (*physical_type)
            {
                case parq::Type::BOOLEAN:
                case parq::Type::INT32:
                case parq::Type::INT64:
                case parq::Type::FLOAT:
                case parq::Type::DOUBLE:
                    return SortOrder::Signed;
                case parq::Type::BYTE_ARRAY:
                case parq::Type::FIXED_LEN_BYTE_ARRAY:
                    return SortOrder::Unsigned;
                case parq::Type::INT96:
                    return SortOrder::Unknown;
            }
            return SortOrder::Unknown;
        case Kind::Decimal:
        case Kind::Date:
        case Kind::Time:
        case Kind::Timestamp:
        case Kind::Float16:
            return SortOrder::Signed;
        case Kind::Int:
            return type.is_signed ? SortOrder::Signed : SortOrder::Unsigned;
        case Kind::String:
        case Kind::Enum:
        case Kind::JSON:
        case Kind::BSON:
        case Kind::UUID:
            return SortOrder::Unsigned;
        case Kind::Map:
        case Kind::List:
        case Kind::Interval:
        case Kind::Null:
        case Kind::Geometry:
        case Kind::Geography:
        case Kind::Variant:
            return SortOrder::Unknown;
    }
}

/// Validates a group node as parquet-cpp's `GroupNode::FromParquet` does.
void checkGroup(const parq::SchemaElement & element)
{
    if (element.__isset.logicalType)
    {
        const auto type = fromThrift(element.logicalType);
        if (!type.isNested())
            throwMalformed(fmt::format("Logical type {} cannot be applied to group node", type.toString()));
        return;
    }
    const auto type = fromConvertedType(loadConvertedType(element), element.precision, element.scale);
    if (!type.isNested() && type.kind != LogicalType::Kind::None)
        throwMalformed(fmt::format("Invalid logical type: {}", type.toString()));
}

/// The leaf columns of a schema, in the pre-order of the flattened schema list.
class SchemaWalker
{
public:
    SchemaWalker(const parq::FileMetaData & metadata_, VectorWithMemoryTracking<FooterMetadata::Column> & columns_, size_t max_depth_)
        : schema(metadata_.schema), columns(columns_), max_depth(max_depth_)
    {
    }

    void walk()
    {
        if (schema.empty())
            throwMalformed("Empty file schema (no root)");
        const auto & root = schema[0];
        if (root.num_children == 0)
        {
            if (schema.size() > 1)
                throwMalformed("Parquet schema had multiple nodes but root had no children");
            return;
        }
        checkGroup(root);
        pos = 1;
        for (Int32 i = 0; i < root.num_children; ++i)
            visit(0, 0, 1);
    }

private:
    void visit(Int16 definition_level, Int16 repetition_level, size_t depth)
    {
        /// The schema is a flat list in the footer, so nothing has limited its nesting so far, and a chain of
        /// `REQUIRED` groups does not even increase the levels. The limit is the one of the native Parquet reader
        /// (`max_parser_depth`, 0 means unlimited), with `checkStackSize` as the backstop.
        checkStackSize();
        if (max_depth != 0 && depth > max_depth)
            throw Exception(
                ErrorCodes::TOO_DEEP_RECURSION,
                "Parquet schema is nested deeper than the limit ({}). It can be raised with the setting "
                "'max_parser_depth', but a very deeply nested schema is rarely intentional",
                max_depth);

        if (pos >= schema.size())
            throwMalformed("Malformed schema: not enough elements");
        const auto & element = schema[pos++];

        /// An unset repetition reads as 0, that is REQUIRED, and so does a value out of the range of the enum.
        const int repetition_type = thriftEnumToInt(element.repetition_type);
        const bool is_repeated = repetition_type == parq::FieldRepetitionType::REPEATED;
        if (is_repeated || repetition_type == parq::FieldRepetitionType::OPTIONAL)
        {
            if (definition_level == std::numeric_limits<Int16>::max())
                throwMalformed("Too many nested optional or repeated fields");
            ++definition_level;
            if (is_repeated)
                ++repetition_level;
        }

        /// One path for the whole walk, so that a deep schema does not hold a copy of the path at every level.
        const size_t parent_path_size = path.size();
        if (parent_path_size != 0)
            path += '.';
        path += element.name;

        if (element.num_children == 0 && element.__isset.type)
        {
            addLeaf(element, definition_level, repetition_level);
        }
        else
        {
            checkGroup(element);
            for (Int32 i = 0; i < element.num_children; ++i)
                visit(definition_level, repetition_level, depth + 1);
        }
        path.resize(parent_path_size);
    }

    void addLeaf(const parq::SchemaElement & element, Int16 definition_level, Int16 repetition_level)
    {
        FooterMetadata::Column column;
        column.name = element.name;
        column.path = path;
        column.max_definition_level = definition_level;
        column.max_repetition_level = repetition_level;
        column.physical_type = loadPhysicalType(element.type);
        column.type_length = element.type_length;

        LogicalType logical_type;
        if (element.__isset.logicalType)
        {
            logical_type = fromThrift(element.logicalType);
            if (logical_type.isNested())
                throwMalformed(fmt::format("Nested logical type {} cannot be applied to non-group node", logical_type.toString()));
            if (!isApplicable(logical_type, column.physical_type, element.type_length))
                throwMalformed(fmt::format("{} cannot be applied to primitive type {}", logical_type.toString(), physicalTypeName(column.physical_type)));
        }
        else if (element.__isset.converted_type)
        {
            const auto converted_type = loadConvertedType(element);
            checkConvertedType(converted_type, column.physical_type, element);
            logical_type = fromConvertedType(converted_type, element.precision, element.scale);
        }

        if (column.physical_type == parq::Type::FIXED_LEN_BYTE_ARRAY && element.type_length <= 0)
            throwMalformed(fmt::format("Invalid FIXED_LEN_BYTE_ARRAY length: {}", element.type_length));

        column.logical_type = logical_type.toString();
        column.sort_order = sortOrder(logical_type, column.physical_type);
        columns.push_back(std::move(column));
    }

    const decltype(parq::FileMetaData::schema) & schema;
    VectorWithMemoryTracking<FooterMetadata::Column> & columns;
    const size_t max_depth;
    size_t pos = 0;
    String path;
};

}

FooterMetadata FooterMetadata::read(ReadBuffer & in, const FormatSettings & format_settings)
{
    ReadOptions options;
    options.format = format_settings;
    Prefetcher prefetcher;
    prefetcher.init(&in, options, /* parser_shared_resources_ */ nullptr);

    const size_t file_size = prefetcher.getFileSize();
    if (file_size < 8)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet file size is {} bytes, smaller than the minimum file footer (8 bytes)", file_size);

    char trailer[8];
    prefetcher.readSync(trailer, 8, file_size - 8);
    if (memcmp(trailer + 4, "PARE", 4) == 0)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parquet files with an encrypted footer are not supported");
    if (memcmp(trailer + 4, "PAR1", 4) != 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Parquet magic bytes not found in footer. Either the file is corrupted or this is not a parquet file");

    UInt32 metadata_length = 0;
    memcpy(&metadata_length, trailer, sizeof(metadata_length));
    if (metadata_length > file_size - 8)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Parquet file size is {} bytes, smaller than the size reported by the footer ({} bytes)",
            file_size, metadata_length);

    PODArray<char> serialized(metadata_length);
    prefetcher.readSync(serialized.data(), metadata_length, file_size - 8 - metadata_length);
    return parse(serialized.data(), serialized.size(), format_settings.max_parser_depth);
}

FooterMetadata FooterMetadata::parse(const char * data, size_t size, size_t max_schema_depth)
{
    FooterMetadata result;
    /// Like parquet-cpp, the size is the part of the footer that the metadata occupies: the signature of a
    /// plaintext footer of an encrypted file follows it.
    result.metadata_size = deserializeThriftStruct(result.file_metadata, data, size);
    result.initialize(max_schema_depth);
    return result;
}

void FooterMetadata::initialize(size_t max_schema_depth)
{
    SchemaWalker(file_metadata, leaf_columns, max_schema_depth).walk();

    if (file_metadata.__isset.column_orders)
    {
        if (file_metadata.column_orders.size() != leaf_columns.size())
            throwMalformed("Malformed schema: not enough ColumnOrder values");
        for (const auto & order : file_metadata.column_orders)
            column_orders.push_back(order.__isset.TYPE_ORDER ? ColumnOrder::TypeDefined : ColumnOrder::Unknown);
    }
    else
        column_orders.assign(leaf_columns.size(), ColumnOrder::Undefined);

    /// The writer version, parsed the way parquet-cpp's `ApplicationVersion` parses `created_by`, e.g.
    /// "parquet-mr version 1.8.0 (build abcd)". Fields parsed before an error are kept.
    if (!file_metadata.__isset.created_by)
        return;
    const std::string_view created_by = file_metadata.created_by;
    static constexpr std::string_view spaces = " \t\v\r\n\f";
    auto trim = [](std::string_view s)
    {
        size_t start = 0;
        while (start < s.size() && spaces.contains(s[start]))
            ++start;
        size_t end = s.size();
        /// parquet-cpp never strips the last remaining character.
        while (start + 1 < end && spaces.contains(s[end - 1]))
            --end;
        return s.substr(start, end - start);
    };

    static constexpr std::string_view version_mark = " version ";
    const size_t version_pos = created_by.find(version_mark);
    writer_version.application = String(trim(created_by.substr(0, version_pos)));
    if (version_pos == std::string_view::npos)
        return;

    std::string_view version = created_by.substr(version_pos + version_mark.size());
    while (!version.empty() && version.front() == ' ')
        version.remove_prefix(1);
    version = trim(version.substr(0, version.find(" (")));
    if (version.empty())
        return;

    /// The strings are of digits only. A number that does not fit reads as 0.
    auto parse_number = [](std::string_view s)
    {
        int value = 0;
        std::from_chars(s.data(), s.data() + s.size(), value);
        return value;
    };
    auto digits_end = [](std::string_view s)
    {
        size_t i = 0;
        while (i < s.size() && isNumericASCII(s[i]))
            ++i;
        return i;
    };

    size_t end = digits_end(version);
    if (end == version.size())
    {
        writer_version.major = parse_number(version);
        return;
    }
    if (version[end] != '.' || end == 0)
        return;
    writer_version.major = parse_number(version.substr(0, end));
    version.remove_prefix(end + 1);

    end = digits_end(version);
    if (end == version.size())
    {
        writer_version.minor = parse_number(version);
        return;
    }
    if (version[end] != '.' || end == 0)
        return;
    writer_version.minor = parse_number(version.substr(0, end));
    version.remove_prefix(end + 1);

    end = digits_end(version);
    if (end == 0)
        return;
    writer_version.patch = parse_number(version.substr(0, end));
}

const parq::ColumnMetaData & FooterMetadata::columnChunk(size_t row_group, size_t column) const
{
    const auto & chunks = file_metadata.row_groups.at(row_group).columns;
    if (column >= chunks.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "The file only has {} columns, requested metadata for column: {}", chunks.size(), column);
    if (column >= leaf_columns.size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid Column Index: {} Num columns: {}", column, leaf_columns.size());

    const auto & chunk = chunks[column];
    if (chunk.__isset.crypto_metadata && chunk.crypto_metadata.__isset.ENCRYPTION_WITH_COLUMN_KEY)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Parquet columns encrypted with a column key are not supported");

    const auto & meta = chunk.meta_data;
    if (meta.__isset.size_statistics)
    {
        const auto & descriptor = leaf_columns[column];
        const auto & size_statistics = meta.size_statistics;
        auto check_histogram = [](const auto & histogram, Int16 max_level, std::string_view what)
        {
            if (!histogram.empty() && histogram.size() != static_cast<size_t>(max_level) + 1)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "{} level histogram size mismatch, size: {}, expected: {}",
                    what, histogram.size(), static_cast<size_t>(max_level) + 1);
        };
        check_histogram(size_statistics.repetition_level_histogram, descriptor.max_repetition_level, "Repetition");
        check_histogram(size_statistics.definition_level_histogram, descriptor.max_definition_level, "Definition");
        if (size_statistics.__isset.unencoded_byte_array_data_bytes && descriptor.physical_type != parq::Type::BYTE_ARRAY)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Unencoded byte array data bytes does not support {}", physicalTypeName(descriptor.physical_type));
    }
    return meta;
}

bool FooterMetadata::hasCorrectStatistics(
    const parq::ColumnMetaData & chunk, const Column & column, bool has_min, bool has_max, std::string_view min, std::string_view max) const
{
    auto version_less = [&](std::string_view application, int major, int minor, int patch)
    {
        if (writer_version.application != application)
            return false;
        return std::tie(writer_version.major, writer_version.minor, writer_version.patch) < std::tie(major, minor, patch);
    };

    /// Statistics are computed correctly for all types since parquet-cpp 1.3.0 and parquet-mr 1.10.0.
    if (version_less("parquet-cpp", 1, 3, 0) || version_less("parquet-mr", 1, 10, 0))
    {
        /// Only signed ones are valid, unless the bounds are equal (then the sort order does not matter).
        const bool max_equals_min = has_min && has_max && min == max;
        if (column.sort_order != SortOrder::Signed && !max_equals_min)
            return false;
        const int chunk_type = thriftEnumToInt(chunk.type);
        if (chunk_type != parq::Type::FIXED_LEN_BYTE_ARRAY && chunk_type != parq::Type::BYTE_ARRAY)
            return true;
    }
    /// An unset `created_by` may come from parquet-mr at the time of PARQUET-251, see PARQUET-297.
    if (writer_version.application == "unknown")
        return true;
    /// PARQUET-251.
    return !version_less("parquet-mr", 1, 8, 0);
}

std::optional<FooterMetadata::Statistics> FooterMetadata::statistics(size_t row_group, size_t column_index) const
{
    const auto & chunk = columnChunk(row_group, column_index);
    if (!chunk.__isset.statistics)
        return std::nullopt;

    const auto & column = leaf_columns[column_index];
    const auto & thrift_statistics = chunk.statistics;

    /// Which bounds can be trusted, given the column order and the sort order of the column.
    bool use_min_max_value = false;
    bool use_legacy_min_max = false;
    switch (column_orders[column_index])
    {
        case ColumnOrder::TypeDefined:
            use_min_max_value = column.sort_order != SortOrder::Unknown;
            break;
        case ColumnOrder::Undefined:
            use_legacy_min_max = column.sort_order == SortOrder::Signed;
            break;
        case ColumnOrder::Unknown:
            break;
    }

    bool has_min = false;
    bool has_max = false;
    std::string_view min;
    std::string_view max;
    if (use_min_max_value)
    {
        has_min = thrift_statistics.__isset.min_value;
        has_max = thrift_statistics.__isset.max_value;
        min = thrift_statistics.min_value;
        max = thrift_statistics.max_value;
    }
    else if (use_legacy_min_max)
    {
        has_min = thrift_statistics.__isset.min;
        has_max = thrift_statistics.__isset.max;
        min = thrift_statistics.min;
        max = thrift_statistics.max;
    }

    if (!hasCorrectStatistics(chunk, column, has_min, has_max, min, max))
        return std::nullopt;

    const auto chunk_physical_type = loadPhysicalType(chunk.type);
    if (chunk_physical_type != column.physical_type)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "ColumnMetaData type does not match ColumnDescriptor physical type: {} vs. {}",
            physicalTypeName(chunk_physical_type), physicalTypeName(column.physical_type));
    if (!column.physical_type)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Can't decode page statistics for selected column type");

    /// The counts come from the file: check their range before the subtraction, which overflows otherwise.
    if (chunk.num_values < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Negative number of values {} in a column chunk of {}", chunk.num_values, column.path);
    if (thrift_statistics.__isset.null_count && (thrift_statistics.null_count < 0 || thrift_statistics.null_count > chunk.num_values))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Invalid null count {} of a column chunk of {} with {} values",
            thrift_statistics.null_count, column.path, chunk.num_values);

    Statistics result;
    /// An unset null count reads as 0.
    result.num_values = chunk.num_values - (thrift_statistics.__isset.null_count ? thrift_statistics.null_count : 0);
    if (thrift_statistics.__isset.null_count)
        result.null_count = thrift_statistics.null_count;
    if (thrift_statistics.__isset.distinct_count)
        result.distinct_count = thrift_statistics.distinct_count;
    if (has_min && has_max)
    {
        /// Bounds too short for their type are rejected, as parquet-cpp's PLAIN decoder does.
        auto check_length = [&](std::string_view value)
        {
            size_t required = 0;
            switch (*column.physical_type)
            {
                case parq::Type::BOOLEAN: required = 1; break;
                case parq::Type::INT32: case parq::Type::FLOAT: required = 4; break;
                case parq::Type::INT64: case parq::Type::DOUBLE: required = 8; break;
                case parq::Type::INT96: required = 12; break;
                case parq::Type::FIXED_LEN_BYTE_ARRAY: required = static_cast<size_t>(column.type_length); break;
                case parq::Type::BYTE_ARRAY: required = 0; break;
            }
            if (value.size() < required)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of stream while decoding Parquet statistics of column {}", column.path);
        };
        check_length(min);
        check_length(max);
        result.min = min;
        result.max = max;
    }
    return result;
}

template <typename T>
T FooterMetadata::decodeStatisticsValue(std::string_view encoded)
{
    if (encoded.size() < sizeof(T))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of stream while decoding Parquet statistics");
    T value;
    memcpy(&value, encoded.data(), sizeof(T));
    return value;
}

template Int32 FooterMetadata::decodeStatisticsValue<Int32>(std::string_view);
template Int64 FooterMetadata::decodeStatisticsValue<Int64>(std::string_view);
template Float32 FooterMetadata::decodeStatisticsValue<Float32>(std::string_view);
template Float64 FooterMetadata::decodeStatisticsValue<Float64>(std::string_view);
template UInt32 FooterMetadata::decodeStatisticsValue<UInt32>(std::string_view);

String FooterMetadata::formatStatisticsValue(size_t column_index, std::string_view encoded) const
{
    const auto & column = leaf_columns.at(column_index);
    if (!column.physical_type)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Can't decode page statistics for selected column type");

    switch (*column.physical_type)
    {
        case parq::Type::BOOLEAN:
            return std::to_string(static_cast<UInt8>(encoded.at(0)) & 1);
        case parq::Type::INT32:
            return std::to_string(decodeStatisticsValue<Int32>(encoded));
        case parq::Type::INT64:
            return std::to_string(decodeStatisticsValue<Int64>(encoded));
        case parq::Type::FLOAT:
            return std::to_string(decodeStatisticsValue<Float32>(encoded));
        case parq::Type::DOUBLE:
            return std::to_string(decodeStatisticsValue<Float64>(encoded));
        case parq::Type::INT96:
        {
            String result;
            for (size_t i = 0; i < 3; ++i)
                result += std::to_string(decodeStatisticsValue<UInt32>(encoded.substr(i * 4))) + " ";
            return result;
        }
        case parq::Type::BYTE_ARRAY:
            return String(encoded);
        case parq::Type::FIXED_LEN_BYTE_ARRAY:
        {
            String result;
            for (Int32 i = 0; i < column.type_length; ++i)
                result += std::to_string(static_cast<UInt8>(encoded.at(i))) + " ";
            return result;
        }
    }
}

}

#endif
