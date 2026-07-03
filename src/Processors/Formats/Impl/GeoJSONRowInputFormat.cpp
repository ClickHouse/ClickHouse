#include <Processors/Formats/Impl/GeoJSONRowInputFormat.h>

#include <array>
#include <cmath>
#include <span>
#include <string_view>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeCustomGeo.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/checkStackSize.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int TOO_DEEP_RECURSION;
}

namespace
{


/// Read a number in the strict JSON grammar (RFC 8259) and return its raw text. The accepted grammar
/// is `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`, so non-JSON spellings such as `+1`, `.5`,
/// `1.`, `+Inf` and `+NaN` are rejected. Returning the raw text rather than a parsed number preserves
/// the exact digits, so a large integer keeps its full precision.
String readStrictJSONNumberText(ReadBuffer & buf)
{
    skipWhitespaceIfAny(buf);

    String number;
    auto peek = [&]() -> int { return buf.eof() ? -1 : static_cast<unsigned char>(*buf.position()); };
    auto consume = [&]() { number += *buf.position(); ++buf.position(); };
    auto is_digit = [](int c) { return c >= '0' && c <= '9'; };

    if (peek() == '-')
        consume();

    int c = peek();
    if (c == '0')
        consume();
    else if (c >= '1' && c <= '9')
    {
        consume();
        while (is_digit(peek()))
            consume();
    }
    else
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: invalid JSON number");

    if (peek() == '.')
    {
        consume();
        if (!is_digit(peek()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: invalid JSON number (missing fraction digits)");
        while (is_digit(peek()))
            consume();
    }

    c = peek();
    if (c == 'e' || c == 'E')
    {
        consume();
        c = peek();
        if (c == '+' || c == '-')
            consume();
        if (!is_digit(peek()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: invalid JSON number (missing exponent digits)");
        while (is_digit(peek()))
            consume();
    }

    return number;
}

/// Read a coordinate number using the strict JSON grammar and return it as `Float64`, additionally
/// rejecting values that overflow `Float64` to a non-finite result (e.g. a huge exponent like `1e400`).
Float64 readStrictJSONNumber(ReadBuffer & buf)
{
    String number = readStrictJSONNumberText(buf);
    Float64 result = 0;
    ReadBufferFromString number_buf(number);
    readFloatText(result, number_buf);
    if (!std::isfinite(result))
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: coordinate value is not finite");
    return result;
}

/// Read a GeoJSON position `[lon, lat, ...]` and return it as `(lon, lat)`. Any extra coordinates
/// (such as an altitude) are read and discarded. When `lon_col`/`lat_col` are provided the position is
/// appended to them directly; otherwise it is only read and validated.
std::pair<Float64, Float64> readGeoJSONPosition(ReadBuffer & buf, ColumnFloat64 * lon_col, ColumnFloat64 * lat_col)
{
    JSONUtils::skipArrayStart(buf);

    Float64 lon = readStrictJSONNumber(buf);
    JSONUtils::skipComma(buf);
    Float64 lat = readStrictJSONNumber(buf);

    /// Any extra position elements (a third altitude coordinate, or more) are discarded, so they are
    /// validated only for JSON-number grammar, not finiteness: a valid but out-of-`Float64`-range value
    /// such as `1e400` in an ignored slot must not reject the document, since it is not stored.
    while (!JSONUtils::checkAndSkipArrayEnd(buf))
    {
        JSONUtils::skipComma(buf);
        readStrictJSONNumberText(buf);
    }

    if (lon_col)
    {
        lon_col->insertValue(lon);
        lat_col->insertValue(lat);
    }
    return {lon, lat};
}

/// Iterate over every element of a JSON array, invoking `handle_element` once per element. The
/// opening `[` must already be consumed, and commas between elements are required.
template <typename ElementHandler>
void forEachElementInJSONArray(ReadBuffer & buf, ElementHandler && handle_element)
{
    bool first = true;
    while (!JSONUtils::checkAndSkipArrayEnd(buf))
    {
        if (!first)
            JSONUtils::skipComma(buf);
        first = false;
        handle_element();
    }
}

/// The shape invariant to enforce on an array of positions: a `LineString`/`MultiLineString` line (at
/// least two points), a `Polygon`/`MultiPolygon` ring (at least four points and closed), or a
/// `MultiPoint` (no invariant).
enum class PositionArrayKind
{
    Line,
    Ring,
    MultiPoint,
};

/// Read an array of positions `[[lon,lat],...]` and enforce the shape invariant for `kind`. When
/// `array_col` (an `Array(Tuple(Float64, Float64))`) is provided, each position is appended to its
/// nested tuple and one array offset is pushed; otherwise the positions are only read and validated.
void readGeoJSONPositions(ReadBuffer & buf, ColumnArray * array_col, PositionArrayKind kind, bool validate)
{
    ColumnFloat64 * lon_col = nullptr;
    ColumnFloat64 * lat_col = nullptr;
    if (array_col)
    {
        auto & tuple_col = assert_cast<ColumnTuple &>(array_col->getData());
        lon_col = &assert_cast<ColumnFloat64 &>(tuple_col.getColumn(0));
        lat_col = &assert_cast<ColumnFloat64 &>(tuple_col.getColumn(1));
    }

    JSONUtils::skipArrayStart(buf);
    size_t count = 0;
    std::pair<Float64, Float64> first{};
    std::pair<Float64, Float64> last{};
    forEachElementInJSONArray(buf, [&]
    {
        auto position = readGeoJSONPosition(buf, lon_col, lat_col);
        if (count == 0)
            first = position;
        last = position;
        ++count;
    });

    /// Enforce the GeoJSON shape invariants only when geometry validation is enabled
    /// (`format_geojson_validate_geometry`). When disabled, degenerate lines and rings are read as-is.
    if (validate)
    {
        if (kind == PositionArrayKind::Line && count < 2)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "GeoJSON: a LineString must have at least two points, but got {}", count);
        if (kind == PositionArrayKind::Ring)
        {
            if (count < 4)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA, "GeoJSON: a Polygon ring must have at least four points, but got {}", count);
            if (first != last)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "GeoJSON: a Polygon ring must be closed (its first and last points must be equal)");
        }
    }

    if (array_col)
        array_col->getOffsets().push_back(array_col->getData().size());
}

/// Guard against unbounded recursion while parsing a nested JSON value. A value nested deeper than the
/// configured limit raises an exception, and the real stack depth is probed every
/// `STACK_CHECK_INTERVAL` levels so a pathological document is rejected before it overflows the stack.
constexpr size_t STACK_CHECK_INTERVAL = 1024;

void checkJSONNestingDepth(size_t current_depth, const FormatSettings::JSON & json_settings)
{
    if (unlikely(current_depth > json_settings.max_depth))
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "GeoJSON is too deep");
    if (unlikely(current_depth > 0 && current_depth % STACK_CHECK_INTERVAL == 0))
        checkStackSize();
}

/// Strictly skip a JSON value, requiring commas between object members and array elements.
/// Unlike `skipJSONField`, which tolerates missing separators, this rejects malformed JSON
/// even inside ignored fields (e.g. a skipped `bbox` object `{"a":1 "b":2}`).
/// The recursion is bounded the same way as `skipJSONField`: deeply nested ignored values
/// (e.g. under a `bbox` key) raise `TOO_DEEP_RECURSION` rather than exhausting the parser stack.
void skipJSONValueStrict(ReadBuffer & buf, const FormatSettings::JSON & json_settings, size_t current_depth = 0)
{
    checkJSONNestingDepth(current_depth, json_settings);

    skipWhitespaceIfAny(buf);
    if (buf.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: unexpected end of input while reading a value");

    if (*buf.position() == '{')
    {
        JSONUtils::skipObjectStart(buf);
        bool first = true;
        while (!JSONUtils::checkAndSkipObjectEnd(buf))
        {
            if (!first)
                JSONUtils::skipComma(buf);
            first = false;
            JSONUtils::readFieldName(buf, json_settings);
            skipJSONValueStrict(buf, json_settings, current_depth + 1);
        }
    }
    else if (*buf.position() == '[')
    {
        JSONUtils::skipArrayStart(buf);
        forEachElementInJSONArray(buf, [&] { skipJSONValueStrict(buf, json_settings, current_depth + 1); });
    }
    else
    {
        /// A scalar: string, boolean, null, or number. Strings/booleans/null have no internal
        /// separators to validate, so the permissive `skipJSONField` is fine for them. A number is
        /// validated against the strict JSON grammar (so non-JSON spellings such as `+Inf` or `+NaN`
        /// are rejected even in an ignored field) but its text is discarded. Finiteness is deliberately
        /// not enforced here: the value is in an ignored field (a `bbox`, a foreign member, an
        /// unrequested `properties` value), so a valid JSON number that merely overflows `Float64`
        /// (such as `1e400`) must not reject the document, since it is never stored as a coordinate.
        const char c = *buf.position();
        if (c == '"' || c == 't' || c == 'f' || c == 'n')
            skipJSONField(buf, "<ignored>", json_settings);
        else
            readStrictJSONNumberText(buf);
    }
}

/// A GeoJSON Feature's `properties` member must be a JSON object or `null` (RFC 7946, section 3.2).
/// Both `skipJSONValueStrict` and the `JSON`-column deserializer would otherwise accept scalar values
/// (numbers, strings, booleans) and arrays, so a malformed feature such as `{"properties": 1}` would be
/// ingested. Peek at the first non-whitespace byte and reject anything that is not an object or `null`
/// before the value is consumed, independently of whether `properties` is a requested output column.
void assertPropertiesIsObjectOrNull(ReadBuffer & buf)
{
    skipWhitespaceIfAny(buf);
    if (buf.eof())
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "GeoJSON: unexpected end of input while reading the 'properties' member");

    /// The only JSON values that start with `{` or `n` are an object and `null` respectively; the
    /// strict skipper or the column deserializer validates the rest of the token afterwards.
    const char c = *buf.position();
    if (c != '{' && c != 'n')
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "GeoJSON: a Feature's 'properties' member must be a JSON object or null");
}

/// Read and validate a Feature's optional `id` member. RFC 7946 (section 3.2) restricts it to a JSON
/// string or number, so an object, array or boolean is rejected. A number is stored verbatim as text
/// so that a large integer id keeps its exact value, and an explicit `null` is treated as an absent
/// id. When `col` is null the member is only validated and nothing is inserted.
void readGeoJSONId(IColumn * col, ReadBuffer & buf, const FormatSettings::JSON & json_settings)
{
    skipWhitespaceIfAny(buf);
    if (buf.eof())
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "GeoJSON: unexpected end of input while reading the 'id' member");

    const char c = *buf.position();
    if (c == 'n')
    {
        assertString("null", buf);
        if (col)
            col->insertDefault();
        return;
    }

    String value;
    if (c == '"')
        readJSONString(value, buf, json_settings);
    else if (c == '-' || (c >= '0' && c <= '9'))
        value = readStrictJSONNumberText(buf);
    else
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "GeoJSON: a Feature's 'id' member must be a JSON string or number");

    if (col)
        col->insertData(value.data(), value.size());
}

/// Iterate over every field of a JSON object (opening `{` must already be consumed).
/// Calls handle_field(key) for each field. If handle_field returns true the value was
/// consumed by the callback; otherwise the value is skipped automatically.
template <typename FieldHandler>
void forEachFieldInJSONObject(ReadBuffer & buf, const FormatSettings::JSON & json_settings, FieldHandler && handle_field)
{
    bool first = true;
    while (!JSONUtils::checkAndSkipObjectEnd(buf))
    {
        if (!first)
            JSONUtils::skipComma(buf);
        first = false;
        String key = JSONUtils::readFieldName(buf, json_settings);
        if (!handle_field(key))
            skipJSONValueStrict(buf, json_settings);
    }
}

/// Return whether `name` matches one of the given fixed geometry-type names.
bool isOneOf(std::string_view name, std::span<const std::string_view> names)
{
    for (std::string_view candidate : names)
        if (name == candidate)
            return true;
    return false;
}

/// The GeoJSON geometry types that ClickHouse's `Geometry` type can represent. `Ring` is part of the
/// `Geometry` Variant but is not a valid GeoJSON geometry type, so it is intentionally absent.
constexpr std::array<std::string_view, 5> supported_geojson_geometry_types
    = {"Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"};

/// Read the `type`, `coordinates`, and `geometries` members of a geometry object into strings (the
/// opening `{` must already be consumed). `coordinates` and `geometries` are buffered verbatim so
/// member order does not matter; any other member is skipped. A repeated `type`, `coordinates`, or
/// `geometries` member is rejected as a duplicate rather than letting a later value silently win.
void readGeometryMembers(
    ReadBuffer & buf,
    const FormatSettings::JSON & json_settings,
    String & geo_type,
    String & raw_coordinates,
    String & raw_geometries)
{
    bool has_type = false;
    bool has_coordinates = false;
    bool has_geometries = false;
    forEachFieldInJSONObject(buf, json_settings, [&](const String & key)
    {
        if (key == "type")
        {
            if (has_type)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'type' field in a geometry");
            readJSONString(geo_type, buf, json_settings);
            has_type = true;
            return true;
        }
        if (key == "coordinates")
        {
            if (has_coordinates)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'coordinates' field in a geometry");
            readJSONField(raw_coordinates, buf, json_settings);
            has_coordinates = true;
            return true;
        }
        if (key == "geometries")
        {
            if (has_geometries)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'geometries' field in a geometry");
            readJSONField(raw_geometries, buf, json_settings);
            has_geometries = true;
            return true;
        }
        return false;
    });
}

/// Insert a NULL geometry — the default of the `Geometry` Variant — when the column is requested.
void insertNullGeometry(IColumn * col)
{
    if (col)
        assert_cast<ColumnVariant &>(*col).insertDefault();
}

/// Read a JSON array into one entry of `array_col`, reading each element with `read_element` (given
/// `array_col`'s nested `ColumnArray`, or null in validation-only mode), then push the array offset.
/// Returns the element count so the caller can reject an empty array.
template <typename ElementReader>
size_t readGeoJSONNestedArray(ReadBuffer & buf, ColumnArray * array_col, ElementReader && read_element)
{
    ColumnArray * nested_col = array_col ? &assert_cast<ColumnArray &>(array_col->getData()) : nullptr;
    JSONUtils::skipArrayStart(buf);
    size_t count = 0;
    forEachElementInJSONArray(buf, [&] { read_element(nested_col); ++count; });
    if (array_col)
        array_col->getOffsets().push_back(array_col->getData().size());
    return count;
}

/// Parse the `coordinates` of a GeoJSON geometry, enforce the GeoJSON shape invariants, and append
/// each position directly into `sub_col` — the matching sub-column of the `Geometry` variant. When
/// `sub_col` is null the coordinates are only read and validated. `Ring` is a synonym for `LineString`
/// because it is part of the `Geometry` type, and `MultiPoint` is read in validation-only mode because
/// it can be stored as NULL.
void parseGeometryCoordinatesInto(const String & geo_type, ReadBuffer & buf, IColumn * sub_col, bool validate)
{
    if (geo_type == "Point")
    {
        ColumnFloat64 * lon_col = nullptr;
        ColumnFloat64 * lat_col = nullptr;
        if (sub_col)
        {
            auto & tuple_col = assert_cast<ColumnTuple &>(*sub_col);
            lon_col = &assert_cast<ColumnFloat64 &>(tuple_col.getColumn(0));
            lat_col = &assert_cast<ColumnFloat64 &>(tuple_col.getColumn(1));
        }
        readGeoJSONPosition(buf, lon_col, lat_col);
        return;
    }

    auto * array_col = sub_col ? &assert_cast<ColumnArray &>(*sub_col) : nullptr;

    if (geo_type == "LineString" || geo_type == "Ring")
    {
        readGeoJSONPositions(buf, array_col, PositionArrayKind::Line, validate);
    }
    else if (geo_type == "MultiPoint")
    {
        readGeoJSONPositions(buf, array_col, PositionArrayKind::MultiPoint, validate);
    }
    else if (geo_type == "MultiLineString")
    {
        /// A `MultiLineString` is an array of lines.
        const size_t lines = readGeoJSONNestedArray(buf, array_col, [&](ColumnArray * line_col) { readGeoJSONPositions(buf, line_col, PositionArrayKind::Line, validate); });
        if (validate && lines == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: a MultiLineString must have at least one LineString");
    }
    else if (geo_type == "Polygon")
    {
        /// A `Polygon` is an array of rings.
        const size_t rings = readGeoJSONNestedArray(buf, array_col, [&](ColumnArray * ring_col) { readGeoJSONPositions(buf, ring_col, PositionArrayKind::Ring, validate); });
        if (validate && rings == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: a Polygon must have at least one ring");
    }
    else if (geo_type == "MultiPolygon")
    {
        /// A `MultiPolygon` is an array of polygons, each an array of rings.
        auto read_polygon = [&](ColumnArray * polygon_col)
        {
            const size_t rings = readGeoJSONNestedArray(buf, polygon_col, [&](ColumnArray * ring_col) { readGeoJSONPositions(buf, ring_col, PositionArrayKind::Ring, validate); });
            if (validate && rings == 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: a Polygon must have at least one ring");
        };
        const size_t polygons = readGeoJSONNestedArray(buf, array_col, read_polygon);
        if (validate && polygons == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: a MultiPolygon must have at least one Polygon");
    }
}

void validateGeoJSONGeometryMembers(
    const String & geo_type,
    const String & raw_coordinates,
    const String & raw_geometries,
    const FormatSettings & format_settings,
    size_t current_depth);

/// Validate that a JSON value read from `buf` is a well-formed GeoJSON geometry, throwing
/// `INCORRECT_DATA` on malformed input. Nothing is inserted: this is used to validate the members
/// of a `GeometryCollection` that is going to be stored as NULL (when
/// `input_format_geojson_unsupported_geometry_handling = 'null'`), so that truncated or malformed
/// documents are still rejected rather than silently loaded as NULL.
void validateGeoJSONGeometry(ReadBuffer & buf, const FormatSettings & format_settings, size_t current_depth)
{
    checkJSONNestingDepth(current_depth, format_settings.json);

    /// Each member of a `GeometryCollection` must be a geometry object. Unlike a Feature's top-level
    /// `geometry` member, a JSON `null` is not a valid geometry here, so reject any non-object value.
    if (!JSONUtils::checkAndSkipObjectStart(buf))
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: a member of a 'GeometryCollection' must be a geometry object, not a scalar or null");

    String geo_type;
    String raw_coordinates;
    String raw_geometries;
    readGeometryMembers(buf, format_settings.json, geo_type, raw_coordinates, raw_geometries);

    validateGeoJSONGeometryMembers(geo_type, raw_coordinates, raw_geometries, format_settings, current_depth);
}

/// Validate the already-parsed members (`type`, `coordinates`, `geometries`) of a GeoJSON geometry.
/// The members of a `GeometryCollection` are validated recursively, bounded by the JSON nesting
/// limit. This is shared by `validateGeoJSONGeometry` and by the NULL-handling path of
/// `GeoJSONRowInputFormat::readGeometry`, which has already buffered these members.
void validateGeoJSONGeometryMembers(
    const String & geo_type,
    const String & raw_coordinates,
    const String & raw_geometries,
    const FormatSettings & format_settings,
    size_t current_depth)
{
    if (geo_type == "GeometryCollection")
    {
        if (raw_geometries.empty())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "GeoJSON: geometry of type 'GeometryCollection' is missing the 'geometries' member");
        /// A 'coordinates' member belongs only to a coordinates-based geometry; reject it on a
        /// GeometryCollection rather than buffering and ignoring it.
        if (!raw_coordinates.empty())
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "GeoJSON: a 'GeometryCollection' must not have a 'coordinates' member");
        /// `geometries` must be an array of geometry objects; validate that it is an array and that
        /// each member is itself a well-formed geometry, so a malformed payload (e.g. a scalar or a
        /// collection containing a truncated child) is rejected rather than loaded as NULL.
        ReadBufferFromString geometries_buf(raw_geometries);
        if (!JSONUtils::checkAndSkipArrayStart(geometries_buf))
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "GeoJSON: the 'geometries' member of a 'GeometryCollection' must be an array");
        forEachElementInJSONArray(
            geometries_buf, [&] { validateGeoJSONGeometry(geometries_buf, format_settings, current_depth + 1); });
        return;
    }

    /// `MultiPoint` also carries a `coordinates` member (validated like a linear ring, without the
    /// closed/length invariants); every other type with coordinates is a supported geometry.
    if (!isOneOf(geo_type, supported_geojson_geometry_types) && geo_type != "MultiPoint")
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: unknown or invalid geometry type '{}'", geo_type);

    /// A 'geometries' member belongs only to a GeometryCollection; reject it on any other type.
    if (!raw_geometries.empty())
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "GeoJSON: a '{}' geometry must not have a 'geometries' member", geo_type);

    if (raw_coordinates.empty())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: geometry of type '{}' is missing the 'coordinates' member", geo_type);

    ReadBufferFromString coord_buf(raw_coordinates);
    parseGeometryCoordinatesInto(geo_type, coord_buf, nullptr, format_settings.geojson.validate_geometry);
}

} /// anonymous namespace


GeoJSONRowInputFormat::GeoJSONRowInputFormat(
    ReadBuffer & in_,
    SharedHeader header_,
    Params params_,
    const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, std::move(params_))
    , format_settings(format_settings_)
{
    const auto & header = getPort().getHeader();
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & col = header.getByPosition(i);
        if (col.name == "id")
        {
            /// Accept both `String` and `Nullable(String)`. Schema inference yields `Nullable(String)`
            /// so that an absent or `null` GeoJSON `id` becomes NULL (distinct from an explicit empty
            /// string), mirroring how `properties` is `Nullable(JSON)`.
            if (!WhichDataType(removeNullable(col.type)).isString())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The 'id' column of the GeoJSON input format must have type 'String' or 'Nullable(String)', "
                    "but it has type '{}'",
                    col.type->getName());
            id_col_idx = i;
        }
        else if (col.name == "geometry")
        {
            geometry_col_idx = i;
            static constexpr std::array geo_type_names
                = {"Point", "LineString", "Polygon", "MultiPolygon", "Ring", "MultiLineString"};

            const auto * variant_type = typeid_cast<const DataTypeVariant *>(col.type.get());
            if (variant_type)
            {
                for (const auto & geo_type_name : geo_type_names)
                {
                    auto discr = variant_type->tryGetVariantDiscriminator(geo_type_name);
                    if (discr.has_value())
                        geometry_discriminators[geo_type_name] = *discr;
                }
            }

            /// The format always produces one of the geometry types of the `Geometry` type, so the column must
            /// contain the full set of them. Otherwise geometries would be silently inserted as NULL (data loss).
            if (geometry_discriminators.size() != geo_type_names.size())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The 'geometry' column of the GeoJSON input format must have type 'Geometry', but it has type '{}'",
                    col.type->getName());
        }
        else if (col.name == "properties")
        {
            properties_col_idx = i;
        }
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unsupported column '{}' for the GeoJSON input format. "
                "Only 'id', 'geometry', and 'properties' columns are supported.",
                col.name);
    }
}

void GeoJSONRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    first_row = true;
    done = false;
    /// `StreamingFormatExecutor` reuses one parser across buffers (Kafka/FileLog/NATS/RabbitMQ/async
    /// insert). The top-level `type` is validated per document, so the flag must be cleared too;
    /// otherwise a later buffer missing the top-level `type` would inherit a stale `true` and skip
    /// the missing-`type` exception in `readSuffix`.
    top_level_type_validated = false;
}

void GeoJSONRowInputFormat::validateTopLevelTypeMember(ReadBuffer & buf)
{
    /// A second top-level `type` member, whether before or after the `features` array, is a duplicate.
    if (top_level_type_validated)
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate top-level 'type' member");
    String type;
    readJSONString(type, buf, format_settings.json);
    if (type != "FeatureCollection")
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: the top-level 'type' member must be 'FeatureCollection', but got '{}'", type);
    top_level_type_validated = true;
}

void GeoJSONRowInputFormat::readPrefix()
{
    auto & buf = getReadBuffer();
    JSONUtils::skipObjectStart(buf);

    /// Scan the top-level object up to the `features` array. The `type` member must be
    /// `FeatureCollection`; it is validated here if it precedes `features`, otherwise `readSuffix`
    /// validates it among the remaining members. Any other member is strictly skipped.
    bool found_features = false;
    bool first = true;
    while (!JSONUtils::checkAndSkipObjectEnd(buf))
    {
        if (!first)
            JSONUtils::skipComma(buf);
        first = false;
        String key = JSONUtils::readFieldName(buf, format_settings.json);
        if (key == "type")
            validateTopLevelTypeMember(buf);
        else if (key == "features")
        {
            found_features = true;
            break;
        }
        else
            skipJSONValueStrict(buf, format_settings.json);
    }

    if (!found_features)
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: 'features' array not found in FeatureCollection");
    JSONUtils::skipArrayStart(buf);
}

void GeoJSONRowInputFormat::readSuffix()
{
    auto & buf = getReadBuffer();
    /// We are positioned right after the `features` array. Strictly skip any remaining members
    /// of the top-level FeatureCollection object (validating commas between members and rejecting
    /// malformed JSON even under ignored fields, e.g. a trailing `bbox` `{"a":1 "b":2}`) up to and
    /// including the closing `}`. The `type` member is validated here if it follows `features`, and a
    /// repeated `features` array is rejected as a duplicate rather than silently dropped.
    while (!JSONUtils::checkAndSkipObjectEnd(buf))
    {
        JSONUtils::skipComma(buf);
        String key = JSONUtils::readFieldName(buf, format_settings.json);
        if (key == "type")
            validateTopLevelTypeMember(buf);
        else if (key == "features")
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "GeoJSON: duplicate 'features' array in the FeatureCollection");
        else
            skipJSONValueStrict(buf, format_settings.json);
    }

    /// The top-level `type` member is required by the GeoJSON specification.
    if (!top_level_type_validated)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: the top-level 'type' member 'FeatureCollection' is missing");

    /// Reject trailing data after the top-level FeatureCollection object instead of ignoring it.
    /// We tolerate a single statement terminator so that the document can be supplied as inline
    /// `INSERT ... FORMAT GeoJSON {...};` data, where the read buffer legitimately ends with the
    /// trailing `;`. This mirrors `JSONEachRowRowInputFormat::readSuffix`: consume at most one `;`,
    /// then require EOF so that genuine trailing garbage (e.g. `...}; garbage`) is still rejected
    /// rather than silently ignored. As with `JSONEachRow`, a further statement left in the same
    /// multi-query buffer (`...};\nSELECT ...`) is therefore rejected too.
    skipWhitespaceIfAny(buf);
    if (!buf.eof() && *buf.position() == ';')
    {
        ++buf.position();
        skipWhitespaceIfAny(buf);
    }
    if (!buf.eof())
        throw Exception(
            ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
            "GeoJSON: unexpected trailing data after the top-level FeatureCollection object");
}

bool GeoJSONRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (done)
        return false;

    auto & buf = getReadBuffer();

    if (JSONUtils::checkAndSkipArrayEnd(buf))
    {
        done = true;
        return false;
    }

    /// Features must be separated by a comma; reject missing separators instead of normalizing them.
    if (!first_row)
        JSONUtils::skipComma(buf);

    first_row = false;

    bool has_type = false;
    bool has_id = false;
    bool has_geometry = false;
    bool has_properties = false;

    JSONUtils::skipObjectStart(buf);
    forEachFieldInJSONObject(buf, format_settings.json, [&](const String & key)
    {
        if (key == "type")
        {
            if (has_type)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'type' field in a feature");
            String type;
            readJSONString(type, buf, format_settings.json);
            if (type != "Feature")
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "GeoJSON: each member of 'features' must have type 'Feature', but got '{}'", type);
            has_type = true;
            return true;
        }
        if (key == "id")
        {
            if (has_id)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'id' field in a feature");
            /// `id` is restricted to a JSON string or number and is validated even when it is not a
            /// requested output column; it is inserted only when the `id` column is selected.
            readGeoJSONId(id_col_idx.has_value() ? columns[*id_col_idx].get() : nullptr, buf, format_settings.json);
            has_id = true;
            return true;
        }
        /// `geometry` and `properties` are required members of every Feature, so they are validated
        /// and required even when they are not requested output columns. An unrequested `geometry` is
        /// still fully validated (without inserting); an unrequested `properties` is validated as a
        /// well-formed JSON object or `null` by the strict skipper.
        if (key == "geometry")
        {
            if (has_geometry)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'geometry' field in a feature");
            readGeometry(geometry_col_idx.has_value() ? columns[*geometry_col_idx].get() : nullptr);
            has_geometry = true;
            return true;
        }
        if (key == "properties")
        {
            if (has_properties)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'properties' field in a feature");
            /// `properties` must be a JSON object or `null`; reject scalars and arrays regardless of
            /// whether `properties` is a requested output column.
            assertPropertiesIsObjectOrNull(buf);
            if (properties_col_idx.has_value())
                SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(
                    *columns[*properties_col_idx], buf, format_settings, serializations[*properties_col_idx]);
            else
                skipJSONValueStrict(buf, format_settings.json);
            has_properties = true;
            return true;
        }
        return false;
    });

    /// Every member of `features` must be a `Feature` object: reject one whose required `type`
    /// member is missing instead of ingesting a non-GeoJSON element.
    if (!has_type)
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "GeoJSON: a feature is missing the required 'type' member 'Feature'");

    /// `id` is optional in a GeoJSON feature, but `geometry` and `properties` are required members
    /// (`geometry` may be an explicit JSON `null`). Default only the optional `id`; reject a feature
    /// that is missing a required member regardless of whether it is a requested output column, so a
    /// malformed Feature is rejected even when only `id` is selected.
    if (!has_id && id_col_idx.has_value())
        columns[*id_col_idx]->insertDefault();
    if (!has_geometry)
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: feature is missing the required 'geometry' member");
    if (!has_properties)
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: feature is missing the required 'properties' member");

    ext.read_columns.assign(columns.size(), 1);
    return true;
}

void GeoJSONRowInputFormat::readGeometry(IColumn * col)
{
    auto & buf = getReadBuffer();

    if (!JSONUtils::checkAndSkipObjectStart(buf))
    {
        /// A null geometry is valid at the top level of a Feature.
        assertString("null", buf);
        insertNullGeometry(col);
        return;
    }

    String geo_type;
    String raw_coordinates;
    String raw_geometries;
    readGeometryMembers(buf, format_settings.json, geo_type, raw_coordinates, raw_geometries);

    /// Valid GeoJSON geometry types that cannot be represented in ClickHouse's Geometry type.
    static constexpr std::array<std::string_view, 2> unrepresentable_geojson_types = {"GeometryCollection", "MultiPoint"};

    if (!isOneOf(geo_type, supported_geojson_geometry_types))
    {
        if (isOneOf(geo_type, unrepresentable_geojson_types))
        {
            /// Validate that the object is a well-formed GeoJSON geometry of its declared type, so a
            /// truncated or malformed document (a missing `coordinates`/`geometries` member, a non-array
            /// `geometries`, or a `GeometryCollection` with a malformed child) is rejected rather than
            /// silently accepted. The representation policy applies only when the value is materialized:
            /// insert NULL when the user opts in, or throw by default so data is not silently lost. When
            /// the `geometry` column is not requested nothing is materialized, so the policy does not apply.
            if (!col || format_settings.geojson.unsupported_geometry_handling == FormatSettings::UnsupportedGeometryHandling::Null)
            {
                validateGeoJSONGeometryMembers(geo_type, raw_coordinates, raw_geometries, format_settings, 0);
                insertNullGeometry(col);
                return;
            }
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "GeoJSON: geometry type '{}' cannot be represented in ClickHouse's Geometry type. "
                "Set input_format_geojson_unsupported_geometry_handling = 'null' to insert NULL "
                "for such geometries instead of throwing.",
                geo_type);
        }

        /// An unknown, missing, or non-GeoJSON geometry type (such as `Ring`) is malformed input.
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "GeoJSON: unknown or invalid geometry type '{}'", geo_type);
    }

    /// A 'geometries' member belongs only to a GeometryCollection; reject it on a coordinates-based
    /// geometry rather than buffering and ignoring it, which would let malformed content slip through.
    if (!raw_geometries.empty())
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "GeoJSON: a '{}' geometry must not have a 'geometries' member", geo_type);

    /// A supported geometry type without a 'coordinates' member is malformed (an explicit JSON `null`
    /// geometry is handled separately above), so reject it instead of silently inserting NULL.
    if (raw_coordinates.empty())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: geometry of type '{}' is missing the 'coordinates' member", geo_type);

    /// Parse the coordinates, enforce the GeoJSON shape invariants, and append them directly into the
    /// geometry's variant sub-column, so degenerate lines and unclosed/too-short rings are rejected as
    /// malformed input instead of being stored. This also runs in validation-only mode (`col ==
    /// nullptr`), where an unrequested `geometry` member is validated without being inserted.
    ReadBufferFromString coord_buf(raw_coordinates);

    if (!col)
    {
        parseGeometryCoordinatesInto(geo_type, coord_buf, nullptr, format_settings.geojson.validate_geometry);
        return;
    }

    auto & variant_col = assert_cast<ColumnVariant &>(*col);
    ColumnVariant::Discriminator global_discr = geometry_discriminators.at(geo_type);
    auto & sub_col = variant_col.getVariantByGlobalDiscriminator(global_discr);
    auto local_discr = variant_col.localDiscriminatorByGlobal(global_discr);
    size_t offset = sub_col.size();

    parseGeometryCoordinatesInto(geo_type, coord_buf, &sub_col, format_settings.geojson.validate_geometry);
    variant_col.getLocalDiscriminators().push_back(local_discr);
    variant_col.getOffsets().push_back(offset);
}

NamesAndTypesList GeoJSONExternalSchemaReader::readSchema()
{
    return {
        /// `id` is `Nullable` so that a feature with an absent or explicit `"id": null` member is
        /// stored as NULL, kept distinct from a feature whose `id` is an explicit empty string `""`.
        {"id", makeNullable(std::make_shared<DataTypeString>())},
        {"geometry", DataTypeFactory::instance().get("Geometry")},
        /// `properties` is `Nullable` so that an explicit GeoJSON `"properties": null` is preserved
        /// as NULL rather than being indistinguishable from a default (empty) JSON object.
        {"properties", makeNullable(DataTypeFactory::instance().get("JSON"))},
    };
}

void registerInputFormatGeoJSON(FormatFactory & factory)
{
    factory.registerInputFormat(
        "GeoJSON",
        [](ReadBuffer & buf,
           const Block & sample,
           IRowInputFormat::Params params,
           const FormatSettings & settings)
        {
            return std::make_shared<GeoJSONRowInputFormat>(
                buf, std::make_shared<const Block>(sample), std::move(params), settings);
        });

    factory.registerFileExtension("geojson", "GeoJSON");
}

void registerGeoJSONSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("GeoJSON", [](const FormatSettings &)
    {
        return std::make_shared<GeoJSONExternalSchemaReader>();
    });
}

}
