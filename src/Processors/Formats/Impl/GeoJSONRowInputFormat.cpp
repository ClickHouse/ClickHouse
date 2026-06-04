#include <Processors/Formats/Impl/GeoJSONRowInputFormat.h>

#include <array>
#include <cmath>
#include <unordered_set>

#include <Columns/ColumnVariant.h>
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


/// Read a single number using the strict JSON grammar (RFC 8259) and return it as `Float64`.
/// `readFloatText` is too permissive for validating JSON: it accepts non-JSON spellings such as
/// `+1`, `.5`, `1.`, `+Inf` and `+NaN`, so a coordinate like `[+Inf, 0]` would be loaded as a
/// `Geometry` instead of being rejected. This helper accepts only `-?(0|[1-9][0-9]*)(\.[0-9]+)?
/// ([eE][+-]?[0-9]+)?` and additionally rejects values that overflow `Float64` to a non-finite
/// result (e.g. a huge exponent like `1e400`).
Float64 readStrictJSONNumber(ReadBuffer & buf)
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
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: invalid JSON number in coordinates");

    if (peek() == '.')
    {
        consume();
        if (!is_digit(peek()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: invalid JSON number in coordinates (missing fraction digits)");
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
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: invalid JSON number in coordinates (missing exponent digits)");
        while (is_digit(peek()))
            consume();
    }

    Float64 result = 0;
    ReadBufferFromString number_buf(number);
    readFloatText(result, number_buf);
    if (!std::isfinite(result))
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: coordinate value is not finite");
    return result;
}

/// Reads a GeoJSON position [lon, lat, ...] into a Tuple{Float64, Float64}.
Field readGeoJSONPoint(ReadBuffer & buf)
{
    JSONUtils::skipArrayStart(buf);

    Float64 lon = readStrictJSONNumber(buf);
    JSONUtils::skipComma(buf);
    Float64 lat = readStrictJSONNumber(buf);

    /// Skip optional extra coordinates (e.g. altitude).
    while (!JSONUtils::checkAndSkipArrayEnd(buf))
    {
        JSONUtils::skipComma(buf);
        readStrictJSONNumber(buf);
    }

    return Tuple{lon, lat};
}

/// Helper to read a JSON array of items produced by read_element into an Array Field.
template <typename ElementReader>
Array readGeoJSONArray(ReadBuffer & buf, ElementReader read_element)
{
    JSONUtils::skipArrayStart(buf);
    Array items;
    bool first = true;
    while (!JSONUtils::checkAndSkipArrayEnd(buf))
    {
        if (!first)
            JSONUtils::skipComma(buf);
        first = false;
        items.push_back(read_element(buf));
    }
    return items;
}

/// Reads [[lon,lat],...] into an Array of Tuple{Float64, Float64} (Ring / LineString coordinates).
Array readGeoJSONLinearRing(ReadBuffer & buf) { return readGeoJSONArray(buf, readGeoJSONPoint); }

/// Reads [[[lon,lat],...]] into Array of Array of Tuple (Polygon / MultiLineString coordinates).
Array readGeoJSONPolygonCoordinates(ReadBuffer & buf) { return readGeoJSONArray(buf, readGeoJSONLinearRing); }

/// Reads [[[[lon,lat],...],...]] into Array of Array of Array of Tuple (MultiPolygon coordinates).
Array readGeoJSONMultiPolygonCoordinates(ReadBuffer & buf) { return readGeoJSONArray(buf, readGeoJSONPolygonCoordinates); }

/// Enforce the GeoJSON shape invariant for a `LineString` (and each line of a `MultiLineString`):
/// it must contain at least two positions. Otherwise an invalid (degenerate) line would be stored
/// as a `Geometry`, leaving geospatial functions to operate on malformed data.
void validateLineStringCoordinates(const Array & line)
{
    if (line.size() < 2)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: a LineString must have at least two positions, but got {}",
            line.size());
}

/// Enforce the GeoJSON shape invariant for a `Polygon` ring (and each ring of a `MultiPolygon`):
/// a linear ring must have at least four positions and be closed (its first and last positions are
/// equal). Otherwise an unclosed or degenerate ring would be stored as a `Geometry`.
void validateRingCoordinates(const Array & ring)
{
    if (ring.size() < 4)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: a Polygon ring must have at least four positions, but got {}",
            ring.size());
    if (ring.front() != ring.back())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: a Polygon ring must be closed (its first and last positions must be equal)");
}

/// Strictly skip a JSON value, requiring commas between object members and array elements.
/// Unlike `skipJSONField`, which tolerates missing separators, this rejects malformed JSON
/// even inside ignored fields (e.g. a skipped `bbox` object `{"a":1 "b":2}`).
/// The recursion is bounded the same way as `skipJSONField`: deeply nested ignored values
/// (e.g. under a `bbox` key) raise `TOO_DEEP_RECURSION` rather than exhausting the parser stack.
void skipJSONValueStrict(ReadBuffer & buf, const FormatSettings::JSON & json_settings, size_t current_depth = 0)
{
    if (unlikely(current_depth > json_settings.max_depth))
        throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "GeoJSON is too deep");

    if (unlikely(current_depth > 0 && current_depth % 1024 == 0))
        checkStackSize();

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
        bool first = true;
        while (!JSONUtils::checkAndSkipArrayEnd(buf))
        {
            if (!first)
                JSONUtils::skipComma(buf);
            first = false;
            skipJSONValueStrict(buf, json_settings, current_depth + 1);
        }
    }
    else
    {
        /// A scalar: string, boolean, null, or number. Strings/booleans/null have no internal
        /// separators to validate, so the permissive `skipJSONField` is fine for them. Numbers,
        /// however, must be validated against the strict JSON grammar even in ignored fields (e.g.
        /// a skipped `bbox`), so that non-JSON spellings such as `+Inf` or `+NaN` are rejected
        /// rather than silently tolerated in an otherwise-rejecting parser.
        const char c = *buf.position();
        if (c == '"' || c == 't' || c == 'f' || c == 'n')
            skipJSONField(buf, "<ignored>", json_settings);
        else
            readStrictJSONNumber(buf);
    }
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
                        geometry_discriminants[geo_type_name] = *discr;
                }
            }

            /// The format always produces one of the geometry types of the `Geometry` type, so the column must
            /// contain the full set of them. Otherwise geometries would be silently inserted as NULL (data loss).
            if (geometry_discriminants.size() != geo_type_names.size())
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
    /// including the closing `}`. The `type` member is validated here if it follows `features`.
    while (!JSONUtils::checkAndSkipObjectEnd(buf))
    {
        JSONUtils::skipComma(buf);
        String key = JSONUtils::readFieldName(buf, format_settings.json);
        if (key == "type")
            validateTopLevelTypeMember(buf);
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
            String type;
            readJSONString(type, buf, format_settings.json);
            if (type != "Feature")
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "GeoJSON: each member of 'features' must have type 'Feature', but got '{}'", type);
            has_type = true;
            return true;
        }
        if (key == "id" && id_col_idx.has_value())
        {
            if (has_id)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'id' field in a feature");
            SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(
                *columns[*id_col_idx], buf, format_settings, serializations[*id_col_idx]);
            has_id = true;
            return true;
        }
        if (key == "geometry" && geometry_col_idx.has_value())
        {
            if (has_geometry)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'geometry' field in a feature");
            readGeometry(*columns[*geometry_col_idx]);
            has_geometry = true;
            return true;
        }
        if (key == "properties" && properties_col_idx.has_value())
        {
            if (has_properties)
                throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: duplicate 'properties' field in a feature");
            SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(
                *columns[*properties_col_idx], buf, format_settings, serializations[*properties_col_idx]);
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
    /// that is missing a requested required member instead of silently inserting a default value.
    if (!has_id && id_col_idx.has_value())
        columns[*id_col_idx]->insertDefault();
    if (!has_geometry && geometry_col_idx.has_value())
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: feature is missing the required 'geometry' member");
    if (!has_properties && properties_col_idx.has_value())
        throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: feature is missing the required 'properties' member");

    ext.read_columns.assign(columns.size(), 1);
    return true;
}

void GeoJSONRowInputFormat::readGeometry(IColumn & col)
{
    auto & buf = getReadBuffer();
    auto & variant_col = assert_cast<ColumnVariant &>(col);

    if (!JSONUtils::checkAndSkipObjectStart(buf))
    {
        assertString("null", buf);
        variant_col.insertDefault();
        return;
    }

    String geo_type;
    String raw_coordinates;
    String raw_geometries;

    forEachFieldInJSONObject(buf, format_settings.json, [&](const String & key)
    {
        if (key == "type") { readJSONString(geo_type, buf, format_settings.json); return true; }
        /// Always buffer coordinates as raw string so key order doesn't matter.
        if (key == "coordinates") { readJSONField(raw_coordinates, buf, format_settings.json); return true; }
        /// `geometries` is the required member of a `GeometryCollection`; buffer it so we can verify
        /// the object is well-formed even when it ends up being inserted as NULL.
        if (key == "geometries") { readJSONField(raw_geometries, buf, format_settings.json); return true; }
        return false;
    });

    /// GeoJSON geometry types that ClickHouse's Geometry type can represent. Note that `Ring` is part of the
    /// Geometry Variant but is NOT a valid GeoJSON geometry type, so it is intentionally excluded here.
    static const std::unordered_set<String> supported_geojson_types
        = {"Point", "LineString", "MultiLineString", "Polygon", "MultiPolygon"};
    /// Valid GeoJSON geometry types that cannot be represented in ClickHouse's Geometry type.
    static const std::unordered_set<String> unrepresentable_geojson_types = {"GeometryCollection", "MultiPoint"};

    if (!supported_geojson_types.contains(geo_type))
    {
        if (unrepresentable_geojson_types.contains(geo_type))
        {
            /// Throw by default so that data is not silently lost; the user can opt into inserting NULL instead.
            if (format_settings.geojson.unsupported_geometry_handling == FormatSettings::UnsupportedGeometryHandling::Null)
            {
                /// Even when the value is going to be inserted as NULL, validate that the object is a
                /// well-formed GeoJSON geometry of its declared type, so truncated/malformed documents
                /// are still rejected rather than being silently loaded as NULL.
                if (geo_type == "MultiPoint")
                {
                    if (raw_coordinates.empty())
                        throw Exception(
                            ErrorCodes::INCORRECT_DATA,
                            "GeoJSON: geometry of type 'MultiPoint' is missing the 'coordinates' member");
                    /// MultiPoint coordinates are an array of positions; parse them to validate the shape.
                    ReadBufferFromString coord_buf(raw_coordinates);
                    readGeoJSONLinearRing(coord_buf);
                }
                else if (geo_type == "GeometryCollection")
                {
                    if (raw_geometries.empty())
                        throw Exception(
                            ErrorCodes::INCORRECT_DATA,
                            "GeoJSON: geometry of type 'GeometryCollection' is missing the 'geometries' member");
                }

                variant_col.insertDefault();
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

    /// A supported geometry type without a 'coordinates' member is malformed (an explicit JSON `null`
    /// geometry is handled separately above), so reject it instead of silently inserting NULL.
    if (raw_coordinates.empty())
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "GeoJSON: geometry of type '{}' is missing the 'coordinates' member", geo_type);

    ColumnVariant::Discriminator global_discr = geometry_discriminants.at(geo_type);
    auto & sub_col = variant_col.getVariantByGlobalDiscriminator(global_discr);
    auto local_discr = variant_col.localDiscriminatorByGlobal(global_discr);
    size_t offset = sub_col.size();

    ReadBufferFromString coord_buf(raw_coordinates);
    Field coordinates_field;

    /// Parse the coordinates and enforce the GeoJSON shape invariants for the geometry type before
    /// inserting, so that degenerate lines and unclosed/too-short rings are rejected as malformed
    /// input instead of being stored as valid `Geometry` values.
    if (geo_type == "Point")
        coordinates_field = readGeoJSONPoint(coord_buf);
    else if (geo_type == "LineString" || geo_type == "Ring")
    {
        coordinates_field = readGeoJSONLinearRing(coord_buf);
        validateLineStringCoordinates(coordinates_field.safeGet<Array>());
    }
    else if (geo_type == "MultiLineString")
    {
        coordinates_field = readGeoJSONPolygonCoordinates(coord_buf);
        for (const auto & line : coordinates_field.safeGet<Array>())
            validateLineStringCoordinates(line.safeGet<Array>());
    }
    else if (geo_type == "Polygon")
    {
        coordinates_field = readGeoJSONPolygonCoordinates(coord_buf);
        for (const auto & ring : coordinates_field.safeGet<Array>())
            validateRingCoordinates(ring.safeGet<Array>());
    }
    else if (geo_type == "MultiPolygon")
    {
        coordinates_field = readGeoJSONMultiPolygonCoordinates(coord_buf);
        for (const auto & polygon : coordinates_field.safeGet<Array>())
            for (const auto & ring : polygon.safeGet<Array>())
                validateRingCoordinates(ring.safeGet<Array>());
    }

    sub_col.insert(coordinates_field);
    variant_col.getLocalDiscriminators().push_back(local_discr);
    variant_col.getOffsets().push_back(offset);
}

NamesAndTypesList GeoJSONExternalSchemaReader::readSchema()
{
    return {
        {"id", std::make_shared<DataTypeString>()},
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
