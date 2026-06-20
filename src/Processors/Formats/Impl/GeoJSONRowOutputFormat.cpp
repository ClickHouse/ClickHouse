#include <Processors/Formats/Impl/GeoJSONRowOutputFormat.h>

#include <cmath>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <IO/WriteHelpers.h>
#include <Processors/Port.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

/// The geo types ClickHouse can hold and how each is emitted as a GeoJSON geometry — the single
/// source of truth for both column detection and serialization.
///   - `depth` is the number of nested coordinate-array levels in storage
///     (Point = 0, LineString/Ring = 1, MultiLineString/Polygon = 2, MultiPolygon = 3).
///   - `Ring` has no GeoJSON geometry type (a linear ring is a `Polygon` component), so it is emitted
///     as a single-ring `Polygon` by wrapping its one ring in an extra array (`wrap_in_array`).
struct GeoTypeEntry
{
    const char * ch_name;
    const char * geojson_type;
    size_t depth;
    bool wrap_in_array;
};

constexpr std::array<GeoTypeEntry, 6> geo_type_table{{
    {"Point", "Point", 0, false},
    {"LineString", "LineString", 1, false},
    {"Ring", "Polygon", 1, true},
    {"MultiLineString", "MultiLineString", 2, false},
    {"Polygon", "Polygon", 2, false},
    {"MultiPolygon", "MultiPolygon", 3, false},
}};

/// Whether a `properties` column should be emitted directly as the GeoJSON `properties` object: a
/// `JSON`/`Object`, a `Map`, or a named `Tuple` all serialize to a JSON object. Anything else is
/// instead wrapped as a single property keyed by the column name.
bool isPropertiesObjectLike(const DataTypePtr & type)
{
    WhichDataType which(type);
    if (which.isObject() || which.isMap())
        return true;
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
        return tuple->hasExplicitNames();
    return false;
}

}

std::optional<GeoJSONRowOutputFormat::GeometryKind> GeoJSONRowOutputFormat::geometryKindFor(const String & type_name)
{
    for (const auto & entry : geo_type_table)
        if (type_name == entry.ch_name)
            return GeometryKind{entry.geojson_type, entry.depth, entry.wrap_in_array};
    return std::nullopt;
}

GeoJSONRowOutputFormat::GeoJSONRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
    : RowOutputFormatWithExceptionHandlerAdaptor<RowOutputFormatWithUTF8ValidationAdaptor, bool>(
          header_, out_, settings_.json.valid_output_on_exception, settings_.json.validate_utf8)
    , settings(settings_)
{
    /// GeoJSON coordinates are plain JSON numbers; never quote 64-bit floats regardless of the user
    /// setting, otherwise coordinates would be emitted as strings, which is not valid GeoJSON.
    settings.json.quote_64bit_floats = false;

    /// A named `Tuple` used as the `properties` object must serialize as a JSON object rather than a
    /// JSON array, so the `properties` member is always a valid GeoJSON object.
    settings.json.write_named_tuples_as_objects = true;

    /// A `Map` used as the `properties` object must serialize as a JSON object rather than an array of
    /// key/value tuples, for the same reason.
    settings.json.write_map_as_array_of_tuples = false;

    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();

    const auto & header = getPort(PortKind::Main).getHeader();

    bool found_geometry = false;
    Names property_names;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & column = header.getByPosition(i);
        /// A geo column may be wrapped in `Nullable` (for example `Nullable(Point)` when
        /// `enable_nullable_tuple_type` is set), so match the type after removing that wrapper.
        const auto geo_type = removeNullable(column.type);
        const String type_name = geo_type->getName();

        /// `geometryKindFor` returns nullopt for the `Geometry` variant (handled separately below) and
        /// for non-geo columns.
        const auto kind = geometryKindFor(type_name);
        const bool is_geometry = type_name == "Geometry" || kind.has_value();
        if (is_geometry)
        {
            if (found_geometry)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The GeoJSON output format requires exactly one geometry-typed column, but found at "
                    "least two: '{}' and '{}'",
                    header.getByPosition(geometry_col_idx).name,
                    column.name);

            found_geometry = true;
            geometry_col_idx = i;
            if (type_name == "Geometry")
            {
                geometry_is_variant = true;
                const auto & variant_type = assert_cast<const DataTypeVariant &>(*geo_type);
                for (const auto & entry : geo_type_table)
                    if (auto discr = variant_type.tryGetVariantDiscriminator(entry.ch_name))
                        variant_kind[*discr] = GeometryKind{entry.geojson_type, entry.depth, entry.wrap_in_array};
            }
            else
            {
                concrete_kind = *kind;
            }
        }
        else if (column.name == "id")
        {
            /// A GeoJSON Feature id must be a JSON string or number, so the column must be a string or
            /// numeric type (optionally wrapped in `Nullable` or `LowCardinality`).
            const WhichDataType id_type(removeLowCardinalityAndNullable(column.type));
            if (!id_type.isStringOrFixedString() && !id_type.isNumber())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The 'id' column of the GeoJSON output format must be a String, FixedString, or numeric "
                    "type, but it has type '{}'",
                    column.type->getName());
            id_col_idx = i;
            id_is_float = id_type.isFloat();
        }
        else
        {
            property_col_indices.push_back(i);
            property_names.push_back(column.name);
        }
    }

    if (!found_geometry)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The GeoJSON output format requires exactly one geometry-typed column (Point, LineString, "
            "MultiLineString, Polygon, MultiPolygon, Ring, or Geometry), but found none");

    /// Emit a lone object-typed `properties` column directly as the properties object so that GeoJSON
    /// produced by the input format round-trips; otherwise build the object from the property columns.
    if (property_col_indices.size() == 1)
    {
        const auto & column = header.getByPosition(property_col_indices.front());
        if (column.name == "properties" && isPropertiesObjectLike(removeNullable(column.type)))
            emit_properties_column_directly = true;
    }

    if (!emit_properties_column_directly)
        property_json_names = JSONUtils::makeNamesValidJSONStrings(property_names, settings, settings.json.validate_utf8);
}

void GeoJSONRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    writeCString(R"({"type":"Feature")", *ostr);

    if (id_col_idx)
        writeId(columns, row_num);

    writeCString(R"(,"geometry":)", *ostr);
    writeGeometry(*columns[geometry_col_idx], row_num);

    writeCString(R"(,"properties":)", *ostr);
    writeProperties(columns, row_num);

    writeChar('}', *ostr);
}

void GeoJSONRowOutputFormat::writeId(const Columns & columns, size_t row_num)
{
    const auto & column = *columns[*id_col_idx];
    /// Omit the `id` member for a NULL id. `isNullAt` is virtual on `IColumn`, so this also covers a
    /// `LowCardinality(Nullable(...))` id, not only a plain `Nullable(...)` one.
    if (column.isNullAt(row_num))
        return;

    /// A floating-point id must be finite, since NaN and infinity have no valid JSON representation;
    /// reject them rather than emit `null` or a quoted token as the Feature id.
    if (id_is_float && !std::isfinite(column.getFloat64(row_num)))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "The GeoJSON output format cannot write a non-finite floating-point id");

    writeCString(R"(,"id":)", *ostr);
    serializations[*id_col_idx]->serializeTextJSON(column, row_num, *ostr, settings);
}

void GeoJSONRowOutputFormat::writeGeometry(const IColumn & column, size_t row_num)
{
    /// A concrete geo column may be `Nullable` (for example `Nullable(Point)`); a null value is written
    /// as a `null` geometry, and a non-null value is read from the nested column.
    const IColumn * geo = &column;
    if (isColumnNullable(column))
    {
        const auto & nullable = assert_cast<const ColumnNullable &>(column);
        if (nullable.isNullAt(row_num))
        {
            writeCString("null", *ostr);
            return;
        }
        geo = &nullable.getNestedColumn();
    }

    if (geometry_is_variant)
    {
        const auto & variant = assert_cast<const ColumnVariant &>(*geo);
        const auto discr = variant.globalDiscriminatorAt(row_num);
        if (discr == ColumnVariant::NULL_DISCRIMINATOR)
        {
            writeCString("null", *ostr);
            return;
        }
        writeGeometryObject(variant_kind[discr], variant.getVariantByGlobalDiscriminator(discr), variant.offsetAt(row_num));
    }
    else
    {
        writeGeometryObject(concrete_kind, *geo, row_num);
    }
}

void GeoJSONRowOutputFormat::writeGeometryObject(const GeometryKind & kind, const IColumn & column, size_t row_num)
{
    writeCString(R"({"type":")", *ostr);
    writeCString(kind.geojson_type, *ostr);
    writeCString(R"(","coordinates":)", *ostr);
    if (kind.wrap_in_array)
        writeChar('[', *ostr);
    writeCoordinates(column, row_num, kind.depth);
    if (kind.wrap_in_array)
        writeChar(']', *ostr);
    writeChar('}', *ostr);
}

void GeoJSONRowOutputFormat::writeCoordinates(const IColumn & column, size_t row_num, size_t depth)
{
    if (depth == 0)
    {
        writePosition(column, row_num);
        return;
    }

    const auto & array = assert_cast<const ColumnArray &>(column);
    const IColumn & nested = array.getData();
    const auto & offsets = array.getOffsets();
    const size_t begin = row_num == 0 ? 0 : offsets[row_num - 1];
    const size_t end = offsets[row_num];

    writeChar('[', *ostr);
    for (size_t i = begin; i < end; ++i)
    {
        if (i != begin)
            writeChar(',', *ostr);
        writeCoordinates(nested, i, depth - 1);
    }
    writeChar(']', *ostr);
}

void GeoJSONRowOutputFormat::writePosition(const IColumn & tuple_column, size_t row_num)
{
    const auto & tuple = assert_cast<const ColumnTuple &>(tuple_column);
    const Float64 x = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData()[row_num];
    const Float64 y = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData()[row_num];

    /// A GeoJSON position must consist of JSON numbers; NaN and infinity have no valid JSON
    /// representation, so reject them rather than emit `null` or a quoted token that would not be a
    /// valid coordinate.
    if (!std::isfinite(x) || !std::isfinite(y))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The GeoJSON output format cannot write a non-finite coordinate value");

    writeChar('[', *ostr);
    writeJSONNumber(x, *ostr, settings);
    writeChar(',', *ostr);
    writeJSONNumber(y, *ostr, settings);
    writeChar(']', *ostr);
}

void GeoJSONRowOutputFormat::writeProperties(const Columns & columns, size_t row_num)
{
    if (emit_properties_column_directly)
    {
        const size_t idx = property_col_indices.front();
        serializations[idx]->serializeTextJSON(*columns[idx], row_num, *ostr, settings);
        return;
    }

    writeChar('{', *ostr);
    for (size_t k = 0; k < property_col_indices.size(); ++k)
    {
        if (k != 0)
            writeChar(',', *ostr);
        const size_t idx = property_col_indices[k];
        /// Writes `"name":value`. The trailing arguments disable number-stringifying, indentation,
        /// the space after the colon, and pretty-printing.
        JSONUtils::writeFieldFromColumn(
            *columns[idx],
            *serializations[idx],
            row_num,
            /*yield_strings=*/false,
            settings,
            *ostr,
            property_json_names[k],
            /*indent=*/0,
            /*title_after_delimiter=*/"",
            /*pretty_json=*/false);
    }
    writeChar('}', *ostr);
}

void GeoJSONRowOutputFormat::writeRowBetweenDelimiter()
{
    writeChar(',', *ostr);
}

void GeoJSONRowOutputFormat::writePrefix()
{
    writeCString(R"({"type":"FeatureCollection","features":[)", *ostr);
}

void GeoJSONRowOutputFormat::writeSuffix()
{
    /// On error (when `valid_output_on_exception` is set) append the exception as a final array
    /// element, mirroring `JSONEachRow`, so the document still closes cleanly.
    if (!exception_message.empty())
    {
        if (haveWrittenData())
            writeRowBetweenDelimiter();
        JSONUtils::writeException(exception_message, *ostr, settings, 0);
    }
    writeCString("]}\n", *ostr);
}

void GeoJSONRowOutputFormat::resetFormatterImpl()
{
    RowOutputFormatWithExceptionHandlerAdaptor::resetFormatterImpl();
    ostr = RowOutputFormatWithExceptionHandlerAdaptor::getWriteBufferPtr();
}

void registerOutputFormatGeoJSON(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "GeoJSON",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & format_settings, FormatFilterInfoPtr)
        { return std::make_shared<GeoJSONRowOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings); });
    factory.markOutputFormatSupportsParallelFormatting("GeoJSON");
    factory.setContentType("GeoJSON", "application/geo+json; charset=UTF-8");
    /// Each output is one complete top-level `FeatureCollection`, so appending to an existing file
    /// would produce a second collection and a malformed document.
    factory.markFormatHasNoAppendSupport("GeoJSON");
}

}
