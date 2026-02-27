#include <Processors/Formats/Impl/GeoJSONRowInputFormat.h>

#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeCustomGeo.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeVariant.h>
#include <Formats/FormatFactory.h>
#include <Formats/JSONUtils.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{


/// Reads a GeoJSON position [lon, lat, ...] into a Tuple{Float64, Float64}.
Field readGeoJSONPoint(ReadBuffer & buf)
{
    skipWhitespaceIfAny(buf);
    assertChar('[', buf);
    skipWhitespaceIfAny(buf);

    Float64 lon, lat;
    readFloatText(lon, buf);
    skipWhitespaceIfAny(buf);
    assertChar(',', buf);
    skipWhitespaceIfAny(buf);
    readFloatText(lat, buf);
    skipWhitespaceIfAny(buf);

    /// Skip optional extra coordinates (e.g. altitude).
    while (!buf.eof() && *buf.position() != ']')
    {
        if (*buf.position() == ',')
            ++buf.position();
        skipWhitespaceIfAny(buf);
        Float64 ignored;
        readFloatText(ignored, buf);
        skipWhitespaceIfAny(buf);
    }

    assertChar(']', buf);
    return Tuple{lon, lat};
}

/// Reads [[lon,lat],...] into an Array of Tuple{Float64, Float64} (Ring / LineString coordinates).
Array readGeoJSONLinearRing(ReadBuffer & buf)
{
    skipWhitespaceIfAny(buf);
    assertChar('[', buf);
    skipWhitespaceIfAny(buf);

    Array points;
    while (!buf.eof() && *buf.position() != ']')
    {
        points.push_back(readGeoJSONPoint(buf));
        skipWhitespaceIfAny(buf);
        if (!buf.eof() && *buf.position() == ',')
        {
            ++buf.position();
            skipWhitespaceIfAny(buf);
        }
    }

    assertChar(']', buf);
    return points;
}

/// Reads [[[lon,lat],...]] into Array of Array of Tuple (Polygon / MultiLineString coordinates).
Array readGeoJSONPolygonCoordinates(ReadBuffer & buf)
{
    skipWhitespaceIfAny(buf);
    assertChar('[', buf);
    skipWhitespaceIfAny(buf);

    Array rings;
    while (!buf.eof() && *buf.position() != ']')
    {
        rings.push_back(readGeoJSONLinearRing(buf));
        skipWhitespaceIfAny(buf);
        if (!buf.eof() && *buf.position() == ',')
        {
            ++buf.position();
            skipWhitespaceIfAny(buf);
        }
    }

    assertChar(']', buf);
    return rings;
}

/// Reads [[[[lon,lat],...],...]] into Array of Array of Array of Tuple (MultiPolygon coordinates).
Array readGeoJSONMultiPolygonCoordinates(ReadBuffer & buf)
{
    skipWhitespaceIfAny(buf);
    assertChar('[', buf);
    skipWhitespaceIfAny(buf);

    Array polygons;
    while (!buf.eof() && *buf.position() != ']')
    {
        polygons.push_back(readGeoJSONPolygonCoordinates(buf));
        skipWhitespaceIfAny(buf);
        if (!buf.eof() && *buf.position() == ',')
        {
            ++buf.position();
            skipWhitespaceIfAny(buf);
        }
    }

    assertChar(']', buf);
    return polygons;
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
    if (!format_settings.geojson.allow_experimental)
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "GeoJSON format is experimental. Set allow_experimental_geojson_format = 1 to enable it.");

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
            const auto * variant_type = typeid_cast<const DataTypeVariant *>(col.type.get());
            if (variant_type)
            {
                for (const auto & geo_type_name :
                     {"Point", "LineString", "Polygon", "MultiPolygon", "Ring", "MultiLineString"})
                {
                    auto discr = variant_type->tryGetVariantDiscriminator(geo_type_name);
                    if (discr.has_value())
                        geometry_discriminants[geo_type_name] = *discr;
                }
            }
        }
        else if (col.name == "properties")
        {
            properties_col_idx = i;
        }
    }
}

void GeoJSONRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    first_row = true;
    done = false;
}

void GeoJSONRowInputFormat::readPrefix()
{
    auto & buf = getReadBuffer();
    skipWhitespaceIfAny(buf);
    assertChar('{', buf);

    while (true)
    {
        skipWhitespaceIfAny(buf);
        if (buf.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: unexpected end of input, 'features' array not found");
        if (*buf.position() == '}')
            throw Exception(ErrorCodes::INCORRECT_DATA, "GeoJSON: 'features' array not found in FeatureCollection");

        String key = JSONUtils::readFieldName(buf, format_settings.json);
        skipWhitespaceIfAny(buf);

        if (key == "features")
        {
            assertChar('[', buf);
            return;
        }

        skipJSONField(buf, "", format_settings.json);
        skipWhitespaceIfAny(buf);
        if (!buf.eof() && *buf.position() == ',')
            ++buf.position();
    }
}

void GeoJSONRowInputFormat::readSuffix()
{
    auto & buf = getReadBuffer();
    skipWhitespaceIfAny(buf);

    /// Skip any remaining keys in the FeatureCollection after the features array.
    while (!buf.eof() && *buf.position() != '}')
    {
        if (*buf.position() == ',')
            ++buf.position();
        skipWhitespaceIfAny(buf);
        if (buf.eof() || *buf.position() == '}')
            break;

        JSONUtils::readFieldName(buf, format_settings.json);
        skipWhitespaceIfAny(buf);
        skipJSONField(buf, "", format_settings.json);
        skipWhitespaceIfAny(buf);
    }

    if (!buf.eof() && *buf.position() == '}')
        ++buf.position();
}

bool GeoJSONRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (done)
        return false;

    auto & buf = getReadBuffer();
    skipWhitespaceIfAny(buf);

    if (!first_row)
    {
        if (buf.eof())
        {
            done = true;
            return false;
        }
        if (*buf.position() == ']')
        {
            ++buf.position();
            done = true;
            return false;
        }
        if (*buf.position() == ',')
            ++buf.position();
        skipWhitespaceIfAny(buf);
    }

    if (buf.eof() || *buf.position() == ']')
    {
        if (!buf.eof())
            ++buf.position();
        done = true;
        return false;
    }

    first_row = false;

    assertChar('{', buf);

    bool has_id = false;
    bool has_geometry = false;
    bool has_properties = false;

    while (true)
    {
        skipWhitespaceIfAny(buf);
        if (buf.eof())
            break;
        if (*buf.position() == '}')
        {
            ++buf.position();
            break;
        }

        String key = JSONUtils::readFieldName(buf, format_settings.json);
        skipWhitespaceIfAny(buf);

        if (key == "id" && id_col_idx.has_value())
        {
            if (!buf.eof() && *buf.position() == 'n')
            {
                assertString("null", buf);
                columns[*id_col_idx]->insertDefault();
            }
            else
            {
                serializations[*id_col_idx]->deserializeTextJSON(*columns[*id_col_idx], buf, format_settings);
            }
            has_id = true;
        }
        else if (key == "geometry" && geometry_col_idx.has_value())
        {
            readGeometry(*columns[*geometry_col_idx]);
            has_geometry = true;
        }
        else if (key == "properties" && properties_col_idx.has_value())
        {
            if (!buf.eof() && *buf.position() == 'n')
            {
                assertString("null", buf);
                columns[*properties_col_idx]->insertDefault();
            }
            else
            {
                serializations[*properties_col_idx]->deserializeTextJSON(
                    *columns[*properties_col_idx], buf, format_settings);
            }
            has_properties = true;
        }
        else
        {
            skipJSONField(buf, "", format_settings.json);
        }

        skipWhitespaceIfAny(buf);
        if (!buf.eof() && *buf.position() == ',')
            ++buf.position();
    }

    if (!has_id && id_col_idx.has_value())
        columns[*id_col_idx]->insertDefault();
    if (!has_geometry && geometry_col_idx.has_value())
        columns[*geometry_col_idx]->insertDefault();
    if (!has_properties && properties_col_idx.has_value())
        columns[*properties_col_idx]->insertDefault();

    ext.read_columns.assign(columns.size(), 1);
    return true;
}

void GeoJSONRowInputFormat::readGeometry(IColumn & col)
{
    auto & buf = getReadBuffer();
    auto & variant_col = assert_cast<ColumnVariant &>(col);

    skipWhitespaceIfAny(buf);

    if (buf.eof() || *buf.position() == 'n')
    {
        if (!buf.eof())
            assertString("null", buf);
        variant_col.insertDefault();
        return;
    }

    assertChar('{', buf);

    String geo_type;
    String raw_coordinates;

    while (true)
    {
        skipWhitespaceIfAny(buf);
        if (buf.eof())
            break;
        if (*buf.position() == '}')
        {
            ++buf.position();
            break;
        }

        String key = JSONUtils::readFieldName(buf, format_settings.json);
        skipWhitespaceIfAny(buf);

        if (key == "type")
        {
            readJSONString(geo_type, buf, format_settings.json);
        }
        else if (key == "coordinates")
        {
            /// Always buffer as raw string so key order doesn't matter.
            readJSONField(raw_coordinates, buf, format_settings.json);
        }
        else
        {
            skipJSONField(buf, "", format_settings.json);
        }

        skipWhitespaceIfAny(buf);
        if (!buf.eof() && *buf.position() == ',')
            ++buf.position();
    }

    if (geo_type == "GeometryCollection")
    {
        if (format_settings.geojson.geometry_collection_handling == FormatSettings::GeometryCollectionHandling::Throw)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "GeoJSON: GeometryCollection is not supported because it cannot be represented "
                "in ClickHouse's Geometry type. Set input_format_geojson_geometry_collection_handling = 'null' "
                "to insert NULL for such geometries instead of throwing.");
        variant_col.insertDefault();
        return;
    }

    if (geo_type.empty() || raw_coordinates.empty())
    {
        variant_col.insertDefault();
        return;
    }

    auto it = geometry_discriminants.find(geo_type);
    if (it == geometry_discriminants.end())
    {
        variant_col.insertDefault();
        return;
    }

    ColumnVariant::Discriminator global_discr = it->second;
    auto & sub_col = variant_col.getVariantByGlobalDiscriminator(global_discr);
    auto local_discr = variant_col.localDiscriminatorByGlobal(global_discr);
    size_t offset = sub_col.size();

    ReadBufferFromString coord_buf(raw_coordinates);
    Field coordinates_field;

    if (geo_type == "Point")
        coordinates_field = readGeoJSONPoint(coord_buf);
    else if (geo_type == "LineString" || geo_type == "Ring")
        coordinates_field = readGeoJSONLinearRing(coord_buf);
    else if (geo_type == "Polygon" || geo_type == "MultiLineString")
        coordinates_field = readGeoJSONPolygonCoordinates(coord_buf);
    else if (geo_type == "MultiPolygon")
        coordinates_field = readGeoJSONMultiPolygonCoordinates(coord_buf);

    sub_col.insert(coordinates_field);
    variant_col.getLocalDiscriminators().push_back(local_discr);
    variant_col.getOffsets().push_back(offset);
}

NamesAndTypesList GeoJSONExternalSchemaReader::readSchema()
{
    return {
        {"id", std::make_shared<DataTypeString>()},
        {"geometry", DataTypeFactory::instance().get("Geometry")},
        {"properties", DataTypeFactory::instance().get("JSON")},
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
