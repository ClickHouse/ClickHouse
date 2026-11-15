#pragma once

#include <variant>
#include <vector>

#include <IO/ReadBuffer.h>
#include <Functions/geometryConverters.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <IO/WriteBuffer.h>

namespace DB
{

enum class WKBGeometry : UInt32
{
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiLineString = 5,
    MultiPolygon = 6
};

using GeometricObject = std::variant<
    CartesianPoint,
    LineString<CartesianPoint>,
    MultiLineString<CartesianPoint>,
    Polygon<CartesianPoint>,
    MultiPolygon<CartesianPoint>>;

/// Documentation about WKB format: https://libgeos.org/specifications/wkb/
GeometricObject parseWKBFormat(ReadBuffer & in_buffer);

struct IWKBTransform
{
    virtual String dumpObject(const Field & geo_object) = 0;

    virtual ~IWKBTransform() = default;
};

struct WKBPointTransform : public IWKBTransform
{
    static constexpr const char * name = "Point";
    static constexpr WKBGeometry geometry_type = WKBGeometry::Point;

    String dumpObject(const Field & geo_object) override;
    static void dumpPointImpl(const Field & geo_object, WriteBuffer & buf);
};

struct WKBLineStringTransform : public IWKBTransform
{
    static constexpr const char * name = "LineString";
    static constexpr WKBGeometry geometry_type = WKBGeometry::LineString;

    String dumpObject(const Field & geo_object) override;
};

struct WKBPolygonTransform : public IWKBTransform
{
    static constexpr const char * name = "Polygon";
    static constexpr WKBGeometry geometry_type = WKBGeometry::Polygon;

    String dumpObject(const Field & geo_object) override;
};

struct WKBMultiLineStringTransform : public IWKBTransform
{
    static constexpr const char * name = "MultiLineString";
    static constexpr WKBGeometry geometry_type = WKBGeometry::MultiLineString;

    String dumpObject(const Field & geo_object) override;
};

struct WKBMultiPolygonTransform : public IWKBTransform
{
    static constexpr const char * name = "MultiPolygon";
    static constexpr WKBGeometry geometry_type = WKBGeometry::MultiPolygon;

    String dumpObject(const Field & geo_object) override;
};


}
