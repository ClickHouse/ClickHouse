#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>

#if USE_RAPIDJSON
#include <rapidjson/document.h>
#endif

namespace DB
{

enum class GeoEncoding : uint8_t
{
    WKB,
    WKT
};

enum class GeoType : uint8_t
{
    Point,
    LineString,
    Polygon
};

struct GeoColumnMetadata
{
    GeoEncoding encoding;
    GeoType type;
};

#if USE_RAPIDJSON
std::unordered_map<String, GeoColumnMetadata> parseGeoMetadataEncoding(const std::optional<rapidjson::Value> & geo_json);
#endif

struct Point
{
    double x;
    double y;
};

using Line = std::vector<Point>;
using Polygon = std::vector<std::vector<Point>>;
using GeometricObject = std::variant<Point, Line, Polygon>;

struct IGeometryBuilder
{
    virtual void appendObject(const GeometricObject & object) = 0;
    virtual ColumnWithTypeAndName getResultColumn() = 0;

    virtual ~IGeometryBuilder() = default;
};

class PointColumnBuilder : public IGeometryBuilder
{
public:
    explicit PointColumnBuilder(const String & name_);

    void appendObject(const GeometricObject & object) override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    using ColumnFloat64Ptr = decltype(ColumnFloat64::create());

    ColumnFloat64Ptr point_column_x;
    ColumnFloat64Ptr point_column_y;
    PODArray<double, 4096UL, Allocator<false, false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD> & point_column_data_x;
    PODArray<double, 4096UL, Allocator<false, false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD> & point_column_data_y;

    String name;
};

class LineColumnBuilder : public IGeometryBuilder
{
public:
    explicit LineColumnBuilder(const String & name_);

    void appendObject(const GeometricObject & object) override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    using OffsetColumnPtr = decltype(ColumnVector<ColumnArray::Offset>::create());

    OffsetColumnPtr offsets_column;
    PODArray<UInt64, 4096UL, Allocator<false, false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD> & offsets;

    PointColumnBuilder point_column_builder;
    size_t offset = 0;

    String name;
};

class PolygonColumnBuilder : public IGeometryBuilder
{
public:
    explicit PolygonColumnBuilder(const String & name_);

    void appendObject(const GeometricObject & object) override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    using OffsetColumnPtr = decltype(ColumnVector<ColumnArray::Offset>::create());

    OffsetColumnPtr offsets_column;
    PODArray<UInt64, 4096UL, Allocator<false, false>, PADDING_FOR_SIMD - 1, PADDING_FOR_SIMD> & offsets;

    LineColumnBuilder line_column_builder;
    size_t offset = 0;

    String name;
};

class GeoColumnBuilder : public IGeometryBuilder
{
public:
    explicit GeoColumnBuilder(const String & name_, GeoType type_);

    void appendObject(const GeometricObject & object) override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    std::unique_ptr<IGeometryBuilder> geomery_column_builder;

    String name;
};

GeometricObject parseWKBFormat(ReadBuffer & in_buffer);
GeometricObject parseWKTFormat(ReadBuffer & in_buffer);

}
