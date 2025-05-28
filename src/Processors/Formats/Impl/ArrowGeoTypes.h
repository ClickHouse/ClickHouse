#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include "base/types.h"

#if USE_ARROW
#    include <arrow/array/array_binary.h>
#    include <arrow/util/key_value_metadata.h>
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
    Polygon,
    MultiLineString,
    MultiPolygon
};

struct GeoColumnMetadata
{
    GeoEncoding encoding;
    GeoType type;
};

#if USE_ARROW
std::optional<Poco::JSON::Object::Ptr> extractGeoMetadata(std::shared_ptr<const arrow::KeyValueMetadata> metadata);
#endif

std::unordered_map<String, GeoColumnMetadata> parseGeoMetadataEncoding(std::optional<Poco::JSON::Object::Ptr> geo_json);

struct ArrowPoint
{
    double x;
    double y;
};

using ArrowLineString = std::vector<ArrowPoint>;
using ArrowPolygon = std::vector<std::vector<ArrowPoint>>;
using ArrowMultiLineString = std::vector<ArrowLineString>;
using ArrowMultiPolygon = std::vector<ArrowPolygon>;

using ArrowGeometricObject = std::variant<ArrowPoint, ArrowLineString, ArrowPolygon, ArrowMultiPolygon>;

struct IGeometryColumnBuilder
{
    virtual void appendObject(const ArrowGeometricObject & object) = 0;
    virtual void appendDefault() = 0;
    virtual ColumnWithTypeAndName getResultColumn() = 0;

    virtual ~IGeometryColumnBuilder() = default;
};

class PointColumnBuilder : public IGeometryColumnBuilder
{
public:
    explicit PointColumnBuilder(const String & name_);

    void appendObject(const ArrowGeometricObject & object) override;
    void appendDefault() override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    using ColumnFloat64Ptr = decltype(ColumnFloat64::create());

    ColumnFloat64Ptr point_column_x;
    ColumnFloat64Ptr point_column_y;
    PaddedPODArray<Float64> & point_column_data_x;
    PaddedPODArray<Float64> & point_column_data_y;

    String name;
};

class LineColumnBuilder : public IGeometryColumnBuilder
{
public:
    explicit LineColumnBuilder(const String & name_);

    void appendObject(const ArrowGeometricObject & object) override;
    void appendDefault() override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    using OffsetColumnPtr = decltype(ColumnVector<ColumnArray::Offset>::create());

    OffsetColumnPtr offsets_column;
    PaddedPODArray<UInt64> & offsets;

    PointColumnBuilder point_column_builder;
    size_t offset = 0;

    String name;
};

class PolygonColumnBuilder : public IGeometryColumnBuilder
{
public:
    explicit PolygonColumnBuilder(const String & name_);

    void appendObject(const ArrowGeometricObject & object) override;
    void appendDefault() override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    using OffsetColumnPtr = decltype(ColumnVector<ColumnArray::Offset>::create());

    OffsetColumnPtr offsets_column;
    PaddedPODArray<UInt64> & offsets;

    LineColumnBuilder line_column_builder;
    size_t offset = 0;

    String name;
};

class MultiLineStringColumnBuilder : public IGeometryColumnBuilder
{
public:
    explicit MultiLineStringColumnBuilder(const String & name_);

    void appendObject(const ArrowGeometricObject & object) override;
    void appendDefault() override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    using OffsetColumnPtr = decltype(ColumnVector<ColumnArray::Offset>::create());

    OffsetColumnPtr offsets_column;
    PaddedPODArray<UInt64> & offsets;

    LineColumnBuilder line_column_builder;
    size_t offset = 0;

    String name;
};

class MultiPolygonColumnBuilder : public IGeometryColumnBuilder
{
public:
    explicit MultiPolygonColumnBuilder(const String & name_);

    void appendObject(const ArrowGeometricObject & object) override;
    void appendDefault() override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    using OffsetColumnPtr = decltype(ColumnVector<ColumnArray::Offset>::create());

    OffsetColumnPtr offsets_column;
    PaddedPODArray<UInt64> & offsets;

    PolygonColumnBuilder polygon_column_builder;
    size_t offset = 0;

    String name;
};


class GeoColumnBuilder : public IGeometryColumnBuilder
{
public:
    explicit GeoColumnBuilder(const String & name_, GeoType type_);

    void appendObject(const ArrowGeometricObject & object) override;
    void appendDefault() override;

    ColumnWithTypeAndName getResultColumn() override;

private:
    std::unique_ptr<IGeometryColumnBuilder> geomery_column_builder;

    String name;
};

ArrowGeometricObject parseWKBFormat(ReadBuffer & in_buffer);
ArrowGeometricObject parseWKTFormat(ReadBuffer & in_buffer);

}
