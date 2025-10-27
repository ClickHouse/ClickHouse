#pragma once

#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Field.h>
#include <base/types.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename Point>
Point getPointFromField(const Field & field)
{
    auto point = field.safeGet<Tuple>();
    auto x = point.at(0).safeGet<Float64>();
    auto y = point.at(1).safeGet<Float64>();
    return {x, y};
}

template <typename Point>
LineString<Point> getLineStringFromField(const Field & field)
{
    LineString<Point> linestring;
    auto array = field.safeGet<Array>();
    for (size_t i = 0; i < array.size(); ++i)
    {
        auto tuple = array.at(i);
        linestring.push_back(getPointFromField<Point>(tuple));
    }
    return linestring;
}

template <typename Point>
Ring<Point> getRingFromField(const Field & field)
{
    Ring<Point> ring;
    auto array = field.safeGet<Array>();
    for (size_t i = 0; i < array.size(); ++i)
    {
        auto tuple = array.at(i);
        ring.push_back(getPointFromField<Point>(tuple));
    }
    return ring;
}

template <typename Point>
MultiLineString<Point> getMultiLineStringFromField(const Field & field)
{
    MultiLineString<Point> multilinestring;
    auto array = field.safeGet<Array>();
    for (size_t i = 0; i < array.size(); ++i)
    {
        auto tuple = array.at(i);
        multilinestring.push_back(getLineStringFromField<Point>(tuple));
    }
    return multilinestring;
}

template <typename Point>
Polygon<Point> getPolygonFromField(const Field & field)
{
    Polygon<Point> polygon;
    auto array = field.safeGet<Array>();
    std::vector<Ring<Point>> rings_outer;
    Ring<Point> ring_inner;
    for (size_t i = 0; i < array.size(); ++i)
    {
        auto ring = array.at(i);
        if (i == 0)
            polygon.outer() = getRingFromField<Point>(ring);
        else
            polygon.inners().push_back(getRingFromField<Point>(ring));
    }
    return polygon;
}

template <typename Point>
MultiPolygon<Point> getMultiPolygonFromField(const Field & field)
{
    MultiPolygon<Point> polygon;
    auto array = field.safeGet<Array>();
    for (size_t i = 0; i < array.size(); ++i)
    {
        auto tuple = array.at(i);
        polygon.push_back(getPolygonFromField<Point>(tuple));
    }
    return polygon;
}

template <typename Point, typename FunctionToCalculate>
class FunctionGeometry : public IFunction
{
public:
    static const char * name;

    explicit FunctionGeometry() = default;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGeometry>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnFloat64::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        const auto & column_variant = assert_cast<const ColumnVariant &>(*arguments.front().column.get());
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Field field;
            column_variant.get(i, field);
            const auto & descriptors = column_variant.getLocalDiscriminatorsColumn();
            auto type = descriptors.getUInt(i);
            if (type == 0)
            {
                // Linestring
                LineString<Point> linestring = getLineStringFromField<Point>(field);
                res_data.push_back(static_cast<Float64>(FunctionToCalculate()(linestring)));
            }
            else if (type == 1)
            {
                // MultiLinestring
                MultiLineString<Point> multilinestring = getMultiLineStringFromField<Point>(field);
                res_data.push_back(static_cast<Float64>(FunctionToCalculate()(multilinestring)));
            }
            else if (type == 2)
            {
                // MultiPolygon
                MultiPolygon<Point> multipolygon = getMultiPolygonFromField<Point>(field);
                res_data.push_back(static_cast<Float64>(FunctionToCalculate()(multipolygon)));
            }
            else if (type == 3)
            {
                // Point
                Point point = getPointFromField<Point>(field);
                res_data.push_back(static_cast<Float64>(FunctionToCalculate()(point)));
            }
            else if (type == 4)
            {
                // Polygon
                Polygon<Point> polygon = getPolygonFromField<Point>(field);
                res_data.push_back(static_cast<Float64>(FunctionToCalculate()(polygon)));
            }
            else if (type == 5)
            {
                // Ring
                Ring<Point> ring = getRingFromField<Point>(field);
                res_data.push_back(static_cast<Float64>(boost::geometry::perimeter(ring)));
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of geometry");
            }
        }

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

}
