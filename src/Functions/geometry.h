#pragma once

#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <base/EnumReflection.h>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/Field.h>
#include <base/types.h>
#include <DataTypes/DataTypeVariant.h>
#include <Functions/FunctionHelpers.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Point>
Point getPointFromField(const Field & field)
{
    const auto & point = field.safeGet<Tuple>();
    auto x = point.at(0).safeGet<Float64>();
    auto y = point.at(1).safeGet<Float64>();
    return {x, y};
}

template <typename Point>
LineString<Point> getLineStringFromField(const Field & field)
{
    LineString<Point> linestring;
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        linestring.push_back(getPointFromField<Point>(tuple));
    return linestring;
}

template <typename Point>
Ring<Point> getRingFromField(const Field & field)
{
    Ring<Point> ring;
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        ring.push_back(getPointFromField<Point>(tuple));
    return ring;
}

template <typename Point>
MultiLineString<Point> getMultiLineStringFromField(const Field & field)
{
    MultiLineString<Point> multilinestring;
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        multilinestring.push_back(getLineStringFromField<Point>(tuple));
    return multilinestring;
}

template <typename Point>
Polygon<Point> getPolygonFromField(const Field & field)
{
    Polygon<Point> polygon;
    const auto & array = field.safeGet<Array>();
    std::vector<Ring<Point>> rings_outer;
    Ring<Point> ring_inner;

    for (size_t i = 0; i < array.size(); ++i)
    {
        const auto & ring = array.at(i);
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
    const auto & array = field.safeGet<Array>();
    for (const auto & tuple : array)
        polygon.push_back(getPolygonFromField<Point>(tuple));
    return polygon;
}

enum class GeometryColumnType
{
    Linestring = 0,
    MultiLinestring = 1,
    MultiPolygon = 2,
    Point = 3,
    Polygon = 4,
    Ring = 5,
    Null = 255
};

}

/// GeometryColumnType has Null = 255, which is outside the default magic_enum range [-128, 127].
template <> struct magic_enum::customize::enum_range<DB::GeometryColumnType>
{
    static constexpr int min = 0;
    static constexpr int max = 255;
};

namespace DB
{

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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->getName() != "Geometry")
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument of function {} should be Geometry, got {}", getName(), arguments[0]->getName());
        }

        return std::make_shared<DataTypeFloat64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Geometry functions work with the Geometry type directly which is a Variant with custom name,
    /// and not with individual variant alternatives. So, don't use default implementation.
    bool useDefaultImplementationForVariant() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnFloat64::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        const auto & column_variant = assert_cast<const ColumnVariant &>(*arguments.front().column.get());
        Field field;
        const auto & descriptors = column_variant.getLocalDiscriminators();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            column_variant.get(i, field);
            auto type = magic_enum::enum_cast<GeometryColumnType>(descriptors[i]);
            if (!type)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown type of geometry {}", static_cast<Int32>(descriptors[i]));
            switch (*type)
            {
                case GeometryColumnType::Linestring:
                {
                    LineString<Point> linestring = getLineStringFromField<Point>(field);
                    res_data.push_back(FunctionToCalculate()(linestring));
                    break;
                }
                case GeometryColumnType::MultiLinestring:
                {
                    MultiLineString<Point> multilinestring = getMultiLineStringFromField<Point>(field);
                    res_data.push_back(FunctionToCalculate()(multilinestring));
                    break;
                }
                case GeometryColumnType::MultiPolygon:
                {
                    MultiPolygon<Point> multipolygon = getMultiPolygonFromField<Point>(field);
                    res_data.push_back(FunctionToCalculate()(multipolygon));
                    break;
                }
                case GeometryColumnType::Point:
                {
                    Point point = getPointFromField<Point>(field);
                    res_data.push_back(FunctionToCalculate()(point));
                    break;
                }
                case GeometryColumnType::Polygon:
                {
                    Polygon<Point> polygon = getPolygonFromField<Point>(field);
                    res_data.push_back(FunctionToCalculate()(polygon));
                    break;
                }
                case GeometryColumnType::Ring:
                {
                    Ring<Point> ring = getRingFromField<Point>(field);
                    res_data.push_back(FunctionToCalculate()(ring));
                    break;
                }
                case GeometryColumnType::Null:
                {
                    res_data.push_back(0);
                    break;
                }
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
