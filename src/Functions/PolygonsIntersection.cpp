#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeCustomGeo.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


using CoordinateType = Float64;
using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
using Polygon = boost::geometry::model::polygon<Point, false>;
using MultiPolygon = boost::geometry::model::multi_polygon<Float64Polygon>;
using Box = boost::geometry::model::box<Point>;


class FunctionPolygonsIntersection : public IFunction
{
public:
    static inline const char * name = "polygonsIntersection";

    explicit FunctionPolygonsIntersection() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonsIntersection>();
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
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return DataTypeCustomMultiPolygonSerialization::nestedDataType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        auto get_parser = [&block, &arguments] (size_t i) {
            const ColumnWithTypeAndName polygon = block.getByPosition(arguments[i]);
            return makeGeometryFromColumnParser(polygon);
        };

        auto first_parser = get_parser(0);
        auto first_container = createContainer(first_parser);

        auto second_parser = get_parser(1);
        auto second_container = createContainer(second_parser);

        auto res_column = ColumnArray::create(ColumnArray::create(ColumnArray::create(
            ColumnTuple::create(
                Columns{ColumnVector<Float64>::create(input_rows_count),
                ColumnVector<Float64>::create(input_rows_count)}
            )
        )));
        auto & data = res_column->getData();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            get(first_parser, first_container, i);
            get(second_parser, second_container, i);

            Float64Geometry intersection;
            boost::geometry::intersection(first_container, second_container, intersection);

            // GeometrySerializer<MultiPolygon, Float64MultiPolygonSerializerVisitor> serializer;
            // serializer.add(intersection);
            // data[i] = serializer.finalize();
        }

        // block.getByPosition(result).column = std::move(res_column);
    }
};


void registerFunctionPolygonsIntersection(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsIntersection>();
}

}
