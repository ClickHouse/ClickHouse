#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <common/logger_useful.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeCustomGeo.h>

#include <memory>
#include <string>

namespace DB
{

class FunctionPolygonsUnion : public IFunction
{
public:
    static inline const char * name = "polygonsUnion";

    explicit FunctionPolygonsUnion() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonsUnion>();
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto first_parser = makeGeometryFromColumnParser<CartesianPoint>(arguments[0]);
        auto first_container = createContainer(first_parser);

        auto second_parser = makeGeometryFromColumnParser<CartesianPoint>(arguments[1]);
        auto second_container = createContainer(second_parser);

        MultiPolygonSerializer<CartesianPoint> serializer;

        for (size_t i = 0; i < input_rows_count; i++)
        {
            get<CartesianPoint>(first_parser, first_container, i);
            get<CartesianPoint>(second_parser, second_container, i);

            Geometry<CartesianPoint> polygons_union = CartesianMultiPolygon({{{{}}}});
            /// NOLINTNEXTLINE
            boost::geometry::union_(
                boost::get<CartesianMultiPolygon>(first_container),
                boost::get<CartesianMultiPolygon>(second_container),
                boost::get<CartesianMultiPolygon>(polygons_union));

            boost::get<CartesianMultiPolygon>(polygons_union).erase(
                boost::get<CartesianMultiPolygon>(polygons_union).begin());

            serializer.add(polygons_union);
        }

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


void registerFunctionPolygonsUnion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsUnion>();
}

}
