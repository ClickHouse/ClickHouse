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
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}


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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto first_parser = makeCartesianGeometryFromColumnParser(arguments[0]);
        auto first_container = createContainer(first_parser);

        auto second_parser = makeCartesianGeometryFromColumnParser(arguments[1]);
        auto second_container = createContainer(second_parser);

        CartesianMultiPolygonSerializer serializer;

        for (size_t i = 0; i < input_rows_count; i++)
        {
            if (i == 0)
                get(first_parser, first_container, i);
            if (i == 0)
                get(second_parser, second_container, i);

            CartesianGeometry intersection = CartesianMultiPolygon({{{{}}}});
            boost::geometry::intersection(
                boost::get<CartesianMultiPolygon>(first_container),
                boost::get<CartesianMultiPolygon>(second_container),
                boost::get<CartesianMultiPolygon>(intersection));

            boost::get<CartesianMultiPolygon>(intersection).erase(
                boost::get<CartesianMultiPolygon>(intersection).begin());

            serializer.add(intersection);
        }

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


void registerFunctionPolygonsIntersection(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsIntersection>();
}

}
