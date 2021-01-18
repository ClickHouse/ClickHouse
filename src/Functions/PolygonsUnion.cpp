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

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

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
        auto get_parser = [&arguments] (size_t i) {
            const auto * const_col =
                checkAndGetColumn<ColumnConst>(arguments[i].column.get());

            bool is_const = static_cast<bool>(const_col);

            return std::pair<bool, CartesianGeometryFromColumnParser>{is_const, is_const ?
                makeCartesianGeometryFromColumnParser(ColumnWithTypeAndName(const_col->getDataColumnPtr(), arguments[i].type, arguments[i].name)) :
                makeCartesianGeometryFromColumnParser(arguments[i])};
        };

        auto [is_first_polygon_const, first_parser] = get_parser(0);
        auto first_container = createContainer(first_parser);

        auto [is_second_polygon_const, second_parser] = get_parser(1);
        auto second_container = createContainer(second_parser);

        CartesianMultiPolygonSerializer serializer;

        for (size_t i = 0; i < input_rows_count; i++)
        {
            if (!is_first_polygon_const || i == 0)
                get(first_parser, first_container, i);
            if (!is_second_polygon_const || i == 0)
                get(second_parser, second_container, i);

            CartesianGeometry polygons_union = CartesianMultiPolygon({{{{}}}});
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
