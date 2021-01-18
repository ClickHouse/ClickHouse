#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <common/logger_useful.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
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

template <typename Derived>
class FunctionPolygonsDistanceBase : public IFunction
{
public:
    static inline const char * name = Derived::name;

    explicit FunctionPolygonsDistanceBase() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonsDistanceBase>();
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
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return static_cast<const Derived*>(this)->executeImplementation(arguments, result_type, input_rows_count);
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


class FunctionPolygonsDistanceCartesian : public FunctionPolygonsDistanceBase<FunctionPolygonsDistanceCartesian>
{
public:
    static inline const char * name = "polygonsDistanceCartesian";

    ColumnPtr executeImplementation(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
    {
        auto first_parser = makeCartesianGeometryFromColumnParser(arguments[0]);
        auto first_container = createContainer(first_parser);

        auto second_parser = makeCartesianGeometryFromColumnParser(arguments[1]);
        auto second_container = createContainer(second_parser);

        auto res_column = ColumnFloat64::create();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            Float64 distance = boost::geometry::distance(
                    boost::get<CartesianMultiPolygon>(first_container),
                    boost::get<CartesianMultiPolygon>(second_container));

            res_column->insertValue(distance);
        }

        return res_column;
    }
};


class FunctionPolygonsDistanceGeographic : public FunctionPolygonsDistanceBase<FunctionPolygonsDistanceGeographic>
{
public:
    static inline const char * name = "polygonsDistanceGeographic";

    ColumnPtr executeImplementation(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
    {
        auto first_parser = makeGeographicGeometryFromColumnParser(arguments[0]);
        auto first_container = createContainer(first_parser);

        auto second_parser = makeGeographicGeometryFromColumnParser(arguments[1]);
        auto second_container = createContainer(second_parser);

        auto res_column = ColumnFloat64::create();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            Float64 distance = boost::geometry::distance(
                    boost::get<GeographicMultiPolygon>(first_container),
                    boost::get<GeographicMultiPolygon>(second_container));

            res_column->insertValue(distance);
        }

        return res_column;
    }
};


void registerFunctionPolygonsDistanceCartesian(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsDistanceCartesian>();
}

void registerFunctionPolygonsDistanceGeographic(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsDistanceGeographic>();
}

}
