#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <common/logger_useful.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
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

template <typename Derived>
class FunctionPolygonAreaBase : public IFunction
{
public:
    static inline const char * name = Derived::name;

    explicit FunctionPolygonAreaBase() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonAreaBase>();
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return static_cast<const Derived*>(this)->executeImplementation(arguments, result_type, input_rows_count);
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};


class FunctionPolygonAreaCartesian : public FunctionPolygonAreaBase<FunctionPolygonAreaCartesian>
{
public:
    static inline const char * name = "polygonAreaCartesian";

    ColumnPtr executeImplementation(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
    {
        auto parser = makeCartesianGeometryFromColumnParser(arguments[0]);
        auto container = createContainer(parser);

        auto res_column = ColumnFloat64::create();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            get(parser, container, i);

            Float64 area = boost::geometry::area(
                boost::get<CartesianMultiPolygon>(container));

            res_column->insertValue(area);
        }

        return res_column;
    }
};

class FunctionPolygonAreaGeographic : public FunctionPolygonAreaBase<FunctionPolygonAreaGeographic>
{
public:
    static inline const char * name = "polygonAreaGeographic";

    ColumnPtr executeImplementation(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
    {
        auto parser = makeGeographicGeometryFromColumnParser(arguments[0]);
        auto container = createContainer(parser);

        auto res_column = ColumnFloat64::create();

        for (size_t i = 0; i < input_rows_count; i++)
        {
            get(parser, container, i);

            Float64 area = boost::geometry::area(
                boost::get<GeographicMultiPolygon>(container));

            res_column->insertValue(area);
        }

        return res_column;
    }
};


void registerFunctionPolygonAreaCartesian(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonAreaCartesian>();
}

void registerFunctionPolygonAreaGeographic(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonAreaGeographic>();
}

}
