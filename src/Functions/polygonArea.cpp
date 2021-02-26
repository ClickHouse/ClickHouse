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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Point>
class FunctionPolygonArea : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonArea() = default;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPolygonArea>();
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        auto res_column = ColumnFloat64::create();
        auto & res_data = res_column->getData();
        res_data.reserve(input_rows_count);

        callOnGeometryDataType<Point>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            if constexpr (std::is_same_v<PointFromColumnConverter<Point>, Converter>)
                throw Exception(fmt::format("The argument of function {} must not be Point", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            else
            {
                Converter converter(arguments[0].column->convertToFullColumnIfConst());
                auto geometries = converter.convert();

                if constexpr (std::is_same_v<PolygonFromColumnConverter<Point>, Converter>) {
                    for (auto & polygon : geometries) {
                        std::cout << "OUTER" << std::endl;
                        for (auto point : polygon.outer()) {
                            if constexpr (std::is_same_v<Point, CartesianPoint>) {
                                std::cout << point.x() << ' ' << point.y() << std::endl;
                            } else {
                                std::cout << point.template get<0>() << ' ' << point.template get<1>() << std::endl;
                            }
                        }
                        std::cout << "INNER" << std::endl;
                        for (auto & inner : polygon.inners()) {
                            for (auto point : inner) {
                                if constexpr (std::is_same_v<Point, CartesianPoint>) {
                                    std::cout << point.x() << ' ' << point.y() << std::endl;
                                } else {
                                    std::cout << point.template get<0>() << ' ' << point.template get<1>() << std::endl;
                                }
                            }
                        }
                    }
                }

                for (size_t i = 0; i < input_rows_count; i++)
                    res_data.emplace_back(boost::geometry::area(geometries[i]));
            }
        }
        );

        return res_column;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonArea<CartesianPoint>::name = "polygonAreaCartesian";

template <>
const char * FunctionPolygonArea<SphericalPoint>::name = "polygonAreaSpherical";


void registerFunctionPolygonArea(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonArea<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonArea<SphericalPoint>>();
}


}
