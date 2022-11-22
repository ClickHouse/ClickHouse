#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <base/logger_useful.h>

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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


template <typename Point>
class FunctionPolygonsUnion : public IFunction
{
public:
    static inline const char * name;

    explicit FunctionPolygonsUnion() = default;

    static FunctionPtr create(ContextPtr)
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
        return DataTypeFactory::instance().get("MultiPolygon");
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        MultiPolygonSerializer<Point> serializer;

        callOnTwoGeometryDataTypes<Point>(arguments[0].type, arguments[1].type, [&](const auto & left_type, const auto & right_type)
        {
            using LeftConverterType = std::decay_t<decltype(left_type)>;
            using RightConverterType = std::decay_t<decltype(right_type)>;

            using LeftConverter = typename LeftConverterType::Type;
            using RightConverter = typename RightConverterType::Type;

            if constexpr (std::is_same_v<ColumnToPointsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToPointsConverter<Point>, RightConverter>)
                throw Exception(fmt::format("Any argument of function {} must not be Point", getName()), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            else
            {
                auto first = LeftConverter::convert(arguments[0].column->convertToFullColumnIfConst());
                auto second = RightConverter::convert(arguments[1].column->convertToFullColumnIfConst());

                /// We are not interested in some pitfalls in third-party libraries
                /// NOLINTNEXTLINE(clang-analyzer-core.uninitialized.Assign)
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    /// Orient the polygons correctly.
                    boost::geometry::correct(first[i]);
                    boost::geometry::correct(second[i]);

                    MultiPolygon<Point> polygons_union{};
                    /// Main work here.
                    boost::geometry::union_(first[i], second[i], polygons_union);

                    serializer.add(polygons_union);
                }
            }
        });

        return serializer.finalize();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
};

template <>
const char * FunctionPolygonsUnion<CartesianPoint>::name = "polygonsUnionCartesian";

template <>
const char * FunctionPolygonsUnion<SphericalPoint>::name = "polygonsUnionSpherical";


void registerFunctionPolygonsUnion(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolygonsUnion<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonsUnion<SphericalPoint>>();
}

}
