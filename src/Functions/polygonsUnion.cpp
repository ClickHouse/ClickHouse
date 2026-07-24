#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <Common/logger_useful.h>

#include <Columns/ColumnTuple.h>

#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

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
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be Point", getName());
            else if constexpr (std::is_same_v<ColumnToLineStringsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToLineStringsConverter<Point>, RightConverter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be LineString", getName());
            else if constexpr (std::is_same_v<ColumnToMultiLineStringsConverter<Point>, LeftConverter> || std::is_same_v<ColumnToMultiLineStringsConverter<Point>, RightConverter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Any argument of function {} must not be MultiLineString", getName());
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

}

REGISTER_FUNCTION(PolygonsUnion)
{
    factory.registerFunction<FunctionPolygonsUnion<CartesianPoint>>();
    factory.registerFunction<FunctionPolygonsUnion<SphericalPoint>>();
}

}
